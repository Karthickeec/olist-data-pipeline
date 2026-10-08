"""Verify Silver.

  full                  Silver vs Postgres, row accounting, geolocation, order_lines totals, and
                        quarantine counts vs the dirty records counted straight from the landing files
  idempotency --date D  rerun Silver for D; every Silver table, quarantine and pending area must be
                        unchanged apart from processing-time columns
"""
import argparse
import json
import sys
from collections import Counter
from datetime import date

from pyspark.sql import functions as F

from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.lake import list_dirs, path_exists, table_path
from olist_pipeline.silver.clean import BRAZIL_STATES
from olist_pipeline.silver.common import pending_dir, quarantine_dir, silver_dir
from olist_pipeline.silver.job import SilverJob
from olist_pipeline.sources import TABLES
from olist_pipeline.spark import build_spark, read_jdbc
from olist_pipeline.verify import lake_fingerprint, silver_snapshot
from olist_pipeline.watermarks import Bookkeeping

PG_TABLES = ("customers", "orders", "order_items", "order_payments", "order_reviews")


class Report:
    def __init__(self):
        self.ok = True

    def __call__(self, name: str, passed: bool, detail: str) -> None:
        self.ok &= bool(passed)
        print(f"{name:<44} {'OK' if passed else 'MISMATCH':<9} {detail}", flush=True)


def same(a, b) -> bool:
    return a.count() == b.count() and a.exceptAll(b).isEmpty() and b.exceptAll(a).isEmpty()


def check_full(spark, cfg, conn, report: Report) -> None:
    root, schema, pipeline = cfg["lake"]["root"], cfg["pg"]["schema"], cfg["pg"]["pipeline_schema"]
    book = Bookkeeping(conn, pipeline)

    for table in PG_TABLES:
        cols = list(TABLES[table].column_names) + ["updated_at"]
        wm = book.watermark("olist_postgres", table)
        silver = spark.read.parquet(silver_dir(root, table)).select(cols)
        source = read_jdbc(spark, cfg, query=f"SELECT * FROM \"{schema}\".\"{table}\" "
                                             f"WHERE updated_at <= TIMESTAMP '{wm}'").select(cols)
        report(f"{table} == Postgres", same(silver, source), f"{silver.count()} rows (<= {wm})")

    bad = conn.execute(
        f"SELECT table_name, batch_date FROM {pipeline}.layer_runs WHERE layer = 'silver' "
        "AND rows_in <> rows_valid + rows_quarantined + rows_duplicate + rows_pending").fetchall()
    n_runs = conn.execute(f"SELECT count(*) FROM {pipeline}.layer_runs WHERE layer = 'silver'").fetchone()[0]
    report("row accounting (in = valid+quar+dup+pending)", not bad, f"{n_runs} table-batches checked {bad or ''}")

    zips = conn.execute(f'SELECT count(DISTINCT geolocation_zip_code_prefix) FROM "{schema}".geolocation').fetchone()[0]
    geo = spark.read.parquet(silver_dir(root, "geolocation"))
    report("geolocation one row per zip prefix", geo.count() == zips == geo.select("zip_code_prefix").distinct().count(),
           f"{geo.count()} rows, {zips} prefixes in source")

    lines = spark.read.parquet(silver_dir(root, "order_lines"))
    items = spark.read.parquet(silver_dir(root, "order_items"))
    report("order_lines: one line per item", lines.count() == items.count(),
           f"{lines.count()} lines, {items.count()} items")
    per_order = (lines.filter(F.col("payment_total").isNotNull()).groupBy("order_id")
                 .agg(F.sum("allocated_payment").alias("alloc"), F.first("payment_total").alias("total")))
    off = per_order.filter(F.col("alloc") != F.col("total")).count()
    report("order_lines: allocated payments sum to total", off == 0,
           f"{per_order.count()} orders, {off} off by any amount")
    untranslated = spark.read.parquet(silver_dir(root, "products")).filter(
        F.col("product_category_name_english").isNull()).count()
    report("products: every category has English", untranslated == 0, f"{untranslated} without")

    check_quarantine_vs_landing(spark, cfg, conn, report)

    last = conn.execute(f"SELECT max(batch_date) FROM {pipeline}.layer_runs WHERE layer = 'silver'").fetchone()[0]
    waiting = sum(lake_fingerprint(spark, f"{pending_dir(root, t)}/batch_date={last}", set())[0]
                  for t in PG_TABLES if path_exists(spark, f"{pending_dir(root, t)}/batch_date={last}"))
    report("late-arriving rows still pending", True, f"{waiting} after batch {last}")


def quarantine_reasons(spark, root: str, table: str) -> Counter:
    path = quarantine_dir(root, table)
    if not path_exists(spark, path) or not list_dirs(spark, path):
        return Counter()
    return Counter({r[0]: r[1] for r in spark.read.parquet(path).groupBy("_reason").count().collect()})


def check_quarantine_vs_landing(spark, cfg, conn, report: Report) -> None:
    """Count the injected dirt independently, from the raw landing files, and compare."""
    root, schema, pipeline = cfg["lake"]["root"], cfg["pg"]["schema"], cfg["pg"]["pipeline_schema"]
    landing = cfg["paths"]["landing_dir"]
    known = {r[0] for r in conn.execute(f'SELECT DISTINCT customer_unique_id FROM "{schema}".customers')}
    zips = {r[0] for r in conn.execute(f'SELECT DISTINCT geolocation_zip_code_prefix FROM "{schema}".geolocation')}
    names = {n.upper() for n in BRAZIL_STATES.values()}
    last = conn.execute(f"SELECT max(batch_date) FROM {pipeline}.layer_runs WHERE layer = 'silver'").fetchone()[0]

    def ingested(source: str, table: str) -> set[str]:
        """Landing days that reached Bronze (and so Silver), up to the last Silver batch."""
        days = {d.split("=", 1)[1] for d in list_dirs(spark, table_path(root, "bronze", source, table))}
        return {d for d in days if d <= last.isoformat()}

    expected, changes, fixed_states = Counter(), {}, 0
    days = ingested("crm", "customer_changes")
    for path in sorted((landing / "customer_changes").glob("dt=*/changes.jsonl")):
        if path.parent.name[3:] not in days:
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            changes[r["change_id"]] = r                         # exact duplicates collapse
    for r in changes.values():
        state = (r["new_state"] or "").strip().upper()
        if state not in BRAZIL_STATES and state not in names:
            expected["invalid_state"] += 1
        elif r["new_city"] is None and r["new_zip_code_prefix"] not in zips:
            expected["unfixable_city"] += 1
        elif r["customer_unique_id"] not in known:
            expected["unknown_customer"] += 1
        else:
            fixed_states += state != r["new_state"]
    got = quarantine_reasons(spark, root, "customer_changes")
    report("customer_changes quarantine == landing dirt", got == expected, f"expected {dict(expected)}, got {dict(got)}")
    silver_changes = spark.read.parquet(silver_dir(root, "customer_changes")).count()
    report("customer_changes: unique ids - quarantined = Silver",
           len(changes) - sum(expected.values()) == silver_changes,
           f"{len(changes)} unique change ids, {silver_changes} in Silver, {fixed_states} states normalised")

    expected, records, reformatted = Counter(), 0, 0
    days = ingested("api", "customer_activity")
    for path in sorted((landing / "customer_activity").glob("dt=*/page_*.json")):
        if path.parent.name[3:] not in days:
            continue
        for r in json.loads(path.read_text())["data"]:
            records += 1
            if r.get("sessions") is not None and int(r["sessions"]) < 0:
                expected["negative_sessions"] += 1
            elif r["customer_unique_id"] not in known:
                expected["unknown_customer"] += 1
            elif "/" in (r.get("last_seen_at") or ""):
                reformatted += 1
    got = quarantine_reasons(spark, root, "customer_activity")
    report("customer_activity quarantine == landing dirt", got == expected, f"expected {dict(expected)}, got {dict(got)}")
    silver_activity = spark.read.parquet(silver_dir(root, "customer_activity"))
    parsed = silver_activity.filter(F.col("_timestamp_reformatted") & F.col("last_seen_at").isNotNull()).count()
    report("customer_activity: Brazilian timestamps parsed", parsed == reformatted,
           f"{reformatted} in landing, {parsed} parsed in Silver; {records} records, {silver_activity.count()} in Silver")


def check_idempotency(spark, cfg, conn, day: date, report: Report) -> None:
    root = cfg["lake"]["root"]
    before = silver_snapshot(spark, root)
    SilverJob(spark, cfg, Bookkeeping(conn, cfg["pg"]["pipeline_schema"]), day).run()
    after = silver_snapshot(spark, root)
    for k in before:
        if before[k] is not None or after[k] is not None:
            report(k, before[k] == after[k], f"rows {before[k][0] if before[k] else 0} -> {after[k][0] if after[k] else 0}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="check", required=True)
    sub.add_parser("full")
    idem = sub.add_parser("idempotency")
    idem.add_argument("--date", type=date.fromisoformat, required=True)
    args = p.parse_args()

    cfg = load_config()
    spark = build_spark(cfg, "verify_silver")
    report = Report()
    try:
        with connect(cfg["pg"]) as conn:
            if args.check == "full":
                check_full(spark, cfg, conn, report)
            else:
                check_idempotency(spark, cfg, conn, args.date, report)
    finally:
        spark.stop()
    print("PASS" if report.ok else "FAIL")
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
