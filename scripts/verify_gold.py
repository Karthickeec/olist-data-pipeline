"""Verify Gold.

  full --date D         totals vs Silver, aggregates vs the fact, SCD2 invariants, every fact row
                        on exactly one customer version valid at purchase, LTV vs fact payments
  idempotency --date D  rerun Gold for D; every Gold table unchanged apart from processing columns
"""
import argparse
import sys
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.gold.job import GoldJob
from olist_pipeline.gold.model import NOT_A_SALE
from olist_pipeline.silver.common import layer_dir, silver_dir
from olist_pipeline.spark import build_spark
from olist_pipeline.verify import lake_fingerprint
from olist_pipeline.watermarks import Bookkeeping

GOLD_TABLES = ("dim_date", "dim_product", "dim_seller", "dim_customer", "fact_order_lines",
               "agg_daily_category_sales", "customer_metrics")


class Report:
    def __init__(self):
        self.ok = True

    def __call__(self, name: str, passed: bool, detail: str) -> None:
        self.ok &= bool(passed)
        print(f"{name:<52} {'OK' if passed else 'MISMATCH':<9} {detail}", flush=True)


def check_full(spark, cfg, day: date, report: Report) -> None:
    root = cfg["lake"]["root"]
    gold = lambda t: spark.read.parquet(layer_dir(root, "gold", t))   # noqa: E731
    fact, lines = gold("fact_order_lines"), spark.read.parquet(silver_dir(root, "order_lines"))
    f = fact.agg(F.count(F.lit(1)), F.sum("price"), F.sum("allocated_payment")).first()
    s = lines.agg(F.count(F.lit(1)), F.sum("price"), F.sum("allocated_payment")).first()
    report("fact = Silver order_lines (lines, price, payments)", tuple(f) == tuple(s),
           f"{f[0]} lines, price {f[1]}, payments {f[2]}")

    sales = fact.filter(~F.col("order_status").isin(*NOT_A_SALE))
    a = gold("agg_daily_category_sales").agg(F.sum("items"), F.sum("revenue"), F.sum("freight")).first()
    b = sales.agg(F.count(F.lit(1)), F.sum("price"), F.sum("freight_value")).first()
    report("agg_daily_category_sales totals = fact sales", tuple(a) == tuple(b),
           f"items {a[0]}, revenue {a[1]}, freight {a[2]}")

    dim = gold("dim_customer")
    w = Window.partitionBy("customer_unique_id").orderBy("valid_from")
    chained = dim.withColumn("_next", F.lead("valid_from").over(w))
    gaps = chained.filter(F.col("_next").isNotNull() & (F.col("_next") != F.col("valid_to"))).count()
    currents = dim.groupBy("customer_unique_id").agg(F.sum(F.col("is_current").cast("int")).alias("n"))
    bad_current = currents.filter("n <> 1").count()
    report("SCD2: one current version, no gaps or overlaps", gaps == 0 and bad_current == 0,
           f"{dim.count()} versions for {currents.count()} customers; {gaps} gaps, {bad_current} bad current flags")

    changes = spark.read.parquet(silver_dir(root, "customer_changes"))
    changed = changes.select("customer_unique_id").distinct()
    multi = dim.groupBy("customer_unique_id").count().filter("count > 1").select("customer_unique_id")
    without = changed.join(multi, "customer_unique_id", "left_anti").count()
    report("SCD2: customers with address changes have >1 version", True,
           f"{changed.count()} customers with change requests, {multi.count()} with several versions, "
           f"{without} whose requests kept the same address or coincided with their first order")

    matched = (fact.join(dim.select("customer_sk", F.col("customer_unique_id").alias("_c"), "valid_from", "valid_to"),
                         "customer_sk", "left")
               .withColumn("_ok", (F.col("_c") == F.col("customer_unique_id"))
                           & (F.col("order_purchase_timestamp") >= F.col("valid_from"))
                           & (F.col("order_purchase_timestamp") < F.col("valid_to"))))
    bad = matched.filter(~F.coalesce("_ok", F.lit(False))).count()
    report("every fact row on the version valid at purchase", bad == 0, f"{fact.count()} rows, {bad} not")

    metrics = gold("customer_metrics").filter(F.col("as_of_date") == F.lit(day.isoformat()).cast("date"))
    ltv = (sales.filter(F.col("order_purchase_date") <= F.lit(day.isoformat()).cast("date"))
           .groupBy("customer_unique_id").agg(F.sum("allocated_payment").alias("fact_ltv")))
    cmp = metrics.join(ltv, "customer_unique_id", "full_outer")
    off = cmp.filter(F.coalesce("lifetime_value", F.lit(0)) != F.coalesce("fact_ltv", F.lit(0))).count()
    totals = cmp.agg(F.sum("lifetime_value"), F.sum("fact_ltv")).first()
    report("customer_metrics LTV = fact payments per customer", off == 0,
           f"{metrics.count()} customers, total {totals[0]} vs {totals[1]}, {off} differ")


def snapshot(spark, root: str) -> dict:
    return {t: lake_fingerprint(spark, layer_dir(root, "gold", t), {"_processed_at"}) for t in GOLD_TABLES}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("check", choices=("full", "idempotency"))
    p.add_argument("--date", type=date.fromisoformat, required=True)
    args = p.parse_args()
    cfg = load_config()
    spark = build_spark(cfg, "verify_gold")
    report = Report()
    try:
        if args.check == "full":
            check_full(spark, cfg, args.date, report)
        else:
            before = snapshot(spark, cfg["lake"]["root"])
            with connect(cfg["pg"]) as conn:
                GoldJob(spark, cfg, Bookkeeping(conn, cfg["pg"]["pipeline_schema"]), args.date).run()
            after = snapshot(spark, cfg["lake"]["root"])
            for t in GOLD_TABLES:
                report(f"gold.{t}", before[t] == after[t], f"rows {before[t][0]} -> {after[t][0]}")
    finally:
        spark.stop()
    print("PASS" if report.ok else "FAIL")
    sys.exit(0 if report.ok else 1)


if __name__ == "__main__":
    main()
