"""Athena tables, saved KPI queries and an Athena-vs-Spark check for the S3 lake.

    athena.py ddl                  generate sql/athena/<layer>/<table>.sql from the local lake's Parquet schemas
    athena.py create               (re)create every table in the Glue database through Athena
    athena.py queries              save sql/athena/kpis/*.sql as Athena named queries, run them, show bytes scanned
    athena.py check --date D       Athena (reading S3) must give the same answers as Spark (reading the local lake,
                                   which `make verify-s3-parity` proves byte-identical to S3)
    athena.py register-partitions  fallback without projection: ALTER TABLE ... ADD PARTITION, then compare
"""
import argparse
import os
import sys
from decimal import Decimal

from olist_pipeline.athena import KPI_DIR, TABLES, Athena, ddl_path, render, table_ddl
from olist_pipeline.aws import session, to_s3_uri
from olist_pipeline.config import load_config

UNPROJECTED = "silver_orders_unprojected"


def aws_cfg() -> dict:
    return load_config(environ={**os.environ, "OLIST_TARGET": "aws"})


def athena_client(cfg: dict) -> Athena:
    aws = cfg["aws"]
    return Athena(session(aws["region"]).client("athena"), aws["athena_workgroup"], aws["glue_database"])


def cmd_ddl(_args) -> None:
    from olist_pipeline.spark import build_spark
    cfg = load_config()
    spark = build_spark(cfg, "athena-ddl")
    for t in TABLES:
        schema = spark.read.parquet(f"{cfg['lake']['root']}/{t.path}").schema
        cols = [(f.name, f.dataType.simpleString()) for f in schema.fields]
        path = ddl_path(t)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(table_ddl(cfg["aws"]["glue_database"], t, cols), encoding="utf-8")
        print(f"{path.relative_to(path.parents[2])}: {len(cols)} columns, partition {t.partition or '-'}")
    spark.stop()


def cmd_create(_args) -> None:
    cfg = aws_cfg()
    athena, lake = athena_client(cfg), to_s3_uri(cfg["lake"]["root"])
    for t in TABLES:
        athena.run(f"DROP TABLE IF EXISTS `{t.name}`")
        athena.run(render(ddl_path(t).read_text(encoding="utf-8"), lake))
        print(f"created {cfg['aws']['glue_database']}.{t.name}")
    print(f"{len(TABLES)} tables")


def cmd_queries(_args) -> None:
    cfg = aws_cfg()
    athena = athena_client(cfg)
    client, wg = athena.client, cfg["aws"]["athena_workgroup"]
    ids = client.list_named_queries(WorkGroup=wg).get("NamedQueryIds", [])
    existing = {q["Name"]: q["NamedQueryId"]
                for q in (client.batch_get_named_query(NamedQueryIds=ids)["NamedQueries"] if ids else [])}
    for path in sorted(KPI_DIR.glob("*.sql")):
        name, sql = path.stem, path.read_text(encoding="utf-8")
        if name in existing:
            client.delete_named_query(NamedQueryId=existing[name])
        client.create_named_query(Name=name, Database=athena.database, QueryString=sql, WorkGroup=wg,
                                  Description=sql.splitlines()[0].lstrip("- "))
        rows, stats = athena.rows(sql)
        print(f"\n== {name}  ({stats['bytes'] / 1e6:.2f} MB scanned, {stats['ms']} ms)")
        for r in rows[:8]:
            print("   " + " | ".join(v or "" for v in r))


def same(a, b) -> bool:
    """Compare an Athena string with a Spark value."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(b, (int, Decimal, float)):
        return Decimal(a) == Decimal(str(b))
    return a == str(b)


def cmd_check(args) -> None:
    from pyspark.sql import functions as F
    from olist_pipeline.spark import build_spark
    cfg = aws_cfg()
    local = load_config()   # Spark reads the local copy: ~0.4 s per S3 round trip from here (see README)
    athena, root, d = athena_client(cfg), local["lake"]["root"], args.date
    spark = build_spark(local, "athena-check")
    rd = lambda p: spark.read.parquet(f"{root}/{p}")
    fact, metrics = rd("gold/fact_order_lines"), rd("gold/customer_metrics")
    checks = {
        "fact lines, payments total":
            ("SELECT count(*), sum(allocated_payment) FROM gold_fact_order_lines",
             [tuple(fact.agg(F.count("*"), F.sum("allocated_payment")).first())]),
        "fact lines on the last day":
            (f"SELECT count(*) FROM gold_fact_order_lines WHERE order_purchase_date = DATE '{d}'",
             [(fact.filter(F.col("order_purchase_date") == d).count(),)]),
        f"RFM segments as of {d}":
            (f"SELECT rfm_segment, count(*) FROM gold_customer_metrics WHERE as_of_date = DATE '{d}' "
             "GROUP BY 1 ORDER BY 1",
             [tuple(r) for r in metrics.filter(F.col("as_of_date") == d).groupBy("rfm_segment").count()
              .orderBy("rfm_segment").collect()]),
        "SCD2 customer versions":
            ("SELECT count(*), count_if(is_current), count(DISTINCT customer_unique_id) FROM gold_dim_customer",
             [tuple(rd("gold/dim_customer").agg(F.count("*"), F.sum(F.col("is_current").cast("int")),
                                                  F.countDistinct("customer_unique_id")).first())]),
        "Silver orders by status":
            ("SELECT order_status, count(*) FROM silver_orders GROUP BY 1 ORDER BY 1",
             [tuple(r) for r in rd("silver/orders").groupBy("order_status").count().orderBy("order_status").collect()]),
        f"Bronze orders ingested on {d}":
            (f"SELECT count(*) FROM bronze_orders WHERE ingest_date = DATE '{d}'",
             [(rd("bronze/olist_postgres/orders").filter(F.col("ingest_date") == d).count(),)]),
        "quarantined API records":
            ("SELECT count(*) FROM silver_quarantine_customer_activity",
             [(rd("silver/_quarantine/customer_activity").count(),)]),
    }
    failed, scanned = 0, 0
    for name, (sql, expected) in checks.items():
        got, stats = athena.rows(sql)
        scanned += stats["bytes"]
        ok = len(got) == len(expected) and all(len(g) == len(e) and all(same(a, b) for a, b in zip(g, e))
                                               for g, e in zip(got, expected))
        failed += not ok
        print(f"{'PASS' if ok else 'FAIL'}  {name}: athena={got[:6]} spark={[tuple(map(str, e)) for e in expected][:6]}"
              f"  ({stats['bytes'] / 1e6:.2f} MB)")
    spark.stop()
    print(f"\n{len(checks) - failed}/{len(checks)} Athena answers equal Spark's; {scanned / 1e6:.1f} MB scanned in total")
    sys.exit(1 if failed else 0)


def cmd_register_partitions(_args) -> None:
    """Partition projection is the default; this shows the fallback for a table without it."""
    cfg = aws_cfg()
    athena, lake = athena_client(cfg), to_s3_uri(cfg["lake"]["root"])
    table = next(t for t in TABLES if t.name == "silver_orders")
    ddl = render(ddl_path(table).read_text(encoding="utf-8"), lake)
    ddl = ddl.split("TBLPROPERTIES")[0].replace(f"`{table.name}`", f"`{UNPROJECTED}`")
    athena.run(f"DROP TABLE IF EXISTS `{UNPROJECTED}`")
    athena.run(ddl)
    before, _ = athena.rows(f"SELECT count(*) FROM {UNPROJECTED}")
    bucket, _, prefix = lake.removeprefix("s3://").partition("/")
    s3 = session(cfg["aws"]["region"]).client("s3")
    parts = [p["Prefix"].rstrip("/").rsplit("=", 1)[1]
             for page in s3.get_paginator("list_objects_v2").paginate(
                 Bucket=bucket, Prefix=f"{prefix}/{table.path}/{table.partition}=", Delimiter="/")
             for p in page.get("CommonPrefixes", [])]
    for i in range(0, len(parts), 100):  # ADD IF NOT EXISTS is idempotent; batches keep statements small
        adds = " ".join(f"PARTITION (`{table.partition}` = DATE '{v}') "
                        f"LOCATION '{lake}/{table.path}/{table.partition}={v}/'" for v in parts[i:i + 100])
        athena.run(f"ALTER TABLE `{UNPROJECTED}` ADD IF NOT EXISTS {adds}")
    after, _ = athena.rows(f"SELECT count(*) FROM {UNPROJECTED}")
    projected, _ = athena.rows("SELECT count(*) FROM silver_orders")
    print(f"{UNPROJECTED}: {before[0][0]} rows before registration, {len(parts)} partitions registered, "
          f"{after[0][0]} rows after; projected silver_orders: {projected[0][0]} rows")
    sys.exit(0 if after == projected and before[0][0] == "0" else 1)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("ddl")
    sub.add_parser("create")
    sub.add_parser("queries")
    c = sub.add_parser("check")
    c.add_argument("--date", required=True)
    sub.add_parser("register-partitions")
    args = p.parse_args()
    {"ddl": cmd_ddl, "create": cmd_create, "queries": cmd_queries, "check": cmd_check,
     "register-partitions": cmd_register_partitions}[args.cmd](args)


if __name__ == "__main__":
    main()
