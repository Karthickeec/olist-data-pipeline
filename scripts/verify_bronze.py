"""Check Bronze against its sources.

* every incremental partition holds the row count recorded in pipeline.ingest_runs
* latest version per key across all partitions == Postgres rows up to the watermark
* the latest reference snapshot == the current Postgres table
* every customer_changes partition holds as many rows as its landing file has lines
* every customer_activity partition holds the total_records the API reported
"""

import json
import sys
from datetime import date

from pyspark.sql import Window as W
from pyspark.sql import functions as F

from olist_pipeline.api.client import landing_dir_for
from olist_pipeline.bronze.api_to_bronze import SOURCE as API_SOURCE
from olist_pipeline.bronze.api_to_bronze import TABLE as API_TABLE
from olist_pipeline.bronze.files_to_bronze import SOURCE as FILES_SOURCE
from olist_pipeline.bronze.files_to_bronze import TABLE as FILES_TABLE
from olist_pipeline.bronze.postgres_to_bronze import SOURCE
from olist_pipeline.config import load_config
from olist_pipeline.customer_changes import partition_dir
from olist_pipeline.db import connect
from olist_pipeline.lake import PARTITION_COLUMN, table_path
from olist_pipeline.sources import REFERENCE_TABLES, TABLES, TRANSACTIONAL_TABLES
from olist_pipeline.spark import build_spark, read_jdbc
from olist_pipeline.watermarks import Bookkeeping


def partition_counts(spark, path: str) -> dict:
    return {r[0]: r[1] for r in spark.read.parquet(path).groupBy(PARTITION_COLUMN).count().collect()}


def main() -> None:
    cfg = load_config()
    schema, pipeline = cfg["pg"]["schema"], cfg["pg"]["pipeline_schema"]
    root = cfg["lake"]["root"]
    spark = build_spark(cfg, "verify_bronze")
    ok = True

    def report(name: str, passed: bool, detail: str) -> None:
        nonlocal ok
        ok &= passed
        print(f"{name:<36} {'OK' if passed else 'MISMATCH':<9} {detail}", flush=True)

    try:
        with connect(cfg["pg"]) as conn:
            book = Bookkeeping(conn, pipeline)
            for table in TRANSACTIONAL_TABLES:
                path = table_path(root, "bronze", SOURCE, table)
                runs = dict(
                    conn.execute(
                        f"SELECT batch_date, row_count FROM {pipeline}.ingest_runs "
                        "WHERE source = %s AND table_name = %s",
                        (SOURCE, table),
                    ).fetchall()
                )
                counts = partition_counts(spark, path)
                parts_ok = {d: n for d, n in runs.items() if n} == counts

                spec = TABLES[table]
                cols = list(spec.column_names) + ["updated_at"]
                latest = (
                    spark.read.parquet(path)
                    .withColumn(
                        "_rn", F.row_number().over(W.partitionBy(*spec.key).orderBy(F.col("updated_at").desc()))
                    )
                    .filter("_rn = 1")
                    .select(cols)
                )
                wm = book.watermark(SOURCE, table)
                source = read_jdbc(
                    spark, cfg, query=(f'SELECT * FROM "{schema}"."{table}" WHERE updated_at <= TIMESTAMP \'{wm}\'')
                ).select(cols)
                n_latest, n_source = latest.count(), source.count()
                same = (
                    n_latest == n_source and latest.exceptAll(source).isEmpty() and source.exceptAll(latest).isEmpty()
                )
                report(
                    table,
                    parts_ok and same,
                    f"{len(counts)} partitions, {sum(counts.values())} rows, "
                    f"{n_latest} latest per key vs {n_source} in Postgres (<= {wm})",
                )

            for table in REFERENCE_TABLES:
                row = conn.execute(
                    f"SELECT batch_date, row_count FROM {pipeline}.reference_snapshots "
                    "WHERE source = %s AND table_name = %s ORDER BY batch_date DESC LIMIT 1",
                    (SOURCE, table),
                ).fetchone()
                if row is None:
                    report(table, False, "no snapshot")
                    continue
                snap = (
                    spark.read.parquet(table_path(root, "bronze", SOURCE, table))
                    .filter(F.col(PARTITION_COLUMN) == row[0])
                    .select(*TABLES[table].column_names)
                )
                source = read_jdbc(spark, cfg, dbtable=f'"{schema}"."{table}"')
                same = snap.count() == source.count() and snap.exceptAll(source).isEmpty()
                report(table, same, f"latest snapshot {row[0]} with {row[1]} rows")

        path = table_path(root, "bronze", FILES_SOURCE, FILES_TABLE)
        counts = partition_counts(spark, path)
        bad = [
            d
            for d, n in counts.items()
            if n != len((partition_dir(cfg["paths"]["landing_dir"], d) / "changes.jsonl").read_text().splitlines())
        ]
        report(
            FILES_TABLE,
            not bad,
            f"{len(counts)} partitions, {sum(counts.values())} rows" + (f", wrong: {bad}" if bad else ""),
        )

        api_root = cfg["paths"]["landing_dir"] / "customer_activity"
        expected = (
            {
                d: json.loads((landing_dir_for(cfg["paths"]["landing_dir"], d) / "page_0001.json").read_text())[
                    "total_records"
                ]
                for d in (date.fromisoformat(p.name[3:]) for p in api_root.glob("dt=*"))
            }
            if api_root.is_dir()
            else {}
        )
        counts = partition_counts(spark, table_path(root, "bronze", API_SOURCE, API_TABLE)) if expected else {}
        bad = sorted(d for d in expected.keys() | counts.keys() if counts.get(d, 0) != expected.get(d))
        report(
            API_TABLE,
            not bad,
            f"{len(counts)} partitions, {sum(counts.values())} rows" + (f", wrong: {bad}" if bad else ""),
        )
    finally:
        spark.stop()
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
