"""postgres_to_bronze: source Postgres -> Bronze Parquet for one batch date.

Transactional tables are extracted incrementally on updated_at, in the window
(previous batch's high watermark, end of batch date]. Watermarks advance only
after the Parquet write succeeded.

Reference tables are snapshotted: their content hash is computed in Postgres
and a new snapshot partition is written only when it differs from the latest
snapshot on or before the batch date (the first run always writes).
"""
from datetime import date, datetime

import psycopg
from pyspark.sql import SparkSession

from olist_pipeline.bronze import TableResult, parse_batch_date, utc_now
from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.lake import table_path, write_partition
from olist_pipeline.sources import REFERENCE_TABLES, TRANSACTIONAL_TABLES
from olist_pipeline.spark import build_spark, read_jdbc
from olist_pipeline.verify import table_fingerprint
from olist_pipeline.watermarks import Bookkeeping

SOURCE = "olist_postgres"
# Integer key columns used to split large reference reads into parallel JDBC queries.
JDBC_PARTITION_COLUMNS = {"geolocation": "geolocation_row_id"}
JDBC_PARTITIONS = 2


def _bronze_dir(cfg: dict, table: str) -> str:
    return table_path(cfg["lake"]["root"], "bronze", SOURCE, table)


def _source_label(cfg: dict, table: str) -> str:
    return f"postgres:{cfg['pg']['schema']}.{table}"


def ingest_incremental(spark: SparkSession, cfg: dict, book: Bookkeeping, table: str,
                       batch_date: date, ingested_at: datetime) -> TableResult:
    window = book.window(SOURCE, table, batch_date)
    query = f'SELECT * FROM "{cfg["pg"]["schema"]}"."{table}" WHERE {window.predicate()}'
    df = read_jdbc(spark, cfg, query=query)
    rows = write_partition(spark, df, _bronze_dir(cfg, table), batch_date,
                           _source_label(cfg, table), ingested_at)
    book.record_run(SOURCE, table, batch_date, window, rows)
    return TableResult(table, "written", rows, f"window {window}")


def ingest_reference(spark: SparkSession, cfg: dict, conn: psycopg.Connection, book: Bookkeeping,
                     table: str, batch_date: date, ingested_at: datetime) -> TableResult:
    source_rows, digest = table_fingerprint(conn, table)
    if book.last_snapshot_hash(SOURCE, table, batch_date) == digest:
        return TableResult(table, "skipped", 0, "unchanged, skipped")

    options = {"dbtable": f'"{cfg["pg"]["schema"]}"."{table}"'}
    if table in JDBC_PARTITION_COLUMNS and source_rows:
        col = JDBC_PARTITION_COLUMNS[table]
        low, high = conn.execute(f'SELECT min("{col}"), max("{col}") FROM "{table}"').fetchone()
        options |= {"partitionColumn": col, "lowerBound": low, "upperBound": high,
                    "numPartitions": JDBC_PARTITIONS}
    df = read_jdbc(spark, cfg, **options)
    rows = write_partition(spark, df, _bronze_dir(cfg, table), batch_date,
                           _source_label(cfg, table), ingested_at)

    # The JDBC read is a separate connection: if the table changed in between, the
    # snapshot is not recorded and the next run rewrites it.
    if rows != source_rows or table_fingerprint(conn, table)[1] != digest:
        raise RuntimeError(f"{table} changed while being snapshotted; rerun the batch")
    book.record_snapshot(SOURCE, table, batch_date, digest, rows)
    return TableResult(table, "written", rows, f"new snapshot, hash {digest[:12]}")


def run(spark: SparkSession, cfg: dict, batch_date: date,
        transactional: tuple[str, ...] = TRANSACTIONAL_TABLES,
        reference: tuple[str, ...] = REFERENCE_TABLES) -> list[TableResult]:
    ingested_at = utc_now()
    results = []
    with connect(cfg["pg"]) as conn:
        book = Bookkeeping(conn, cfg["pg"]["pipeline_schema"])
        book.ensure_schema()
        for table in reference:
            results.append(ingest_reference(spark, cfg, conn, book, table, batch_date, ingested_at))
            print(results[-1], flush=True)
        for table in transactional:
            results.append(ingest_incremental(spark, cfg, book, table, batch_date, ingested_at))
            print(results[-1], flush=True)
    return results


def main(argv=None) -> None:
    batch_date = parse_batch_date(argv, "Ingest the source Postgres into Bronze for one batch date.")
    cfg = load_config()
    spark = build_spark(cfg, "postgres_to_bronze")
    try:
        run(spark, cfg, batch_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
