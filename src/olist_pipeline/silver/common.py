"""Shared Silver machinery: Bronze range reads, latest-per-key, quarantine split, merge + swap.

Merge on plain Parquet (no MERGE statement): read the Silver partitions the new rows
touch, union, keep the latest row per key, write the result to a staging directory,
then swap each staged partition into place. Unpartitioned tables are swapped whole.
A crash between swaps can leave a partition missing; `--full-refresh` rebuilds any
table from Bronze, which stays the source of truth (Iceberg would make this atomic).
"""
from dataclasses import dataclass
from datetime import date, datetime

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from olist_pipeline.lake import (PARTITION_COLUMN, delete_path, list_dirs, move_path,
                                 overwrite_partition, path_exists)

BRONZE_INGEST_DATE = "_bronze_ingest_date"
REASON = "_reason"
FIRST_SEEN = "_first_seen_batch"
FIRST_BATCH = "_first_batch_date"


def silver_dir(root: str, table: str) -> str:
    return f"{root}/silver/{table}"


def quarantine_dir(root: str, table: str) -> str:
    return f"{root}/silver/_quarantine/{table}"


def pending_dir(root: str, table: str) -> str:
    return f"{root}/silver/_pending/{table}"


def staging_dir(root: str, table: str) -> str:
    return f"{root}/silver/_staging/{table}"


def read_bronze(spark: SparkSession, path: str, low: date | None, high: date) -> DataFrame | None:
    """Bronze partitions with ingest_date in (low, high], or None if the table does not exist yet."""
    if not path_exists(spark, path) or not list_dirs(spark, path):
        return None
    df = spark.read.parquet(path).filter(F.col(PARTITION_COLUMN) <= F.lit(high))
    if low is not None:
        df = df.filter(F.col(PARTITION_COLUMN) > F.lit(low))
    return df.withColumnRenamed(PARTITION_COLUMN, BRONZE_INGEST_DATE)


def latest_snapshot(spark: SparkSession, path: str, high: date) -> DataFrame | None:
    """The newest reference snapshot partition on or before `high`."""
    dates = [d.split("=", 1)[1] for d in list_dirs(spark, path)]
    dates = [d for d in dates if d <= high.isoformat()]
    if not dates:
        return None
    return spark.read.parquet(f"{path}/{PARTITION_COLUMN}={max(dates)}")


def latest_per_key(df: DataFrame, key: list[str], order: list[Column]) -> DataFrame:
    """Keep one row per key: the first by `order` (callers pass a fully deterministic order)."""
    w = Window.partitionBy(*key).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def split_quarantine(df: DataFrame, rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Tag each row with the first failing rule's name; return (valid rows, quarantined rows)."""
    reason = F.lit(None).cast("string")
    for name, failed in reversed(rules):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    tagged = df.withColumn(REASON, reason)
    return tagged.filter(F.col(REASON).isNull()).drop(REASON), tagged.filter(F.col(REASON).isNotNull())


def add_silver_metadata(df: DataFrame, batch_date: date, processed_at: datetime) -> DataFrame:
    """_batch_date: batch that wrote this version; _first_batch_date: batch that first wrote the key
    (kept as the minimum by merge_into); _processed_at: wall clock."""
    batch = F.lit(batch_date.isoformat()).cast("date")
    return (df.withColumn("_batch_date", batch)
              .withColumn(FIRST_BATCH, batch)
              .withColumn("_processed_at", F.lit(processed_at.isoformat(sep=" ")).cast("timestamp")))


def write_quarantine(spark: SparkSession, df: DataFrame, root: str, table: str, batch_date: date) -> int:
    """Replace this batch's quarantine partition (cleared when nothing was quarantined)."""
    out = df.withColumn("batch_date", F.lit(batch_date.isoformat()).cast("date"))
    return overwrite_partition(spark, out, quarantine_dir(root, table), "batch_date", batch_date)


@dataclass
class MergeResult:
    rows_written: int
    partitions: list[str]


def merge_into(spark: SparkSession, new: DataFrame, root: str, table: str, key: list[str],
               order: list[Column], partition_col: str | None = None,
               full_refresh: bool = False, union_existing: bool = True) -> MergeResult:
    """Merge `new` into silver/<table>: latest row per key, idempotent, partition-scoped.

    `new` must have the table's full Silver schema. With full_refresh the existing table
    is ignored and replaced entirely. With union_existing=False the touched partitions are
    replaced by `new` alone (for tables recomputed whole partitions at a time).
    """
    target, staging = silver_dir(root, table), staging_dir(root, table)
    exists = path_exists(spark, target) and not full_refresh and union_existing
    if partition_col:
        parts = sorted(r[0] for r in new.select(partition_col).distinct().collect())
        if not parts:
            return MergeResult(0, [])
        combined = new
        if exists:
            existing = (spark.read.parquet(target)
                        .filter(F.col(partition_col).isin(parts)).select(*new.columns))
            combined = new.unionByName(existing)
    else:
        parts = []
        combined = new.unionByName(spark.read.parquet(target).select(*new.columns)) if exists else new

    if FIRST_BATCH in combined.columns:
        # Keep the batch that first wrote each key, so later batches can do as-of lookups.
        combined = combined.withColumn(FIRST_BATCH, F.min(FIRST_BATCH).over(Window.partitionBy(*key)))
    merged = latest_per_key(combined, key, order)
    delete_path(spark, staging)
    if partition_col:
        # One file per partition: daily partitions are small, so avoid many tiny files.
        merged.repartition(partition_col).write.partitionBy(partition_col).parquet(staging)
    else:
        merged.coalesce(1).write.parquet(staging)
    rows = spark.read.parquet(staging).count()

    if partition_col and not full_refresh:
        for part in list_dirs(spark, staging):
            move_path(spark, f"{staging}/{part}", f"{target}/{part}")
        delete_path(spark, staging)
    else:
        move_path(spark, staging, target)
    return MergeResult(rows, [str(p) for p in parts])
