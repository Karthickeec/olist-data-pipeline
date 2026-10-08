"""Lake paths and partition writes. Paths are plain strings so the root can be s3://."""
from datetime import date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PARTITION_COLUMN = "ingest_date"
METADATA_COLUMNS = ("_ingested_at", "_batch_date", "_source")


def table_path(root: str, layer: str, source: str, table: str) -> str:
    return f"{root}/{layer}/{source}/{table}"


def partition_path(table_dir: str, day: date) -> str:
    return f"{table_dir}/{PARTITION_COLUMN}={day.isoformat()}"


def add_metadata(df: DataFrame, batch_date: date, source: str, ingested_at: datetime) -> DataFrame:
    """Append the Bronze metadata columns and the partition column. Source columns are untouched."""
    return (
        df.withColumn("_ingested_at", F.lit(ingested_at.isoformat(sep=" ")).cast("timestamp"))
        .withColumn("_batch_date", F.lit(batch_date.isoformat()).cast("date"))
        .withColumn("_source", F.lit(source))
        .withColumn(PARTITION_COLUMN, F.lit(batch_date.isoformat()).cast("date"))
    )


def delete_path(spark: SparkSession, path: str) -> None:
    """Recursive delete through Hadoop's FileSystem, so it works for file:// and s3a:// alike."""
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if fs.exists(hpath):
        fs.delete(hpath, True)


def write_partition(spark: SparkSession, df: DataFrame, table_dir: str, batch_date: date,
                    source: str, ingested_at: datetime) -> int:
    """Replace partition `batch_date` of `table_dir` (and nothing else). Returns rows written.

    Uses dynamic partition overwrite. An empty batch writes no files, which would
    leave a stale partition behind on a rerun, so that case clears the partition.
    """
    out = add_metadata(df, batch_date, source, ingested_at)
    if out.isEmpty():
        delete_path(spark, partition_path(table_dir, batch_date))
        return 0
    (out.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(PARTITION_COLUMN)
        .parquet(table_dir))
    return spark.read.parquet(partition_path(table_dir, batch_date)).count()
