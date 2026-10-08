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


def _fs_path(spark: SparkSession, path: str):
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    return hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), hpath


def path_exists(spark: SparkSession, path: str) -> bool:
    fs, hpath = _fs_path(spark, path)
    return fs.exists(hpath)


def delete_path(spark: SparkSession, path: str) -> None:
    """Recursive delete through Hadoop's FileSystem, so it works for file:// and s3a:// alike."""
    fs, hpath = _fs_path(spark, path)
    if fs.exists(hpath):
        fs.delete(hpath, True)


def list_dirs(spark: SparkSession, path: str) -> list[str]:
    """Names of the sub-directories of `path` (e.g. partition dirs), skipping _/. prefixed ones."""
    fs, hpath = _fs_path(spark, path)
    if not fs.exists(hpath):
        return []
    names = [s.getPath().getName() for s in fs.listStatus(hpath) if s.isDirectory()]
    return sorted(n for n in names if not n.startswith(("_", ".")))


def move_path(spark: SparkSession, src: str, dst: str) -> None:
    """Replace `dst` with `src` (delete, then rename). On S3 the rename is a copy."""
    fs, hdst = _fs_path(spark, dst)
    _, hsrc = _fs_path(spark, src)
    if fs.exists(hdst):
        fs.delete(hdst, True)
    fs.mkdirs(hdst.getParent())
    if not fs.rename(hsrc, hdst):
        raise OSError(f"could not move {src} to {dst}")


def overwrite_partition(spark: SparkSession, df: DataFrame, table_dir: str, column: str, value: date) -> int:
    """Replace partition column=value of table_dir with df (which must hold only that value).

    Uses dynamic partition overwrite. An empty df writes no files, which would leave a
    stale partition behind on a rerun, so that case clears the partition instead.
    """
    part = f"{table_dir}/{column}={value.isoformat()}"
    if df.isEmpty():
        delete_path(spark, part)
        return 0
    (df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(column)
        .parquet(table_dir))
    return spark.read.parquet(part).count()


def write_partition(spark: SparkSession, df: DataFrame, table_dir: str, batch_date: date,
                    source: str, ingested_at: datetime) -> int:
    """Add the Bronze metadata and replace partition `batch_date` of `table_dir` (and nothing else)."""
    out = add_metadata(df, batch_date, source, ingested_at)
    return overwrite_partition(spark, out, table_dir, PARTITION_COLUMN, batch_date)
