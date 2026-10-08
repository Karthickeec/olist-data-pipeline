"""SparkSession and JDBC settings from config/pipeline.yaml."""
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

UTC_JVM = "-Duser.timezone=UTC"


def build_spark(cfg: dict, app_name: str = "olist-pipeline") -> SparkSession:
    """Local Spark session.

    Session and JVM time zones are pinned to UTC: Postgres `timestamp` columns
    have no zone, and any other setting shifts them by the machine's offset.
    """
    s = cfg["spark"]
    if s.get("java_home") and Path(s["java_home"]).is_dir():
        os.environ["JAVA_HOME"] = s["java_home"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName(app_name)
        .master(s["master"])
        .config("spark.driver.memory", s["driver_memory"])
        .config("spark.sql.shuffle.partitions", str(s["shuffle_partitions"]))
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", s["jdbc_package"])
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", UTC_JVM)
        .config("spark.executor.extraJavaOptions", UTC_JVM)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        # Unparseable timestamps become null instead of raising (Silver quarantines them).
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_jdbc(spark: SparkSession, cfg: dict, **options) -> DataFrame:
    """Read from the source Postgres; pass `query=` or `dbtable=` (+ partitioning options)."""
    pg = cfg["pg"]
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{pg['host']}:{pg['port']}/{pg['dbname']}")
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", str(cfg["spark"]["jdbc_fetchsize"]))
        .options(**{k: str(v) for k, v in options.items()})
        .load()
    )
