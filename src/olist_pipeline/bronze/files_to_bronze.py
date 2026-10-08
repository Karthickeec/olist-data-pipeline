"""files_to_bronze: CRM address-change JSONL for one batch date -> Bronze Parquet.

Every line is kept, dirty or not. Fields are read as strings exactly as sent;
lines that are not valid JSON land in _corrupt_record with the other fields null.
"""

from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from olist_pipeline.bronze import TableResult, parse_batch_dates, utc_now
from olist_pipeline.config import load_config
from olist_pipeline.customer_changes import partition_dir
from olist_pipeline.lake import table_path, write_partition
from olist_pipeline.spark import build_spark

SOURCE = "crm"
TABLE = "customer_changes"
CORRUPT = "_corrupt_record"
FIELDS = ("change_id", "customer_unique_id", "new_zip_code_prefix", "new_city", "new_state", "requested_at", "source")
SCHEMA = StructType([StructField(f, StringType()) for f in FIELDS + (CORRUPT,)])


def ingest_customer_changes(spark: SparkSession, cfg: dict, batch_date: date, ingested_at: datetime) -> TableResult:
    src = partition_dir(cfg["paths"]["landing_dir"], batch_date)
    if not src.is_dir():
        raise FileNotFoundError(f"no landing partition for {batch_date}: {src}")
    df = (
        spark.read.schema(SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .json(str(src))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    out_dir = table_path(cfg["lake"]["root"], "bronze", SOURCE, TABLE)
    rows = write_partition(spark, df, out_dir, batch_date, f"file:{TABLE}", ingested_at)
    return TableResult(TABLE, "written", rows, f"from {src}")


def main(argv=None) -> None:
    days = parse_batch_dates(argv, "Ingest the CRM address-change files into Bronze (one partition per day).")
    cfg = load_config()
    spark = build_spark(cfg, "files_to_bronze")
    try:
        for day in days:
            print(ingest_customer_changes(spark, cfg, day, utc_now()), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
