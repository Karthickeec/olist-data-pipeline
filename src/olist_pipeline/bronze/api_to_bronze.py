"""api_to_bronze: customer-activity API -> raw landing pages -> Bronze Parquet, for one batch date.

1. Page through the API (retries/backoff in ActivityClient) and keep every raw body.
2. Land the pages in data/landing/customer_activity/dt=D/ (folder swapped in only when complete).
3. Spark reads the landed pages and writes one row per record to bronze/api/customer_activity.
Record fields are read as strings, exactly as sent; pages that are not valid JSON are kept as
a single row with _corrupt_record.
"""
import logging
import os
import sys
from datetime import date, datetime

import httpx
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from olist_pipeline.api.client import ActivityClient, FetchStats, land_pages, landing_dir_for
from olist_pipeline.bronze import TableResult, parse_batch_date, utc_now
from olist_pipeline.config import load_config
from olist_pipeline.lake import table_path, write_partition
from olist_pipeline.spark import build_spark

SOURCE = "api"
TABLE = "customer_activity"
CORRUPT = "_corrupt_record"
RECORD_FIELDS = ("customer_unique_id", "activity_date", "sessions", "page_views", "cart_adds",
                 "support_tickets", "last_seen_at", "device")
PAGE_SCHEMA = StructType([
    StructField("page", LongType()),
    StructField("data", ArrayType(StructType([StructField(f, StringType()) for f in RECORD_FIELDS]))),
    StructField(CORRUPT, StringType()),
])


def make_client(cfg: dict, http: httpx.Client | None = None, **overrides) -> ActivityClient:
    api = cfg["api"]
    api_key = os.environ.get(api["key_env"])
    if not api_key:
        raise SystemExit(f"{api['key_env']} is not set")
    http = http or httpx.Client(base_url=api["base_url"], timeout=api["timeout_seconds"])
    return ActivityClient(http, api_key, page_size=api["page_size"], max_attempts=api["max_attempts"],
                          backoff_base=api["backoff_base_seconds"], backoff_max=api["backoff_max_seconds"],
                          **overrides)


def fetch_and_land(client: ActivityClient, cfg: dict, batch_date: date) -> FetchStats:
    pages = client.fetch_day(batch_date)
    land_pages(cfg["paths"]["landing_dir"], batch_date, pages)
    return client.stats


def load_to_bronze(spark: SparkSession, cfg: dict, batch_date: date, ingested_at: datetime) -> TableResult:
    src = landing_dir_for(cfg["paths"]["landing_dir"], batch_date)
    if not src.is_dir():
        raise FileNotFoundError(f"no landed pages for {batch_date}: {src}")
    pages = (
        spark.read.schema(PAGE_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT)
        .json(str(src))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )
    records = (
        pages.select(F.explode_outer("data").alias("r"), F.col("page").alias("_page"),
                     "_source_file", CORRUPT)
        # explode_outer keeps unparseable pages (data is null); drop the null row of empty pages.
        .filter(F.col("r").isNotNull() | F.col(CORRUPT).isNotNull())
        .select(*[F.col(f"r.{f}").alias(f) for f in RECORD_FIELDS], "_page", "_source_file", CORRUPT)
    )
    out_dir = table_path(cfg["lake"]["root"], "bronze", SOURCE, TABLE)
    rows = write_partition(spark, records, out_dir, batch_date, "api:customer-activity", ingested_at)
    return TableResult(TABLE, "written", rows, f"from {src}")


def main(argv=None) -> None:
    batch_date = parse_batch_date(argv, "Fetch the customer-activity API for one date into Bronze.")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    cfg = load_config()
    client = make_client(cfg)
    with client.http:
        stats = fetch_and_land(client, cfg, batch_date)

    spark = build_spark(cfg, "api_to_bronze")
    try:
        result = load_to_bronze(spark, cfg, batch_date, utc_now())
    finally:
        spark.stop()
    print(result, flush=True)
    print(f"api_to_bronze {batch_date}: pages={stats.pages} records={stats.records} "
          f"retries={stats.retries} total_wait={stats.wait_seconds:.1f}s bronze_rows={result.rows}", flush=True)


if __name__ == "__main__":
    main()
