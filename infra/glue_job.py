"""Glue entry point: run one pipeline task (silver, gold, dq_silver, dq_gold) for one batch date on S3.

Arguments: --TASK, --DATE; --LAKE (s3://bucket/lake) and --SUITES (s3://bucket/glue/dq/) are job defaults.
The olist_pipeline wheel is installed with --additional-python-modules. Bookkeeping comes from
CONTROL/state.json (exported from the local Postgres), where CONTROL = s3://<lake bucket>/control/<task>/<date>/,
so callers (Airflow) only pass task and date; what the job records goes to CONTROL/output.json and is
imported locally afterwards.
"""

import json
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

from olist_pipeline.control import FileBook

args = getResolvedOptions(sys.argv, ["TASK", "DATE", "LAKE", "SUITES"])
task, day, lake = args["TASK"], date.fromisoformat(args["DATE"]), args["LAKE"].rstrip("/")
s3 = boto3.client("s3")


def split(uri: str) -> tuple[str, str]:
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def read_json(uri: str) -> dict:
    bucket, key = split(uri)
    return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())


def write_json(uri: str, data: dict) -> None:
    bucket, key = split(uri)
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, default=str).encode())


spark = SparkSession.builder.getOrCreate()
# The same runtime settings as olist_pipeline.spark.build_spark (Glue owns the SparkContext).
for k, v in {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "4",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
}.items():
    spark.conf.set(k, v)

control = f"s3://{split(lake)[0]}/control/{task}/{day.isoformat()}"
book = FileBook(read_json(f"{control}/state.json"))
cfg = {"lake": {"root": lake}, "pg": {"pipeline_schema": "pipeline"}}
t0, summary = time.time(), {}

if task == "silver":
    from olist_pipeline.silver.job import SilverJob

    runs = SilverJob(spark, cfg, book, day).run()
    summary = {t: r.rows_written for t, r in runs.items()}
elif task == "gold":
    from olist_pipeline.gold.job import GoldJob

    GoldJob(spark, cfg, book, day).run()
elif task.startswith("dq_"):
    from olist_pipeline.dq.engine import Runner, record, report
    from olist_pipeline.dq.suite import load_suite

    layer = task[3:]
    bucket, prefix = split(args["SUITES"].rstrip("/"))
    suite_path = Path(tempfile.mkdtemp()) / f"{layer}.yaml"
    s3.download_file(bucket, f"{prefix}/{layer}.yaml", str(suite_path))
    results = Runner(spark, lake, load_suite(layer, suite_path), day, book).run()
    record(book, layer, day, results, {r.table for r in results})
    report(results, layer, day)
    summary = {"checks": len(results), "blocking": sum(r.blocking for r in results)}
else:
    raise SystemExit(f"unknown task {task}")

out = {
    **book.outputs(),
    "task": task,
    "summary": summary,
    "seconds": round(time.time() - t0, 1),
    "spark_version": spark.version,
}
write_json(f"{control}/output.json", out)
print(json.dumps({k: out[k] for k in ("task", "batch_date", "summary", "seconds", "spark_version")}))
