from datetime import date, datetime
from pathlib import Path

from pyspark.sql import functions as F

from conftest import assert_same_rows
from olist_pipeline.config import PROJECT_ROOT, resolve_uri
from olist_pipeline.lake import partition_path, table_path, write_partition
from olist_pipeline.watermarks import Window

D1, D2 = date(2017, 3, 1), date(2017, 3, 2)
RUN = datetime(2026, 10, 8, 12, 0, 0)


def test_resolve_uri():
    assert resolve_uri("data/lake/") == str(PROJECT_ROOT / "data" / "lake")
    assert resolve_uri("s3://bucket/lake/") == "s3://bucket/lake"
    assert table_path("s3://b/lake", "bronze", "crm", "t") == "s3://b/lake/bronze/crm/t"
    assert partition_path("s3://b/lake/bronze/crm/t", D1) == "s3://b/lake/bronze/crm/t/ingest_date=2017-03-01"


def test_window_predicate():
    high = datetime(2017, 3, 2, 23, 59, 59)
    assert Window(None, high).predicate() == "updated_at <= TIMESTAMP '2017-03-02 23:59:59'"
    assert Window(datetime(2017, 3, 1, 23, 59, 59), high).predicate() == (
        "updated_at > TIMESTAMP '2017-03-01 23:59:59' AND updated_at <= TIMESTAMP '2017-03-02 23:59:59'")


def _df(spark, rows):
    return spark.createDataFrame(rows, "id int, name string")


def test_metadata_columns(spark, tmp_path):
    out = str(tmp_path / "t")
    assert write_partition(spark, _df(spark, [(1, "a")]), out, D1, "test:src", RUN) == 1
    # Format inside Spark (UTC session): collect() would convert timestamps to the local zone.
    row = (spark.read.parquet(out)
           .withColumn("_ingested_at", F.date_format("_ingested_at", "yyyy-MM-dd HH:mm:ss"))
           .first().asDict())
    assert row == {"id": 1, "name": "a", "_ingested_at": "2026-10-08 12:00:00", "_batch_date": D1,
                   "_source": "test:src", "ingest_date": D1}
    assert (Path(out) / "ingest_date=2017-03-01").is_dir()


def test_rewrite_replaces_only_its_partition(spark, tmp_path):
    out = str(tmp_path / "t")
    write_partition(spark, _df(spark, [(1, "a"), (2, "b")]), out, D1, "s", RUN)
    write_partition(spark, _df(spark, [(3, "c")]), out, D2, "s", RUN)
    before_d1 = spark.read.parquet(partition_path(out, D1)).collect()

    assert write_partition(spark, _df(spark, [(4, "d")]), out, D2, "s", RUN) == 1
    assert spark.read.parquet(partition_path(out, D1)).collect() == before_d1
    assert [r.id for r in spark.read.parquet(partition_path(out, D2)).collect()] == [4]


def test_rerun_with_same_input_is_identical(spark, tmp_path):
    out = str(tmp_path / "t")
    rows = [(i, f"n{i}") for i in range(50)]
    write_partition(spark, _df(spark, rows), out, D1, "s", RUN)
    df = spark.read.parquet(out).drop("_ingested_at")
    first = spark.createDataFrame(df.collect(), df.schema)
    write_partition(spark, _df(spark, rows), out, D1, "s", datetime(2026, 10, 9))
    assert_same_rows(first, spark.read.parquet(out).drop("_ingested_at"))


def test_empty_batch_clears_partition(spark, tmp_path):
    out = str(tmp_path / "t")
    write_partition(spark, _df(spark, [(1, "a")]), out, D1, "s", RUN)
    write_partition(spark, _df(spark, [(2, "b")]), out, D2, "s", RUN)
    assert write_partition(spark, _df(spark, []), out, D2, "s", RUN) == 0
    assert not (Path(out) / "ingest_date=2017-03-02").exists()
    assert (Path(out) / "ingest_date=2017-03-01").is_dir()
