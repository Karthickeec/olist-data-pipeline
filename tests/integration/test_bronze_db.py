"""Bronze against a real Postgres: throwaway schemas, lake and landing in a temp dir."""
import copy
import shutil
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import psycopg
import pytest
from pyspark.sql import Window as W
from pyspark.sql import functions as F

from conftest import assert_same_rows
from olist_pipeline.bronze import utc_now
from olist_pipeline.bronze.files_to_bronze import ingest_customer_changes
from olist_pipeline.bronze.postgres_to_bronze import SOURCE, ingest_reference, run
from olist_pipeline.config import load_config
from olist_pipeline.db import apply_schema, connect
from olist_pipeline.lake import partition_path, table_path
from olist_pipeline.replay import replay_day
from olist_pipeline.replay_logic import build_index, end_of_day
from olist_pipeline.seed import seed_reference
from olist_pipeline.sources import TABLES, TRANSACTIONAL_TABLES, load_sources, read_address_pool
from olist_pipeline.spark import read_jdbc
from olist_pipeline.watermarks import Bookkeeping

pytestmark = pytest.mark.integration
SCHEMA, PIPELINE = "olist_bronze_test", "pipeline_bronze_test"
REFERENCE = ("product_category_name_translation", "products", "sellers")  # geolocation: 1M rows, skipped
DAYS = (date(2017, 3, 1), date(2017, 3, 2), date(2017, 3, 3))
INSERT_ONLY = ("customers", "order_items", "order_payments", "order_reviews")


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    cfg = copy.deepcopy(load_config())
    tmp = tmp_path_factory.mktemp("bronze")
    cfg["pg"]["schema"], cfg["pg"]["pipeline_schema"] = SCHEMA, PIPELINE
    cfg["lake"]["root"] = str(tmp / "lake")
    cfg["paths"]["landing_dir"] = tmp / "landing"
    try:
        conn = connect(cfg["pg"])
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    for s in (SCHEMA, PIPELINE, "pipeline_ref_test"):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    apply_schema(conn, SCHEMA)
    seed_reference(conn, cfg["paths"]["raw_dir"], tables=REFERENCE)
    src = load_sources(cfg["paths"]["raw_dir"])
    yield SimpleNamespace(cfg=cfg, conn=conn, src=src, idx=build_index(src),
                          pool=read_address_pool(cfg["paths"]["raw_dir"]), tmp=tmp)
    for s in (SCHEMA, PIPELINE, "pipeline_ref_test"):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    conn.close()


def replay(env, day):
    replay_day(env.conn, env.src, env.idx, day, env.cfg["paths"]["landing_dir"], env.pool,
               env.cfg["customer_changes"])


def bronze(spark, env, day) -> dict:
    results = run(spark, env.cfg, day, reference=REFERENCE)
    results.append(ingest_customer_changes(spark, env.cfg, day, utc_now()))
    return {r.table: r for r in results}


def bronze_dir(env, table):
    if table == "customer_changes":
        return table_path(env.cfg["lake"]["root"], "bronze", "crm", table)
    return table_path(env.cfg["lake"]["root"], "bronze", SOURCE, table)


def read_partition(spark, path: Path):
    """A partition without _ingested_at, or None when the batch was empty."""
    return spark.read.parquet(str(path)).drop("_ingested_at") if path.exists() else None


def ingest_run(env, table, day):
    return env.conn.execute(
        f"SELECT low_wm, high_wm, row_count FROM {PIPELINE}.ingest_runs "
        "WHERE source = %s AND table_name = %s AND batch_date = %s", (SOURCE, table, day)).fetchone()


def pg_count(env, table, low, high):
    where = "updated_at <= %s" + ("" if low is None else " AND updated_at > %s")
    params = (high,) if low is None else (high, low)
    return env.conn.execute(f'SELECT count(*) FROM "{table}" WHERE {where}', params).fetchone()[0]


def test_three_days_then_rerun_middle_day(spark, env):
    # Daily cycle: the source moves one day, then Bronze ingests that day.
    for day in DAYS:
        replay(env, day)
        res = bronze(spark, env, day)
        for table in TRANSACTIONAL_TABLES:
            low, high, rows = ingest_run(env, table, day)
            assert high == end_of_day(day)
            # Partition count == Postgres rows in the window, counted before the source moves on.
            assert res[table].rows == rows == pg_count(env, table, low, high), table
            assert spark.read.parquet(partition_path(bronze_dir(env, table), day)).count() == rows
        landing = env.cfg["paths"]["landing_dir"] / "customer_changes" / f"dt={day}" / "changes.jsonl"
        assert res["customer_changes"].rows == len(landing.read_text().splitlines())

    # Windows chain: initial load has no lower bound, each later batch starts where the last ended.
    assert ingest_run(env, "orders", DAYS[0])[0] is None
    assert ingest_run(env, "orders", DAYS[1])[0] == end_of_day(DAYS[0])
    assert ingest_run(env, "orders", DAYS[2])[0] == end_of_day(DAYS[1])

    # Keep a copy of day 2 as first written, then rerun day 2 after day 3.
    # An empty batch has no partition (e.g. a day without change requests): kept as None.
    saved = {}
    for table in TRANSACTIONAL_TABLES + ("customer_changes",):
        part = Path(partition_path(bronze_dir(env, table), DAYS[1]))
        dst = env.tmp / "saved" / table
        if part.exists():
            shutil.copytree(part, dst)
        saved[table] = read_partition(spark, dst)
    book = Bookkeeping(env.conn, PIPELINE)
    watermarks = {t: book.watermark(SOURCE, t) for t in TRANSACTIONAL_TABLES}

    bronze(spark, env, DAYS[1])
    rerun = {t: read_partition(spark, Path(partition_path(bronze_dir(env, t), DAYS[1]))) for t in saved}

    # Insert-only tables and the files: identical apart from _ingested_at.
    for table in INSERT_ONLY + ("customer_changes",):
        if saved[table] is None:
            assert rerun[table] is None, table
        else:
            assert_same_rows(saved[table], rerun[table])
    # Orders: identical minus the orders that changed again on day 3 (they now live in day 3).
    moved = [r[0] for r in env.conn.execute(
        "SELECT order_id FROM orders WHERE updated_at > %s", (end_of_day(DAYS[1]),))]
    expected = saved["orders"].filter(~F.col("order_id").isin(moved))
    assert expected.count() < saved["orders"].count(), "expected some day-2 orders to move to day 3"
    assert_same_rows(expected, rerun["orders"])
    # The rerun reused its window and did not move any watermark.
    assert ingest_run(env, "orders", DAYS[1])[0] == end_of_day(DAYS[0])
    assert {t: book.watermark(SOURCE, t) for t in TRANSACTIONAL_TABLES} == watermarks

    # Nothing is lost: latest version per key across all partitions == Postgres.
    for table in TRANSACTIONAL_TABLES:
        spec = TABLES[table]
        cols = list(spec.column_names) + ["updated_at"]
        latest = (spark.read.parquet(bronze_dir(env, table))
                  .withColumn("_rn", F.row_number().over(
                      W.partitionBy(*spec.key).orderBy(F.col("updated_at").desc())))
                  .filter("_rn = 1").select(cols))
        source = read_jdbc(spark, env.cfg, query=f'SELECT * FROM "{SCHEMA}"."{table}"').select(cols)
        assert_same_rows(latest, source)


def test_reference_snapshot_written_skipped_and_rewritten(spark, env):
    cfg = copy.deepcopy(env.cfg)
    cfg["lake"]["root"] = str(env.tmp / "ref_lake")
    book = Bookkeeping(env.conn, "pipeline_ref_test")
    book.ensure_schema()
    d1, d2, d3 = DAYS
    sellers = table_path(cfg["lake"]["root"], "bronze", SOURCE, "sellers")

    first = ingest_reference(spark, cfg, env.conn, book, "sellers", d1, utc_now())
    assert (first.action, first.rows) == ("written", 3095)

    second = ingest_reference(spark, cfg, env.conn, book, "sellers", d2, utc_now())
    assert (second.action, second.detail) == ("skipped", "unchanged, skipped")
    assert not (env.tmp / "ref_lake/bronze/olist_postgres/sellers/ingest_date=2017-03-02").exists()

    rerun = ingest_reference(spark, cfg, env.conn, book, "sellers", d1, utc_now())
    assert rerun.action == "skipped"
    assert spark.read.parquet(partition_path(sellers, d1)).count() == 3095

    seller_id, city = env.conn.execute(
        "SELECT seller_id, seller_city FROM sellers ORDER BY seller_id LIMIT 1").fetchone()
    env.conn.execute("UPDATE sellers SET seller_city = 'renamed' WHERE seller_id = %s", (seller_id,))
    try:
        third = ingest_reference(spark, cfg, env.conn, book, "sellers", d3, utc_now())
    finally:
        env.conn.execute("UPDATE sellers SET seller_city = %s WHERE seller_id = %s", (city, seller_id))
    assert (third.action, third.rows) == ("written", 3095)
    assert spark.read.parquet(partition_path(sellers, d3)).filter(
        F.col("seller_city") == "renamed").count() == 1

    snapshots = env.conn.execute(
        "SELECT batch_date FROM pipeline_ref_test.reference_snapshots ORDER BY batch_date").fetchall()
    assert [r[0] for r in snapshots] == [d1, d3]


def test_window_after_gap_and_rerun(env):
    book = Bookkeeping(env.conn, "pipeline_ref_test")
    book.ensure_schema()
    d1, d2 = date(2018, 1, 1), date(2018, 1, 2)
    for day in (d1, d2):
        book.record_run("t", "x", day, book.window("t", "x", day), 0)
    assert book.window("t", "x", date(2018, 1, 5)).low == end_of_day(d2)   # catch-up after a gap
    assert book.window("t", "x", d2).low == end_of_day(d1)                  # rerun reuses its window
    book.record_run("t", "x", d1, book.window("t", "x", d1), 0)            # rerun of an older day
    assert book.watermark("t", "x") == end_of_day(d2)                       # never moves back
