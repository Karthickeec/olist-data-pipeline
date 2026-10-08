"""Silver end to end against a real Postgres: replay -> Bronze (Postgres, files, API) -> Silver."""
import copy
from datetime import date
from types import SimpleNamespace

import psycopg
import pytest
from fastapi.testclient import TestClient
from pyspark.sql import functions as F

from olist_pipeline.api.app import ServerSettings, create_app
from olist_pipeline.bronze import utc_now
from olist_pipeline.bronze.api_to_bronze import fetch_and_land, load_to_bronze, make_client
from olist_pipeline.bronze.files_to_bronze import ingest_customer_changes
from olist_pipeline.bronze.postgres_to_bronze import run as postgres_to_bronze
from olist_pipeline.config import load_config
from olist_pipeline.db import apply_schema, connect
from olist_pipeline.replay import replay_day
from olist_pipeline.replay_logic import build_index
from olist_pipeline.seed import seed_reference
from olist_pipeline.silver import job as silver
from olist_pipeline.silver.common import pending_dir, silver_dir
from olist_pipeline.sources import REFERENCE_TABLES, TABLES, load_sources, read_address_pool
from olist_pipeline.spark import read_jdbc
from olist_pipeline.verify import silver_snapshot

pytestmark = pytest.mark.integration
SCHEMA, PIPELINE = "olist_silver_test", "pipeline_silver_test"
KEY = "silver-test-key"
D1, D2, D3 = date(2017, 3, 1), date(2017, 3, 2), date(2017, 3, 3)


@pytest.fixture(scope="module")
def env(tmp_path_factory, activity_index):
    cfg = copy.deepcopy(load_config())
    tmp = tmp_path_factory.mktemp("silver")
    cfg["pg"]["schema"], cfg["pg"]["pipeline_schema"] = SCHEMA, PIPELINE
    cfg["lake"]["root"] = str(tmp / "lake")
    cfg["paths"]["landing_dir"] = tmp / "landing"
    try:
        conn = connect(cfg["pg"])
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    for s in (SCHEMA, PIPELINE):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    apply_schema(conn, SCHEMA)
    seed_reference(conn, cfg["paths"]["raw_dir"])            # geolocation too: Silver needs it
    src = load_sources(cfg["paths"]["raw_dir"])
    api = TestClient(create_app(activity_index, KEY, ServerSettings()))
    yield SimpleNamespace(cfg=cfg, conn=conn, src=src, idx=build_index(src),
                          pool=read_address_pool(cfg["paths"]["raw_dir"]), api=api)
    for s in (SCHEMA, PIPELINE):
        conn.execute(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    conn.close()


def replay(env, day):
    replay_day(env.conn, env.src, env.idx, day, env.cfg["paths"]["landing_dir"], env.pool,
               env.cfg["customer_changes"])


def bronze(spark, env, day, monkeypatch):
    postgres_to_bronze(spark, env.cfg, day, reference=REFERENCE_TABLES)
    ingest_customer_changes(spark, env.cfg, day, utc_now())
    monkeypatch.setenv("OLIST_API_KEY", KEY)
    fetch_and_land(make_client(env.cfg, http=env.api, sleep=lambda s: None), env.cfg, day)
    load_to_bronze(spark, env.cfg, day, utc_now())


def runs(env, day):
    return {r[0]: r[1:] for r in env.conn.execute(
        f"SELECT table_name, rows_in, rows_valid, rows_quarantined, rows_duplicate, rows_pending "
        f"FROM {PIPELINE}.layer_runs WHERE layer = 'silver' AND batch_date = %s", (day,))}


def test_daily_silver_with_late_arriving_orders_and_rerun(spark, env, monkeypatch):
    root = env.cfg["lake"]["root"]
    replay(env, D1)
    bronze(spark, env, D1, monkeypatch)
    silver.run(spark, env.cfg, D1)

    # The source moves two days before day 2 is extracted: orders bought on day 2 that changed
    # again on day 3 are outside day 2's window, but their items and payments are inside it.
    replay(env, D2)
    replay(env, D3)
    bronze(spark, env, D2, monkeypatch)
    silver.run(spark, env.cfg, D2)
    day2 = runs(env, D2)
    assert day2["order_items"][4] > 0 and day2["order_payments"][4] > 0        # pending, not quarantined
    assert day2["order_items"][2] == 0

    bronze(spark, env, D3, monkeypatch)
    silver.run(spark, env.cfg, D3)
    day3 = runs(env, D3)
    assert day3["order_items"][4] == 0 and day3["order_payments"][4] == 0      # resolved

    # Every batch balances: in = valid + quarantined + duplicate + pending.
    for day in (D1, D2, D3):
        for table, (rows_in, valid, quarantined, duplicate, pending) in runs(env, day).items():
            assert rows_in == valid + quarantined + duplicate + pending, (day, table)

    # Silver equals the source for every Postgres table.
    for table in ("customers", "orders", "order_items", "order_payments", "order_reviews"):
        cols = list(TABLES[table].column_names) + ["updated_at"]
        got = spark.read.parquet(silver_dir(root, table)).select(cols)
        want = read_jdbc(spark, env.cfg, query=f'SELECT * FROM "{SCHEMA}"."{table}"').select(cols)
        assert got.count() == want.count() and got.exceptAll(want).isEmpty(), table
    lines = spark.read.parquet(silver_dir(root, "order_lines"))
    assert lines.count() == spark.read.parquet(silver_dir(root, "order_items")).count()
    assert spark.read.parquet(silver_dir(root, "geolocation")).count() == 19015

    # Rerun the middle day: nothing changes (apart from processing-time columns).
    before = silver_snapshot(spark, root)
    silver.run(spark, env.cfg, D2)
    assert silver_snapshot(spark, root) == before
    assert spark.read.parquet(f"{pending_dir(root, 'order_items')}/batch_date={D2}").count() == day2["order_items"][4]


def test_dirty_crm_and_api_rows_are_fixed_or_quarantined(spark, env):
    root = env.cfg["lake"]["root"]
    activity = spark.read.parquet(silver_dir(root, "customer_activity"))
    assert activity.filter(F.col("sessions") < 0).isEmpty()
    assert activity.filter(F.col("_timestamp_reformatted") & F.col("last_seen_at").isNull()).isEmpty()
    changes = spark.read.parquet(silver_dir(root, "customer_changes"))
    states = {r[0] for r in changes.select("new_state").distinct().collect()}
    assert states and all(len(s) == 2 and s.isupper() for s in states)
    assert changes.filter(F.col("new_city").isNull()).isEmpty()
