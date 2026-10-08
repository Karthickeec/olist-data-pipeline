"""Replay against a real Postgres, in a throwaway schema (olist_test)."""

from datetime import date

import psycopg
import pytest
from conftest import require_raw_csvs

from olist_pipeline.config import load_config
from olist_pipeline.db import apply_schema, connect
from olist_pipeline.replay import daterange, replay_day
from olist_pipeline.replay_logic import build_index
from olist_pipeline.seed import seed_reference
from olist_pipeline.sources import load_sources, read_address_pool
from olist_pipeline.verify import snapshot

pytestmark = pytest.mark.integration
SCHEMA = "olist_test"


@pytest.fixture(scope="module")
def cfg():
    cfg = load_config()
    require_raw_csvs(cfg["paths"]["raw_dir"])
    cfg["pg"]["schema"] = SCHEMA
    return cfg


@pytest.fixture(scope="module")
def conn(cfg):
    try:
        conn = connect(cfg["pg"])
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    apply_schema(conn, SCHEMA)
    # Geolocation is skipped: nothing references it and it is 1M rows.
    seed_reference(conn, cfg["paths"]["raw_dir"], tables=("products", "sellers"))
    yield conn
    conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    conn.close()


@pytest.fixture(scope="module")
def replay(cfg, conn, tmp_path_factory):
    raw_dir = cfg["paths"]["raw_dir"]
    src = load_sources(raw_dir)
    idx = build_index(src)
    pool = read_address_pool(raw_dir)
    landing = tmp_path_factory.mktemp("landing")

    def run(day):
        return replay_day(conn, src, idx, day, landing, pool, cfg["customer_changes"])

    run.landing = landing
    return run


def test_reseed_inserts_nothing(conn, cfg):
    assert seed_reference(conn, cfg["paths"]["raw_dir"], tables=("products", "sellers")) == {
        "products": (32951, 0),
        "sellers": (3095, 0),
    }


def test_rerunning_days_changes_nothing(conn, replay):
    for day in daterange(date(2017, 1, 1), date(2017, 1, 31)):
        replay(day)
    before = snapshot(conn, replay.landing)
    assert before["orders"][0] > 0

    for day in (date(2017, 1, 31), date(2017, 1, 15), date(2017, 1, 1)):
        result = replay(day)
        assert (result.customers, result.orders, result.items, result.payments, result.reviews) == (0,) * 5
    assert snapshot(conn, replay.landing) == before


def test_older_day_cannot_overwrite_newer_state(conn, replay):
    order_id, status_before, updated_before = conn.execute(
        "SELECT order_id, order_status, updated_at FROM orders "
        "WHERE order_status = 'delivered' ORDER BY order_id LIMIT 1"
    ).fetchone()
    purchased = conn.execute(
        "SELECT order_purchase_timestamp::date FROM orders WHERE order_id = %s", (order_id,)
    ).fetchone()[0]
    replay(purchased)
    assert conn.execute("SELECT order_status, updated_at FROM orders WHERE order_id = %s", (order_id,)).fetchone() == (
        status_before,
        updated_before,
    )
