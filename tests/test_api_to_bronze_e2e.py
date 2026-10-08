"""End to end over real HTTP: uvicorn in a thread, failure injection on, Spark writing Bronze."""

import copy
import json
import logging
import shutil
import socket
import threading
import time
from datetime import date

import pytest
import uvicorn
from conftest import assert_same_rows

from olist_pipeline.api.app import ServerSettings, create_app
from olist_pipeline.api.client import landing_dir_for
from olist_pipeline.bronze import utc_now
from olist_pipeline.bronze.api_to_bronze import fetch_and_land, load_to_bronze, make_client
from olist_pipeline.config import load_config
from olist_pipeline.lake import partition_path, table_path

KEY = "e2e-key"
DAYS = (date(2018, 3, 1), date(2018, 3, 2), date(2018, 3, 3))


@pytest.fixture(scope="module")
def api_url(activity_index):
    # Retry-After 0 keeps the test fast; the rates are high so every run sees retries.
    app = create_app(
        activity_index, KEY, ServerSettings(rate_limit_rate=0.15, error_rate=0.10, retry_after_seconds=0, seed=7)
    )
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        assert time.monotonic() < deadline, "uvicorn did not start"
        time.sleep(0.05)
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=5)


@pytest.fixture
def cfg(api_url, tmp_path, monkeypatch):
    monkeypatch.setenv("OLIST_API_KEY", KEY)
    cfg = copy.deepcopy(load_config())
    cfg["api"]["base_url"] = api_url
    cfg["api"]["backoff_base_seconds"] = 0.01
    cfg["paths"]["landing_dir"] = tmp_path / "landing"
    cfg["lake"]["root"] = str(tmp_path / "lake")
    return cfg


def run_day(spark, cfg, day):
    client = make_client(cfg)
    with client.http:
        stats = fetch_and_land(client, cfg, day)
    return stats, load_to_bronze(spark, cfg, day, utc_now())


def test_three_days_then_rerun_middle_day(spark, cfg, tmp_path, caplog):
    out = table_path(cfg["lake"]["root"], "bronze", "api", "customer_activity")
    retries = 0
    with caplog.at_level(logging.WARNING, logger="olist_pipeline.api.client"):
        for day in DAYS:
            stats, result = run_day(spark, cfg, day)
            retries += stats.retries
            pages = sorted(landing_dir_for(cfg["paths"]["landing_dir"], day).iterdir())
            first = json.loads(pages[0].read_text())
            assert [p.name for p in pages] == [f"page_{n:04d}.json" for n in range(1, first["total_pages"] + 1)]
            assert result.rows == stats.records == first["total_records"] > 100
            assert spark.read.parquet(partition_path(out, day)).count() == result.rows
    assert retries > 0 and any("HTTP 429" in r.getMessage() for r in caplog.records)

    middle = DAYS[1]
    saved_landing = {p.name: p.read_bytes() for p in landing_dir_for(cfg["paths"]["landing_dir"], middle).iterdir()}
    shutil.copytree(partition_path(out, middle), tmp_path / "saved")
    before = spark.read.parquet(str(tmp_path / "saved")).drop("_ingested_at")

    run_day(spark, cfg, middle)
    after = spark.read.parquet(partition_path(out, middle)).drop("_ingested_at")
    assert_same_rows(before, after)
    assert {
        p.name: p.read_bytes() for p in landing_dir_for(cfg["paths"]["landing_dir"], middle).iterdir()
    } == saved_landing
    assert spark.read.parquet(out).select("ingest_date").distinct().count() == 3


def test_dirty_records_land_raw(spark, cfg):
    _, result = run_day(spark, cfg, DAYS[0])
    df = spark.read.parquet(
        partition_path(table_path(cfg["lake"]["root"], "bronze", "api", "customer_activity"), DAYS[0])
    )
    assert set(df.columns) >= {
        "customer_unique_id",
        "sessions",
        "last_seen_at",
        "device",
        "_page",
        "_source_file",
        "_corrupt_record",
        "_batch_date",
        "_source",
    }
    dirty = df.filter(
        "sessions LIKE '-%' OR last_seen_at LIKE '%/%' OR page_views IS NULL OR device IS NULL OR last_seen_at IS NULL"
    ).count()
    assert 0 < dirty < result.rows * 0.05
    assert df.filter("_corrupt_record IS NOT NULL").count() == 0
    assert dict(df.dtypes)["sessions"] == "string"  # raw as sent, not cast


def test_corrupt_page_is_kept(spark, cfg):
    run_day(spark, cfg, DAYS[0])
    folder = landing_dir_for(cfg["paths"]["landing_dir"], DAYS[0])
    (folder / "page_0099.json").write_text('{"page": 99, "data": [ {"customer_unique_id": ')
    load_to_bronze(spark, cfg, DAYS[0], utc_now())
    df = spark.read.parquet(
        partition_path(table_path(cfg["lake"]["root"], "bronze", "api", "customer_activity"), DAYS[0])
    )
    [bad] = df.filter("_corrupt_record IS NOT NULL").collect()
    assert bad._source_file.endswith("page_0099.json") and bad.customer_unique_id is None
