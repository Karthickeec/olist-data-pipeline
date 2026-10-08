import json
import logging
import random
from datetime import date, datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
from fastapi.testclient import TestClient

from olist_pipeline.api.app import ServerSettings, create_app
from olist_pipeline.api.client import ActivityClient, ApiError, land_pages, landing_dir_for, parse_retry_after
from olist_pipeline.bronze.api_to_bronze import fetch_and_land

KEY = "test-key"
DAY = date(2018, 3, 1)


def make_client(index, settings, key=KEY, **kwargs):
    waits = []
    http = TestClient(create_app(index, KEY, settings))
    client = ActivityClient(http, key, page_size=100, sleep=waits.append, jitter=random.Random(0), **kwargs)
    return client, waits


def test_recovers_from_injected_429s_and_500s(activity_index, caplog):
    clean, _ = make_client(activity_index, ServerSettings())
    expected = clean.fetch_day(DAY)

    flaky, waits = make_client(activity_index, ServerSettings(
        rate_limit_rate=0.3, error_rate=0.2, retry_after_seconds=2, seed=11))
    with caplog.at_level(logging.WARNING, logger="olist_pipeline.api.client"):
        pages = flaky.fetch_day(DAY)

    assert pages == expected                       # failures never change the data
    assert flaky.stats.records == clean.stats.records > 0
    assert flaky.stats.retries == len(waits) == len(caplog.records) > 0
    assert flaky.stats.wait_seconds == pytest.approx(sum(waits))
    saw = set()
    for rec, wait in zip(caplog.records, waits):
        msg = rec.getMessage()
        assert f"waiting {wait:.2f}s" in msg and "attempt" in msg
        if "HTTP 429" in msg:
            assert wait == 2 and msg.endswith("(Retry-After)")
            saw.add(429)
        else:
            assert "HTTP 500" in msg and msg.endswith("(backoff)")
            attempt = int(msg.split("attempt ")[1].split("/")[0])
            assert 0.5 * 0.5 * 2 ** (attempt - 1) <= wait <= 0.5 * 2 ** (attempt - 1)
            saw.add(500)
    assert saw == {429, 500}


def test_gives_up_after_max_attempts(activity_index):
    client, waits = make_client(activity_index, ServerSettings(rate_limit_rate=1.0), max_attempts=5)
    with pytest.raises(ApiError, match="giving up after 5 attempts .*HTTP 429"):
        client.fetch_day(DAY)
    assert len(waits) == 4          # 5 attempts, 4 waits in between


def test_backoff_doubles_and_is_capped(activity_index):
    client, waits = make_client(activity_index, ServerSettings(error_rate=1.0), max_attempts=6,
                                backoff_base=0.5, backoff_max=3.0)
    with pytest.raises(ApiError):
        client.get_page(DAY, 1)
    caps = [0.5, 1.0, 2.0, 3.0, 3.0]
    assert len(waits) == 5
    for wait, cap in zip(waits, caps):
        assert cap * 0.5 <= wait <= cap


def test_no_retry_on_401(activity_index):
    client, waits = make_client(activity_index, ServerSettings(), key="wrong")
    with pytest.raises(ApiError, match="HTTP 401"):
        client.fetch_day(DAY)
    assert waits == []


def test_transport_errors_are_retried():
    calls = {"n": 0}
    body = {"date": "2018-03-01", "page": 1, "page_size": 100, "total_records": 1, "total_pages": 1,
            "next_page": None, "data": [{"customer_unique_id": "u1"}]}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json=body)

    waits = []
    http = httpx.Client(base_url="http://api", transport=httpx.MockTransport(handler))
    client = ActivityClient(http, KEY, sleep=waits.append, jitter=random.Random(0))
    assert [n for n, _ in client.fetch_day(DAY)] == [1]
    assert calls["n"] == 2 and len(waits) == 1


def test_total_records_mismatch_fails():
    body = {"date": "2018-03-01", "page": 1, "page_size": 100, "total_records": 5, "total_pages": 1,
            "next_page": None, "data": [{"customer_unique_id": "u1"}]}
    http = httpx.Client(base_url="http://api", transport=httpx.MockTransport(
        lambda request: httpx.Response(200, json=body)))
    with pytest.raises(ApiError, match="expected 5"):
        ActivityClient(http, KEY, sleep=lambda s: None).fetch_day(DAY)


def test_parse_retry_after():
    assert parse_retry_after("3") == 3.0
    assert parse_retry_after(None) is None
    assert parse_retry_after("soon") is None
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=30), usegmt=True)
    assert 25 < parse_retry_after(future) <= 30


def test_land_pages_replaces_the_whole_folder(tmp_path):
    land_pages(tmp_path, DAY, [(1, b'{"a":1}'), (2, b'{"a":2}'), (3, b'{"a":3}')])
    out = land_pages(tmp_path, DAY, [(1, b'{"a":9}')])
    assert out == landing_dir_for(tmp_path, DAY)
    assert sorted(p.name for p in out.iterdir()) == ["page_0001.json"]
    assert (out / "page_0001.json").read_bytes() == b'{"a":9}'
    assert sorted(p.name for p in out.parent.iterdir()) == ["dt=2018-03-01"]


def test_failed_fetch_keeps_previous_landing(activity_index, tmp_path):
    land_pages(tmp_path, DAY, [(1, b'{"old": true}')])
    client, _ = make_client(activity_index, ServerSettings(error_rate=1.0), max_attempts=2)
    with pytest.raises(ApiError):
        fetch_and_land(client, {"paths": {"landing_dir": tmp_path}}, DAY)
    assert json.loads((landing_dir_for(tmp_path, DAY) / "page_0001.json").read_text()) == {"old": True}
