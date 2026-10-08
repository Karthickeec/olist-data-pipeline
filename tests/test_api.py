from collections import Counter
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient

from olist_pipeline.api.activity import activity_record, active_customers
from olist_pipeline.api.app import ServerSettings, create_app

KEY = "test-key"
DAY = date(2018, 3, 1)
FIELDS = {"customer_unique_id", "activity_date", "sessions", "page_views", "cart_adds",
          "support_tickets", "last_seen_at", "device"}


@pytest.fixture(scope="module")
def client(activity_index):
    return TestClient(create_app(activity_index, KEY, ServerSettings()))


def get(client, key=KEY, **params):
    headers = {} if key is None else {"X-API-Key": key}
    return client.get("/v1/customer-activity", params={"date": DAY.isoformat(), **params}, headers=headers)


def all_records(client, day=DAY, page_size=100):
    first = get(client, date=day.isoformat(), page_size=page_size).json()
    records = list(first["data"])
    for page in range(2, first["total_pages"] + 1):
        records += get(client, date=day.isoformat(), page=page, page_size=page_size).json()["data"]
    return first, records


def test_requires_api_key(client):
    assert get(client, key=None).status_code == 401
    wrong = get(client, key="nope")
    assert wrong.status_code == 401
    assert wrong.headers["WWW-Authenticate"] == "ApiKey"
    assert get(client).status_code == 200


def test_healthz_is_open(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200 and resp.json()["customers_indexed"] == 96096


def test_pagination_covers_every_record_once(client):
    first, records = all_records(client, page_size=100)
    assert first["total_pages"] == -(-first["total_records"] // 100) > 1
    assert first["next_page"] == 2
    assert len(records) == first["total_records"]
    ids = [r["customer_unique_id"] for r in records]
    assert ids == sorted(set(ids))

    last = get(client, page=first["total_pages"], page_size=100).json()
    assert last["next_page"] is None
    beyond = get(client, page=first["total_pages"] + 1, page_size=100).json()
    assert beyond["data"] == [] and beyond["total_records"] == first["total_records"]


@pytest.mark.parametrize("params", [{"page_size": 0}, {"page_size": 501}, {"page": 0}, {"date": "2018-13-01"}])
def test_invalid_parameters_give_422(client, params):
    assert get(client, **params).status_code == 422


def test_same_date_is_deterministic(client, activity_index):
    _, records = all_records(client, page_size=100)
    fresh = TestClient(create_app(activity_index, KEY, ServerSettings()))
    _, again = all_records(fresh, page_size=37)   # different paging, same data
    assert again == records


def test_only_customers_who_had_ordered(client, activity_index):
    for day in (date(2016, 10, 15), date(2017, 6, 15), DAY):
        _, records = all_records(client, day=day)
        assert records
        for r in records:
            last = activity_index.latest_order_on_or_before(r["customer_unique_id"], day)
            assert last is not None and r["activity_date"] == day.isoformat()
    assert get(client, date="2016-09-01").json()["total_records"] == 0


def test_volume_is_a_few_hundred_per_day(activity_index):
    counts = [len(active_customers(activity_index, date(2017, 9, 1) + timedelta(days=7 * i)))
              for i in range(12)]
    assert all(150 <= n <= 600 for n in counts), counts


def test_dirty_records_about_one_percent(activity_index):
    records = [activity_record(d, c, 0.01)
               for d in (date(2018, 1, 1) + timedelta(days=i) for i in range(20))
               for c in active_customers(activity_index, d)]
    kinds = Counter()
    for r in records:
        if set(r) != FIELDS:
            kinds["missing_field"] += 1
        elif r["sessions"] < 0:
            kinds["negative_sessions"] += 1
        elif not r["last_seen_at"].endswith("Z"):
            kinds["timestamp_format"] += 1
    assert set(kinds) == {"missing_field", "negative_sessions", "timestamp_format"}
    assert 0.005 < sum(kinds.values()) / len(records) < 0.02


def test_dirty_rate_only_changes_picked_records(activity_index):
    for cuid in active_customers(activity_index, DAY)[:200]:
        clean, maybe_dirty = activity_record(DAY, cuid, 0.0), activity_record(DAY, cuid, 0.5)
        assert set(clean) == FIELDS
        if set(maybe_dirty) == FIELDS and maybe_dirty["sessions"] > 0 and maybe_dirty["last_seen_at"].endswith("Z"):
            assert maybe_dirty == clean


def test_injected_429_has_retry_after(activity_index):
    app = TestClient(create_app(activity_index, KEY, ServerSettings(rate_limit_rate=1.0, retry_after_seconds=3)))
    resp = get(app)
    assert resp.status_code == 429 and resp.headers["Retry-After"] == "3"
    assert get(app, key=None).status_code == 401   # auth is checked before failures


def test_injected_500(activity_index):
    app = TestClient(create_app(activity_index, KEY, ServerSettings(error_rate=1.0)))
    assert get(app).status_code == 500


def test_injection_rates_roughly_hold(activity_index):
    app = TestClient(create_app(activity_index, KEY, ServerSettings(rate_limit_rate=0.03, error_rate=0.02, seed=1)))
    codes = Counter(get(app, page_size=1).status_code for _ in range(1000))
    assert 15 <= codes[429] <= 50 and 8 <= codes[500] <= 35 and codes[200] > 900


def test_refuses_empty_key(activity_index):
    with pytest.raises(ValueError):
        create_app(activity_index, "", ServerSettings())
