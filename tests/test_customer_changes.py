import json
from collections import Counter
from datetime import date, timedelta

from olist_pipeline.customer_changes import generate_changes, write_changes

KNOWN = [f"{i:032x}" for i in range(500)]
POOL = [("01037", "sao paulo", "SP"), ("20010", "rio de janeiro", "RJ"), ("30110", "belo horizonte", "MG")]
STATES = {s for _, _, s in POOL}


def gen(day, n_orders=200, rate=0.03, dirty_rate=0.05):
    return generate_changes(day, n_orders, KNOWN, POOL, rate=rate, dirty_rate=dirty_rate)


def test_same_day_is_deterministic():
    assert gen(date(2017, 5, 1)) == gen(date(2017, 5, 1))


def test_different_days_differ():
    assert gen(date(2017, 5, 1)) != gen(date(2017, 5, 2))


def test_no_orders_no_changes():
    assert gen(date(2017, 5, 1), n_orders=0) == []


def test_rate_and_dirty_mix_over_many_days():
    days = [date(2017, 1, 1) + timedelta(days=i) for i in range(400)]
    records = [r for d in days for r in gen(d, n_orders=100)]
    unique_ids = {r["change_id"] for r in records}
    assert 0.025 < len(unique_ids) / (400 * 100) < 0.035

    kinds = Counter()
    seen = set()
    for r in records:
        if r["change_id"] in seen:
            kinds["duplicate"] += 1
        seen.add(r["change_id"])
        if r["new_state"] not in STATES:
            kinds["messy_state"] += 1
        if r["new_city"] is None:
            kinds["null_city"] += 1
        if r["customer_unique_id"] not in KNOWN:
            kinds["unknown_customer"] += 1
    assert set(kinds) == {"duplicate", "messy_state", "null_city", "unknown_customer"}
    assert 0.03 < sum(kinds.values()) / len(unique_ids) < 0.07


def test_records_are_well_formed():
    day = date(2017, 5, 1)
    for r in gen(day, n_orders=2000):
        assert set(r) == {"change_id", "customer_unique_id", "new_zip_code_prefix", "new_city",
                          "new_state", "requested_at", "source"}
        assert r["requested_at"].startswith("2017-05-01 ")


def test_write_is_byte_identical_on_rerun(tmp_path):
    day = date(2017, 5, 1)
    path = write_changes(tmp_path, day, gen(day))
    first = path.read_bytes()
    write_changes(tmp_path, day, gen(day))
    assert path.read_bytes() == first
    assert path == tmp_path / "customer_changes" / "dt=2017-05-01" / "changes.jsonl"
    assert [p.name for p in path.parent.iterdir()] == ["changes.jsonl"]
    assert [json.loads(line) for line in first.decode().splitlines()] == gen(day)


def test_empty_day_writes_empty_file(tmp_path):
    path = write_changes(tmp_path, date(2017, 5, 1), [])
    assert path.read_bytes() == b""
