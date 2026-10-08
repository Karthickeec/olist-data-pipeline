"""Synthetic customer address-change requests (a second, file-based source system).

Requests for day D land in <landing>/customer_changes/dt=D/changes.jsonl. The RNG
is seeded from the date alone, so regenerating a day produces identical bytes.
About `dirty_rate` of the requests are deliberately malformed so later layers have
something to clean: exact duplicates, messy state codes, null city, or a
customer_unique_id that does not exist.
"""
import hashlib
import json
import os
import random
import uuid
from collections.abc import Sequence
from datetime import date, datetime, timedelta
from pathlib import Path

SOURCE_NAME = "crm_portal"
CHANGE_ID_NAMESPACE = uuid.UUID("6f1c1d2e-3b4a-4c5d-8e9f-0a1b2c3d4e5f")
DIRTY_KINDS = ("duplicate", "messy_state", "null_city", "unknown_customer")
STATE_NAMES = {
    "SP": "São Paulo", "RJ": "Rio de Janeiro", "MG": "Minas Gerais",
    "RS": "Rio Grande do Sul", "PR": "Paraná", "BA": "Bahia",
}


def rng_for(day: date) -> random.Random:
    seed = hashlib.sha256(f"customer_changes:{day.isoformat()}".encode()).digest()
    return random.Random(int.from_bytes(seed[:8], "big"))


def generate_changes(
    day: date,
    n_orders: int,
    known_customers: Sequence[str],
    address_pool: Sequence[tuple[str, str, str]],
    rate: float,
    dirty_rate: float,
) -> list[dict]:
    """Address-change requests for `day`; each of the day's orders has a `rate` chance of one."""
    rng = rng_for(day)
    n_changes = sum(rng.random() < rate for _ in range(n_orders))
    if not known_customers or not address_pool:
        return []

    records = []
    midnight = datetime.combine(day, datetime.min.time())
    for i in range(n_changes):
        zip_prefix, city, state = rng.choice(address_pool)
        record = {
            "change_id": str(uuid.uuid5(CHANGE_ID_NAMESPACE, f"{day.isoformat()}:{i}")),
            "customer_unique_id": rng.choice(known_customers),
            "new_zip_code_prefix": zip_prefix,
            "new_city": city,
            "new_state": state,
            "requested_at": (midnight + timedelta(seconds=rng.randrange(86400))).isoformat(sep=" "),
            "source": SOURCE_NAME,
        }
        dirty = rng.choice(DIRTY_KINDS) if rng.random() < dirty_rate else None
        if dirty == "messy_state":
            record["new_state"] = rng.choice([
                state.lower(), f" {state} ", f"{state.lower()} ", STATE_NAMES.get(state, state.title()),
            ])
        elif dirty == "null_city":
            record["new_city"] = None
        elif dirty == "unknown_customer":
            record["customer_unique_id"] = f"{rng.getrandbits(128):032x}"
        records.append(record)
        if dirty == "duplicate":
            records.append(dict(record))
    return records


def partition_dir(landing_dir: Path, day: date) -> Path:
    return Path(landing_dir) / "customer_changes" / f"dt={day.isoformat()}"


def write_changes(landing_dir: Path, day: date, records: list[dict]) -> Path:
    """Write the day's file atomically (temp file + rename). Empty days get an empty file."""
    out_dir = partition_dir(landing_dir, day)
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "changes.jsonl"
    tmp = out_dir / ".changes.jsonl.tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(tmp, target)
    return target
