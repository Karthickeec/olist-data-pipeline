"""Deterministic daily customer activity, derived from who had ordered by that day.

Everything is seeded by (day, customer_unique_id) through sha256, so the same
date always yields the same records regardless of process, order or paging.
"""
import hashlib
import random
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

from olist_pipeline.sources import read_table

# Chance that a customer is active on a day, by days since their latest order.
RECENT_ACTIVITY = ((7, 0.15), (30, 0.02))   # (age below N days, probability)
BASELINE_ACTIVITY = 0.001
DEVICES = ("web", "android", "ios")
DEVICE_WEIGHTS = (50, 35, 15)
DIRTY_KINDS = ("missing_field", "negative_sessions", "timestamp_format")
DROPPABLE_FIELDS = ("page_views", "device", "last_seen_at")


@dataclass
class CustomerIndex:
    """customer_unique_id -> sorted purchase days. Small enough to keep in memory (~96k ids)."""
    ids: list[str]
    order_days: dict[str, list[date]]

    def latest_order_on_or_before(self, cuid: str, day: date) -> date | None:
        days = self.order_days[cuid]
        i = bisect_right(days, day)
        return days[i - 1] if i else None


def build_customer_index(raw_dir: Path) -> CustomerIndex:
    person = {c["customer_id"]: c["customer_unique_id"] for c in read_table(raw_dir, "customers")}
    order_days: dict[str, list[date]] = {}
    for o in read_table(raw_dir, "orders"):
        order_days.setdefault(person[o["customer_id"]], []).append(o["order_purchase_timestamp"].date())
    for days in order_days.values():
        days.sort()
    return CustomerIndex(ids=sorted(order_days), order_days=order_days)


def _seed(*parts) -> int:
    return int.from_bytes(hashlib.sha256("|".join(map(str, parts)).encode()).digest()[:8], "big")


def _unit(*parts) -> float:
    """Uniform [0, 1) from a hash: a stable coin flip per (purpose, day, customer)."""
    return _seed(*parts) / 2**64


def activity_probability(days_since_order: int) -> float:
    for limit, p in RECENT_ACTIVITY:
        if days_since_order < limit:
            return p
    return BASELINE_ACTIVITY


def active_customers(index: CustomerIndex, day: date) -> list[str]:
    """Customers with activity on `day`, sorted by id. Only people who had ordered by then."""
    active = []
    for cuid in index.ids:
        last = index.latest_order_on_or_before(cuid, day)
        if last is not None and _unit("active", day, cuid) < activity_probability((day - last).days):
            active.append(cuid)
    return active


def activity_record(day: date, cuid: str, dirty_rate: float) -> dict:
    """One customer's activity for `day`. About `dirty_rate` of records are malformed on purpose."""
    rng = random.Random(_seed("activity", day, cuid))
    sessions = 1 + min(7, int(rng.expovariate(0.6)))
    page_views = sum(rng.randint(2, 15) for _ in range(sessions))
    last_seen = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randrange(86400))
    record = {
        "customer_unique_id": cuid,
        "activity_date": day.isoformat(),
        "sessions": sessions,
        "page_views": page_views,
        "cart_adds": rng.randint(0, page_views // 5),
        "support_tickets": rng.choices((0, 1, 2), (92, 7, 1))[0],
        "last_seen_at": last_seen.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "device": rng.choices(DEVICES, DEVICE_WEIGHTS)[0],
    }
    # Always draw, so a record's content does not depend on dirty_rate unless it is picked.
    dirty = rng.choice(DIRTY_KINDS) if rng.random() < dirty_rate else None
    if dirty == "missing_field":
        del record[rng.choice(DROPPABLE_FIELDS)]
    elif dirty == "negative_sessions":
        record["sessions"] = -rng.randint(1, 3)
    elif dirty == "timestamp_format":
        record["last_seen_at"] = last_seen.strftime("%d/%m/%Y %H:%M:%S")
    return record
