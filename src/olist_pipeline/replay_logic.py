"""Pure replay rules: what the source system showed at the end of a given day.

No database access here, so everything is unit-testable.
"""

from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time

from olist_pipeline.sources import Sources

# Order milestones in their nominal sequence. The data does not always respect it
# (e.g. ~1.4k orders reached the carrier before approval), so status is derived
# from the furthest milestone known, not from the sequence.
MILESTONES = ("order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date")
END_OF_DAY = time(23, 59, 59)


def end_of_day(day: date) -> datetime:
    """Logical updated_at for rows written by the replay of `day`."""
    return datetime.combine(day, END_OF_DAY)


def purchase_day(order: dict) -> date:
    return order["order_purchase_timestamp"].date()


def derive_status(visible: dict) -> str:
    """Interim status from the milestones visible so far."""
    if visible["order_delivered_customer_date"] is not None:
        return "delivered"
    if visible["order_delivered_carrier_date"] is not None:
        return "shipped"
    if visible["order_approved_at"] is not None:
        return "approved"
    return "created"


def order_as_of(order: dict, day: date) -> dict:
    """The order as it looked at the end of `day` (day must be on/after purchase).

    Milestones dated after `day` are hidden. While any are hidden the status is
    derived from the visible ones; once nothing is pending the final status shows.
    This applies to every final status, including canceled/unavailable.
    """
    visible = dict(order)
    pending = False
    for col in MILESTONES:
        ts = order[col]
        if ts is not None and ts.date() > day:
            visible[col] = None
            pending = True
    if pending:
        visible["order_status"] = derive_status(visible)
    return visible


def change_days(order: dict) -> set[date]:
    """Days on which the order's visible state changes (purchase day included).

    A milestone dated on or before the purchase day is already visible at purchase.
    """
    bought = purchase_day(order)
    return {bought} | {order[c].date() for c in MILESTONES if order[c] is not None and order[c].date() > bought}


def review_release_day(review: dict, order: dict) -> date:
    """Reviews appear on their creation date, but never before the order exists."""
    return max(review["review_creation_date"].date(), purchase_day(order))


@dataclass
class ReplayIndex:
    purchased_on: dict[date, list[str]] = field(default_factory=dict)
    changed_on: dict[date, list[str]] = field(default_factory=dict)  # milestone days after purchase
    reviews_on: dict[date, list[dict]] = field(default_factory=dict)
    # customer_unique_ids ordered by the day each was first seen (ties by id),
    # so "customers known by day D" is a prefix of this list.
    known_ids: list[str] = field(default_factory=list)
    known_days: list[date] = field(default_factory=list)
    first_day: date | None = None
    last_day: date | None = None

    def known_customers(self, day: date) -> list[str]:
        return self.known_ids[: bisect_right(self.known_days, day)]


def build_index(src: Sources) -> ReplayIndex:
    purchased, changed, reviews = defaultdict(list), defaultdict(list), defaultdict(list)
    first_seen: dict[str, date] = {}
    all_days: set[date] = set()

    for order_id in sorted(src.orders):
        order = src.orders[order_id]
        bought = purchase_day(order)
        purchased[bought].append(order_id)
        days = change_days(order)
        all_days |= days
        for d in days - {bought}:
            changed[d].append(order_id)
        cuid = src.customers[order["customer_id"]]["customer_unique_id"]
        if cuid not in first_seen or bought < first_seen[cuid]:
            first_seen[cuid] = bought

    for review in src.reviews:
        d = review_release_day(review, src.orders[review["order_id"]])
        reviews[d].append(review)
        all_days.add(d)

    ordered = sorted(first_seen.items(), key=lambda kv: (kv[1], kv[0]))
    return ReplayIndex(
        purchased_on=dict(purchased),
        changed_on=dict(changed),
        reviews_on=dict(reviews),
        known_ids=[cuid for cuid, _ in ordered],
        known_days=[d for _, d in ordered],
        first_day=min(all_days, default=None),
        last_day=max(all_days, default=None),
    )
