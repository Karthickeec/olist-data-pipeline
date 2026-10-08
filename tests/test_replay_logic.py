from datetime import date, datetime

import pytest

from olist_pipeline.replay_logic import (
    build_index, change_days, end_of_day, order_as_of, review_release_day,
)
from olist_pipeline.sources import Sources


def order(status="delivered", purchase="2017-03-01 10:00:00", approved=None, carrier=None,
          delivered=None, order_id="o1", customer_id="c1"):
    ts = lambda s: datetime.fromisoformat(s) if s else None
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": status,
        "order_purchase_timestamp": ts(purchase),
        "order_approved_at": ts(approved),
        "order_delivered_carrier_date": ts(carrier),
        "order_delivered_customer_date": ts(delivered),
        "order_estimated_delivery_date": ts("2017-03-20 00:00:00"),
    }


D = date.fromisoformat


def test_delivered_order_walks_through_statuses():
    o = order(approved="2017-03-01 10:15:00", carrier="2017-03-03 09:00:00",
              delivered="2017-03-08 18:00:00")
    seen = {d: order_as_of(o, D(d)) for d in ("2017-03-01", "2017-03-02", "2017-03-03", "2017-03-08")}
    assert seen["2017-03-01"]["order_status"] == "approved"
    assert seen["2017-03-01"]["order_delivered_carrier_date"] is None
    assert seen["2017-03-02"]["order_status"] == "approved"
    assert seen["2017-03-03"]["order_status"] == "shipped"
    assert seen["2017-03-03"]["order_delivered_customer_date"] is None
    assert seen["2017-03-08"] == o


def test_purchase_day_without_approval_is_created():
    o = order(approved="2017-03-02 08:00:00", carrier="2017-03-04 08:00:00",
              delivered="2017-03-09 08:00:00")
    assert order_as_of(o, D("2017-03-01"))["order_status"] == "created"


def test_estimated_delivery_is_known_from_purchase():
    o = order(approved="2017-03-02 08:00:00")
    assert order_as_of(o, D("2017-03-01"))["order_estimated_delivery_date"] == o["order_estimated_delivery_date"]


def test_carrier_before_approval_shows_shipped_without_approval():
    o = order(approved="2017-03-05 08:00:00", carrier="2017-03-03 08:00:00",
              delivered="2017-03-10 08:00:00")
    visible = order_as_of(o, D("2017-03-03"))
    assert visible["order_status"] == "shipped"
    assert visible["order_approved_at"] is None


def test_milestone_before_purchase_is_visible_at_purchase():
    o = order(status="shipped", approved="2017-03-01 10:05:00", carrier="2017-02-27 08:00:00")
    assert order_as_of(o, D("2017-03-01")) == o
    assert change_days(o) == {D("2017-03-01")}


def test_delivered_without_delivery_date_ends_delivered():
    o = order(approved="2017-03-01 11:00:00", carrier="2017-03-04 08:00:00")
    assert order_as_of(o, D("2017-03-03"))["order_status"] == "approved"
    assert order_as_of(o, D("2017-03-04"))["order_status"] == "delivered"


@pytest.mark.parametrize("final", ["canceled", "unavailable", "invoiced", "processing"])
def test_other_final_statuses_show_interim_status_until_nothing_pending(final):
    o = order(status=final, approved="2017-03-02 08:00:00")
    assert order_as_of(o, D("2017-03-01"))["order_status"] == "created"
    assert order_as_of(o, D("2017-03-02"))["order_status"] == final


def test_order_without_milestones_shows_final_status_immediately():
    o = order(status="canceled")
    assert order_as_of(o, D("2017-03-01")) == o


def test_change_days():
    o = order(approved="2017-03-01 23:00:00", carrier="2017-03-03 08:00:00",
              delivered="2017-03-03 18:00:00")
    assert change_days(o) == {D("2017-03-01"), D("2017-03-03")}


def test_review_release_day_never_before_purchase():
    o = order()
    early = {"review_creation_date": datetime(2017, 2, 25)}
    late = {"review_creation_date": datetime(2017, 3, 12)}
    assert review_release_day(early, o) == D("2017-03-01")
    assert review_release_day(late, o) == D("2017-03-12")


def test_end_of_day():
    assert end_of_day(D("2017-03-01")) == datetime(2017, 3, 1, 23, 59, 59)


def test_build_index():
    o1 = order(order_id="o1", customer_id="c1", approved="2017-03-02 08:00:00")
    o2 = order(order_id="o2", customer_id="c2", purchase="2017-03-02 09:00:00")
    o3 = order(order_id="o3", customer_id="c3", purchase="2017-03-04 09:00:00")
    src = Sources(
        customers={
            "c1": {"customer_unique_id": "p1"},
            "c2": {"customer_unique_id": "p2"},
            "c3": {"customer_unique_id": "p1"},  # same person, second order
        },
        orders={"o1": o1, "o2": o2, "o3": o3},
        items={}, payments={},
        reviews=[{"order_id": "o1", "review_creation_date": datetime(2017, 3, 6)}],
    )
    idx = build_index(src)
    assert idx.purchased_on == {D("2017-03-01"): ["o1"], D("2017-03-02"): ["o2"], D("2017-03-04"): ["o3"]}
    assert idx.changed_on == {D("2017-03-02"): ["o1"]}
    assert list(idx.reviews_on) == [D("2017-03-06")]
    assert (idx.first_day, idx.last_day) == (D("2017-03-01"), D("2017-03-06"))
    assert idx.known_customers(D("2017-02-28")) == []
    assert idx.known_customers(D("2017-03-01")) == ["p1"]
    assert idx.known_customers(D("2017-03-10")) == ["p1", "p2"]
