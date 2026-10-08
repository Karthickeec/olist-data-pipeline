from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from olist_pipeline.silver.clean import (
    build_geolocation,
    build_products,
    clean_customer_activity,
    clean_customer_changes,
    normalize_state,
    parse_activity_timestamp,
)
from olist_pipeline.silver.common import latest_per_key, merge_into, split_quarantine
from olist_pipeline.silver.order_lines import allocate_payment, build_order_lines

D1, D2 = date(2017, 3, 1), date(2017, 3, 2)


def test_normalize_state(spark):
    rows = [
        (" sp ",),
        ("Sp",),
        ("sp ",),
        ("São Paulo",),
        ("rio de janeiro",),
        ("MINAS GERAIS",),
        ("XX",),
        (None,),
        ("Paraná",),
    ]
    out = spark.createDataFrame(rows, "s string").select(normalize_state(F.col("s")).alias("n")).collect()
    assert [r.n for r in out] == ["SP", "SP", "SP", "SP", "RJ", "MG", None, None, "PR"]


def test_parse_both_timestamp_formats(spark):
    rows = [("2017-03-01T14:22:05Z",), ("01/03/2017 14:22:05",), ("yesterday",), (None,)]
    out = (
        spark.createDataFrame(rows, "s string")
        .select(F.date_format(parse_activity_timestamp(F.col("s")), "yyyy-MM-dd HH:mm:ss").alias("t"))
        .collect()
    )
    assert [r.t for r in out] == ["2017-03-01 14:22:05", "2017-03-01 14:22:05", None, None]


def test_latest_per_key_tie_breaks(spark):
    df = spark.createDataFrame(
        [
            ("o1", datetime(2017, 3, 1), D1, "old"),
            ("o1", datetime(2017, 3, 2), D1, "newer source version"),
            ("o2", datetime(2017, 3, 1), D1, "first ingest"),
            ("o2", datetime(2017, 3, 1), D2, "same version, newer partition"),
        ],
        "id string, updated_at timestamp, ingest date, note string",
    )
    out = latest_per_key(df, ["id"], [F.col("updated_at").desc(), F.col("ingest").desc()])
    assert {r.id: r.note for r in out.collect()} == {
        "o1": "newer source version",
        "o2": "same version, newer partition",
    }


def test_split_quarantine_uses_first_failing_rule(spark):
    df = spark.createDataFrame([(1, -1, None), (2, 5, None), (3, -1, "x")], "id int, n int, flag string")
    valid, bad = split_quarantine(df, [("negative", F.col("n") < 0), ("flagged", F.col("flag").isNotNull())])
    assert [r.id for r in valid.collect()] == [2]
    assert {r.id: r._reason for r in bad.collect()} == {1: "negative", 3: "negative"}


CHANGE_COLS = (
    "change_id string, customer_unique_id string, new_zip_code_prefix string, new_city string, "
    "new_state string, requested_at string, source string, _corrupt_record string, "
    "_source_file string, _bronze_ingest_date date"
)


def change(cid, cuid="u1", zip_="01037", city="sao paulo", state="SP", at="2017-03-01 10:00:00", corrupt=None):
    return (cid, cuid, zip_, city, state, at, "crm_portal", corrupt, "f.jsonl", D1)


def test_clean_customer_changes(spark):
    bronze = spark.createDataFrame(
        [
            change("c1"),
            change("c1"),  # exact duplicate
            change("c2", state=" sp "),  # messy state, fixed
            change("c3", state="São Paulo"),  # state name, fixed
            change("c4", city=None),  # null city, filled from geolocation
            change("c5", city=None, zip_="99999"),  # null city, zip unknown
            change("c6", cuid="ghost"),  # unknown customer
            change("c7", state="XX"),  # not a state
            change("c8", at="not a time"),
            change(None, cuid=None, zip_=None, city=None, state=None, at=None, corrupt='{"change_id": '),
        ],
        CHANGE_COLS,
    )
    known = spark.createDataFrame([("u1",)], "customer_unique_id string")
    zip_city = spark.createDataFrame([("01037", "sao paulo")], "zip_code_prefix string, city string")
    valid, quarantined, duplicates = clean_customer_changes(bronze, known, zip_city)

    assert duplicates == 1
    v = {r.change_id: r for r in valid.collect()}
    assert sorted(v) == ["c1", "c2", "c3", "c4"]
    assert {v[c].new_state for c in v} == {"SP"}
    assert v["c2"]._state_fixed and v["c3"]._state_fixed and not v["c1"]._state_fixed
    assert v["c4"].new_city == "sao paulo" and v["c4"]._city_filled
    assert v["c1"].requested_date == D1
    assert {r.change_id: r._reason for r in quarantined.collect()} == {
        "c5": "unfixable_city",
        "c6": "unknown_customer",
        "c7": "invalid_state",
        "c8": "unparseable_timestamp",
        None: "corrupt_record",
    }
    assert set(quarantined.columns) == set(bronze.columns) | {"_reason"}  # originals kept


ACTIVITY_COLS = (
    "customer_unique_id string, activity_date string, sessions string, page_views string, "
    "cart_adds string, support_tickets string, last_seen_at string, device string, _page long, "
    "_source_file string, _corrupt_record string, _bronze_ingest_date date"
)


def activity(
    cuid, sessions="2", page_views="10", last_seen="2017-03-01T10:00:00Z", device="web", day="2017-03-01", corrupt=None
):
    return (cuid, day, sessions, page_views, "1", "0", last_seen, device, 1, "p.json", corrupt, D1)


def test_clean_customer_activity(spark):
    bronze = spark.createDataFrame(
        [
            activity("u1"),
            activity("u2", last_seen="01/03/2017 10:00:00"),  # Brazilian timestamp, parsed
            activity("u3", page_views=None, device=None),  # optional fields missing, kept
            activity("u4", sessions="-2"),
            activity("u5", sessions=None),
            activity("u6", sessions="two"),
            activity("u7", last_seen="03-01-2017"),
            activity("ghost"),
            activity(None, sessions=None, day=None, corrupt="{bad"),
        ],
        ACTIVITY_COLS,
    )
    known = spark.createDataFrame([(f"u{i}",) for i in range(1, 8)], "customer_unique_id string")
    valid, quarantined, duplicates = clean_customer_activity(bronze, known)

    assert duplicates == 0
    v = {r.customer_unique_id: r for r in valid.collect()}
    assert sorted(v) == ["u1", "u2", "u3"]
    assert v["u1"].sessions == 2 and v["u1"].activity_date == D1
    assert v["u2"].last_seen_at is not None and v["u2"]._timestamp_reformatted
    assert v["u3"].page_views is None and v["u3"].device is None
    assert {r.customer_unique_id: r._reason for r in quarantined.collect()} == {
        "u4": "negative_sessions",
        "u5": "missing_required_field",
        "u6": "invalid_number",
        "u7": "unparseable_timestamp",
        "ghost": "unknown_customer",
        None: "corrupt_record",
    }


def test_build_geolocation_median_and_mode(spark):
    snap = spark.createDataFrame(
        [
            ("01037", -23.0, -46.0, "sao paulo", "SP"),
            ("01037", -23.2, -46.2, "sao paulo", "SP"),
            ("01037", -10.0, -10.0, "são paulo", "SP"),  # outlier + spelling variant
            ("01037", -23.2, -46.2, "sao paulo", "SP"),  # exact duplicate row
            ("20010", -22.9, -43.1, "rio de janeiro", "RJ"),
        ],
        "geolocation_zip_code_prefix string, geolocation_lat double, geolocation_lng double, "
        "geolocation_city string, geolocation_state string",
    )
    out = {r.zip_code_prefix: r for r in build_geolocation(snap).collect()}
    assert len(out) == 2
    assert out["01037"].lat == pytest.approx(-23.1) and out["01037"].city == "sao paulo"
    assert out["01037"].source_rows == 4


def test_build_products_translates_every_category(spark):
    products = spark.createDataFrame(
        [
            ("p1", "beleza_saude"),
            ("p2", "pc_gamer"),
            ("p3", "portateis_cozinha_e_preparadores_de_alimentos"),
            ("p4", None),
        ],
        "product_id string, product_category_name string",
    ).select(
        "*",
        *[
            F.lit(1).alias(c)
            for c in (
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            )
        ],
    )
    translation = spark.createDataFrame(
        [("beleza_saude", "health_beauty")], "product_category_name string, product_category_name_english string"
    )
    out = {r.product_id: r for r in build_products(products, translation).collect()}
    assert {k: r.product_category_name_english for k, r in out.items()} == {
        "p1": "health_beauty",
        "p2": "pc_gamer",
        "p3": "kitchen_portables_and_food_preparers",
        "p4": "unknown",
    }
    assert "product_name_length" in build_products(products, translation).columns


def test_allocate_payment_sums_exactly(spark):
    lines = spark.createDataFrame(
        [
            ("o1", 1, Decimal("10.00"), Decimal("100.00")),
            ("o1", 2, Decimal("10.00"), Decimal("100.00")),
            ("o1", 3, Decimal("10.00"), Decimal("100.00")),  # 100/3 does not split evenly
            ("o2", 1, Decimal("0.00"), Decimal("50.00")),
            ("o2", 2, Decimal("0.00"), Decimal("50.00")),  # zero weights: equal split
        ],
        "order_id string, order_item_id int, line_total decimal(12,2), payment_total decimal(20,2)",
    )
    out = (
        allocate_payment(lines)
        .groupBy("order_id")
        .agg(F.sum("allocated_payment").alias("s"), F.collect_list("allocated_payment").alias("parts"))
    )
    got = {r.order_id: r for r in out.collect()}
    assert got["o1"].s == Decimal("100.00") and sorted(got["o1"].parts) == [
        Decimal("33.33"),
        Decimal("33.33"),
        Decimal("33.34"),
    ]
    assert got["o2"].s == Decimal("50.00")


def test_order_lines_broadcasts_small_tables(spark):
    orders = spark.createDataFrame(
        [("o1", "c1", "delivered", datetime(2017, 3, 1, 10), D1)],
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp, order_purchase_date date",
    )
    for c in (
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        orders = orders.withColumn(c, F.lit(None).cast("timestamp"))
    items = spark.createDataFrame(
        [("o1", 1, "p1", "s1", datetime(2017, 3, 2), Decimal("10.00"), Decimal("2.00"))],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "shipping_limit_date timestamp, price decimal(10,2), freight_value decimal(10,2)",
    )
    payments = spark.createDataFrame(
        [("o1", 1, "credit_card", 3, Decimal("12.00"))],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value decimal(10,2)",
    )
    customers = spark.createDataFrame(
        [("c1", "u1", "01037", "sao paulo", "SP")],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix "
        "string, customer_city string, customer_state string",
    )
    products = spark.createDataFrame(
        [("p1", "beleza_saude", "health_beauty")],
        "product_id string, product_category_name string, product_category_name_english string",
    )
    sellers = spark.createDataFrame(
        [("s1", "13023", "campinas", "SP")],
        "seller_id string, seller_zip_code_prefix string, seller_city string, seller_state string",
    )
    lines = build_order_lines(orders, items, payments, customers, products, sellers)
    [row] = lines.collect()
    assert row.allocated_payment == Decimal("12.00") and row.payment_type_main == "credit_card"
    assert row.customer_unique_id == "u1" and row.product_category_name_english == "health_beauty"
    plan = lines._jdf.queryExecution().executedPlan().toString()
    assert plan.count("BroadcastHashJoin") >= 3


def test_merge_into_is_idempotent_and_partition_scoped(spark, tmp_path):
    root = str(tmp_path)
    order = [F.col("v").desc()]
    schema = "id string, v int, d date"
    merge_into(spark, spark.createDataFrame([("a", 1, D1), ("b", 1, D2)], schema), root, "t", ["id"], order, "d")
    merge_into(spark, spark.createDataFrame([("a", 2, D1), ("c", 1, D1)], schema), root, "t", ["id"], order, "d")
    first = sorted(spark.read.parquet(f"{root}/silver/t").collect())
    # Same batch again: no change. Older version of "a": ignored.
    merge_into(spark, spark.createDataFrame([("a", 2, D1), ("c", 1, D1)], schema), root, "t", ["id"], order, "d")
    merge_into(spark, spark.createDataFrame([("a", 1, D1)], schema), root, "t", ["id"], order, "d")
    after = sorted(spark.read.parquet(f"{root}/silver/t").collect())
    assert after == first
    assert {(r.id, r.v) for r in after} == {("a", 2), ("b", 1), ("c", 1)}
    assert not (tmp_path / "silver" / "_staging" / "t").exists()
