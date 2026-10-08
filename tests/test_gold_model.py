from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import functions as F

from olist_pipeline.gold.model import (
    build_customer_metrics,
    build_daily_category_sales,
    build_dim_customer,
    build_dim_date,
    build_fact_order_lines,
    frequency_band,
    rfm_segment,
)

T = datetime


def orders_customers(spark, rows):
    """rows: (order_id, customer_id, cuid, purchase_ts, zip, city, state)"""
    orders = spark.createDataFrame(
        [(o, c, ts) for o, c, _, ts, *_ in rows],
        "order_id string, customer_id string, order_purchase_timestamp timestamp",
    )
    customers = spark.createDataFrame(
        [(c, u, z, ci, s) for _, c, u, _, z, ci, s in rows],
        "customer_id string, customer_unique_id string, customer_zip_code_prefix string, "
        "customer_city string, customer_state string",
    )
    return orders, customers


def changes_df(spark, rows):
    """rows: (change_id, cuid, requested_at, zip, city, state)"""
    return spark.createDataFrame(
        rows,
        "change_id string, customer_unique_id string, requested_at timestamp, "
        "new_zip_code_prefix string, new_city string, new_state string",
    )


def versions(dim, cuid):
    return [
        (r.state, r.valid_from, r.valid_to, r.is_current, r.version)
        for r in dim.filter(F.col("customer_unique_id") == cuid).orderBy("valid_from").collect()
    ]


OPEN = T(9999, 12, 31)


def test_scd2_versions(spark):
    orders, customers = orders_customers(
        spark,
        [
            ("o1", "c1", "u1", T(2017, 3, 1, 10), "01037", "sao paulo", "SP"),
            ("o2", "c2", "u1", T(2017, 5, 1, 10), "20010", "rio", "RJ"),  # later order: not version 1
            ("o3", "c3", "u2", T(2017, 3, 2, 9), "30110", "bh", "MG"),
        ],
    )
    changes = changes_df(
        spark,
        [
            ("x1", "u1", T(2017, 4, 1), "20010", "rio de janeiro", "RJ"),
            ("x2", "u1", T(2017, 4, 5), "20010", "rio de janeiro", "RJ"),  # same address: skipped
            ("x3", "u1", T(2017, 6, 1), "80010", "curitiba", "PR"),
            ("x4", "u2", T(2017, 3, 2, 8), "40010", "salvador", "BA"),  # before first order: clamped
        ],
    )
    dim = build_dim_customer(orders, customers, changes)
    assert versions(dim, "u1") == [
        ("SP", T(2017, 3, 1, 10), T(2017, 4, 1), False, 1),
        ("RJ", T(2017, 4, 1), T(2017, 6, 1), False, 2),
        ("PR", T(2017, 6, 1), OPEN, True, 3),
    ]
    # u2's change came before (same day as) the first order: it supersedes the initial address at that instant.
    assert versions(dim, "u2") == [("BA", T(2017, 3, 2, 9), OPEN, True, 1)]
    assert dim.select("customer_sk").distinct().count() == dim.count()
    # Rebuilding gives the same surrogate keys.
    again = build_dim_customer(orders, customers, changes)
    assert sorted(r.customer_sk for r in dim.collect()) == sorted(r.customer_sk for r in again.collect())


def test_fact_uses_version_valid_at_purchase(spark):
    orders, customers = orders_customers(spark, [("o1", "c1", "u1", T(2017, 3, 1, 10), "01037", "sp", "SP")])
    dim = build_dim_customer(orders, customers, changes_df(spark, [("x1", "u1", T(2017, 4, 1), "20010", "rio", "RJ")]))
    lines = spark.createDataFrame(
        [
            ("o1", 1, "u1", T(2017, 3, 1, 10), date(2017, 3, 1), "SP"),
            ("o9", 1, "u1", T(2017, 4, 1), date(2017, 4, 1), "SP"),  # exactly at valid_from of v2
        ],
        "order_id string, order_item_id int, customer_unique_id string, order_purchase_timestamp timestamp, "
        "order_purchase_date date, customer_state string",
    )
    for c, t in (
        ("product_id", "string"),
        ("seller_id", "string"),
        ("order_status", "string"),
        ("payment_type_main", "string"),
        ("payment_installments_max", "int"),
        ("price", "decimal(10,2)"),
        ("freight_value", "decimal(10,2)"),
        ("line_total", "decimal(12,2)"),
        ("allocated_payment", "decimal(12,2)"),
    ):
        lines = lines.withColumn(c, F.lit(None).cast(t))
    fact = {r.order_id: r for r in build_fact_order_lines(lines, dim).collect()}
    assert fact["o1"].customer_state == "SP" and fact["o9"].customer_state == "RJ"
    assert fact["o1"].date_key == 20170301
    assert fact["o1"].customer_sk != fact["o9"].customer_sk


def fact_rows(spark, rows):
    """rows: (order_id, item, cuid, purchase_date, status, product_sk, price, allocated)"""
    return spark.createDataFrame(
        [
            (o, i, u, d, s, p, Decimal(str(pr)), Decimal(str(fr)), Decimal(str(pr + fr)), Decimal(str(a)))
            for o, i, u, d, s, p, pr, fr, a in rows
        ],
        "order_id string, order_item_id int, customer_unique_id string, order_purchase_date date, "
        "order_status string, product_sk bigint, price decimal(10,2), freight_value decimal(10,2), "
        "line_total decimal(12,2), allocated_payment decimal(12,2)",
    )


def test_daily_category_sales_excludes_canceled(spark):
    D = date(2017, 3, 1)
    fact = fact_rows(
        spark,
        [
            ("o1", 1, "u1", D, "delivered", 1, 10.0, 2.0, 12.0),
            ("o1", 2, "u1", D, "delivered", 1, 20.0, 3.0, 23.0),
            ("o2", 1, "u2", D, "canceled", 1, 99.0, 1.0, 100.0),
        ],
    )
    products = spark.createDataFrame([(1, "toys")], "product_sk bigint, product_category_name_english string")
    [row] = build_daily_category_sales(fact, products).collect()
    assert (row.category, row.orders, row.items, row.revenue, row.freight) == (
        "toys",
        1,
        2,
        Decimal("30.00"),
        Decimal("5.00"),
    )
    assert row.avg_item_price == Decimal("15.00")


def test_customer_metrics_and_rfm(spark):
    as_of = date(2017, 12, 31)
    rows = []
    for i in range(10):  # u0..u9: one order each, rising value and recency
        rows.append((f"o{i}", 1, f"u{i}", date(2017, 1 + i, 1), "delivered", 1, 10.0 * (i + 1), 0.0, 10.0 * (i + 1)))
    rows += [
        ("p1", 1, "u9", date(2017, 12, 1), "delivered", 1, 50.0, 0.0, 50.0),
        ("p2", 1, "u9", date(2017, 12, 20), "delivered", 1, 50.0, 0.0, 50.0),
        ("x1", 1, "u9", date(2017, 12, 21), "canceled", 1, 999.0, 0.0, 999.0),  # not counted
        ("f1", 1, "u9", date(2018, 1, 5), "delivered", 1, 999.0, 0.0, 999.0),
    ]  # after as_of
    m = {r.customer_unique_id: r for r in build_customer_metrics(fact_rows(spark, rows), as_of).collect()}
    assert len(m) == 10
    u9 = m["u9"]
    assert (u9.order_count, u9.lifetime_value, u9.days_since_last_order) == (3, Decimal("200.00"), 11)
    assert u9.avg_order_value == Decimal("66.67") and u9.f_score == 3
    assert (u9.r_score, u9.m_score, u9.rfm_segment) == (5, 5, "Champions")
    assert m["u0"].r_score == 1 and m["u0"].rfm_segment == "Lost"
    assert m["u8"].rfm_segment == "Potential"  # recent, one order


def test_frequency_bands_and_segments(spark):
    df = spark.createDataFrame([(n,) for n in (1, 2, 3, 4, 5, 6, 20)], "n int")
    assert [r[0] for r in df.select(frequency_band(F.col("n"))).collect()] == [1, 2, 3, 4, 4, 5, 5]
    cases = spark.createDataFrame(
        [(5, 2, 5), (3, 3, 1), (5, 1, 1), (3, 1, 5), (2, 1, 1), (1, 5, 5)], "r int, f int, m int"
    )
    got = [r[0] for r in cases.select(rfm_segment(F.col("r"), F.col("f"), F.col("m"))).collect()]
    assert got == ["Champions", "Loyal", "Potential", "At risk", "Hibernating", "Lost"]


def test_dim_date(spark):
    dim = build_dim_date(spark, date(2017, 1, 1), date(2017, 12, 31))
    assert dim.count() == 365
    row = dim.filter("date_key = 20171124").first()
    assert (row.day_name, row.is_weekend, row.quarter) == ("Friday", False, 4)
