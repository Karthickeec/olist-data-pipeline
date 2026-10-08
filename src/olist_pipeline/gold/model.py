"""Pure builders for the Gold tables (no I/O), so each can be unit-tested on small DataFrames.

Surrogate keys are deterministic 64-bit hashes of the natural key (plus valid_from for
SCD2 versions), so rebuilding a dimension never changes the keys facts point to.
"""

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

OPEN_END = "9999-12-31 00:00:00"
NOT_A_SALE = ("canceled", "unavailable")  # excluded from sales and customer value
RFM_SEGMENTS = ("Champions", "Loyal", "Potential", "At risk", "Hibernating", "Lost")


def surrogate_key(kind: str, *cols: str) -> Column:
    return F.xxhash64(F.lit(kind), *[F.col(c) for c in cols])


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
        "date",
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.weekofyear("date").alias("week_of_year"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        surrogate_key("product", "product_id").alias("product_sk"),
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    )


def build_dim_seller(sellers: DataFrame, geolocation: DataFrame) -> DataFrame:
    # Geolocation is one row per zip prefix (19k rows): broadcast it.
    geo = geolocation.select(F.col("zip_code_prefix").alias("seller_zip_code_prefix"), "lat", "lng")
    return sellers.join(F.broadcast(geo), "seller_zip_code_prefix", "left").select(
        surrogate_key("seller", "seller_id").alias("seller_sk"),
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "lat",
        "lng",
    )


def build_dim_customer(orders: DataFrame, customers: DataFrame, changes: DataFrame) -> DataFrame:
    """SCD Type 2 per customer_unique_id.

    Version 1 is the address on the customer's first order, valid from that order's timestamp.
    Each CRM address change opens a new version at requested_at (never before the first order)
    and closes the previous one; changes that keep the same address are skipped. When several
    events fall on the same instant, the last one wins. valid_to is exclusive; the current
    version ends at 9999-12-31.
    """
    first_order = Window.partitionBy("customer_unique_id").orderBy("order_purchase_timestamp", "order_id")
    first = (
        orders.select("order_id", "customer_id", "order_purchase_timestamp")
        .join(
            customers.select(
                "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
            ),
            "customer_id",
        )
        .withColumn("_rn", F.row_number().over(first_order))
        .filter("_rn = 1")
    )
    initial = first.select(
        "customer_unique_id",
        F.col("order_purchase_timestamp").alias("ts"),
        F.lit(0).alias("prio"),
        F.lit("").alias("seq"),
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
    )
    starts = first.select("customer_unique_id", F.col("order_purchase_timestamp").alias("_first_ts"))
    moves = changes.join(starts, "customer_unique_id").select(
        "customer_unique_id",
        F.greatest("requested_at", "_first_ts").alias("ts"),
        F.lit(1).alias("prio"),
        F.concat_ws("|", F.col("requested_at").cast("string"), "change_id").alias("seq"),
        F.col("new_zip_code_prefix").alias("zip_code_prefix"),
        F.col("new_city").alias("city"),
        F.col("new_state").alias("state"),
    )
    events = initial.unionByName(moves)

    same_instant = Window.partitionBy("customer_unique_id", "ts").orderBy(F.col("prio").desc(), F.col("seq").desc())
    events = events.withColumn("_rn", F.row_number().over(same_instant)).filter("_rn = 1").drop("_rn")
    by_time = Window.partitionBy("customer_unique_id").orderBy("ts")
    address = F.struct("zip_code_prefix", "city", "state")
    events = (
        events.withColumn("_prev", F.lag(address).over(by_time))
        .filter(F.col("_prev").isNull() | (F.col("_prev") != address))
        .drop("_prev")
    )
    nxt = F.lead("ts").over(by_time)
    return events.select(
        F.xxhash64(F.lit("customer"), "customer_unique_id", "ts").alias("customer_sk"),
        "customer_unique_id",
        "zip_code_prefix",
        "city",
        "state",
        F.col("ts").alias("valid_from"),
        F.coalesce(nxt, F.lit(OPEN_END).cast("timestamp")).alias("valid_to"),
        nxt.isNull().alias("is_current"),
        F.row_number().over(by_time).alias("version"),
    )


def build_fact_order_lines(lines: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """One row per order item, keyed to the dimensions; customer_sk is the version valid at purchase."""
    # dim_customer is ~1 row per customer (≈96k at full scale, a few MB): broadcast it. The join is
    # an equi-join on customer_unique_id plus a range condition on the purchase timestamp.
    versions = dim_customer.select(
        "customer_sk",
        F.col("customer_unique_id").alias("_cuid"),
        "valid_from",
        "valid_to",
        F.col("state").alias("customer_state"),
    )
    # customer_state comes from the SCD2 version valid at purchase, not from the order's own address.
    lines = lines.drop("customer_state")
    joined = lines.join(
        F.broadcast(versions),
        (lines.customer_unique_id == versions._cuid)
        & (lines.order_purchase_timestamp >= versions.valid_from)
        & (lines.order_purchase_timestamp < versions.valid_to),
        "left",
    )
    return joined.select(
        "order_id",
        "order_item_id",
        F.date_format("order_purchase_date", "yyyyMMdd").cast("int").alias("date_key"),
        "customer_sk",
        "customer_unique_id",
        surrogate_key("product", "product_id").alias("product_sk"),
        surrogate_key("seller", "seller_id").alias("seller_sk"),
        "order_status",
        "order_purchase_timestamp",
        "customer_state",
        "payment_type_main",
        "payment_installments_max",
        "price",
        "freight_value",
        "line_total",
        "allocated_payment",
        "order_purchase_date",
    )


def build_daily_category_sales(fact: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Sales per day and English category. Canceled/unavailable orders are not sales."""
    # dim_product is 33k rows: broadcast.
    sales = fact.filter(~F.col("order_status").isin(*NOT_A_SALE)).join(
        F.broadcast(dim_product.select("product_sk", "product_category_name_english")), "product_sk"
    )
    return (
        sales.groupBy("order_purchase_date", F.col("product_category_name_english").alias("category"))
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.count(F.lit(1)).alias("items"),
            F.sum("price").alias("revenue"),
            F.sum("freight_value").alias("freight"),
            F.sum("line_total").alias("gmv"),
        )
        .withColumn("avg_item_price", (F.col("revenue") / F.col("items")).cast("decimal(12,2)"))
    )


def frequency_band(orders: Column) -> Column:
    """Fixed bands: ~97% of customers ordered once, so ntile on frequency would be meaningless."""
    return F.when(orders >= 6, 5).when(orders >= 4, 4).when(orders == 3, 3).when(orders == 2, 2).otherwise(1)


def rfm_segment(r: Column, f: Column, m: Column) -> Column:
    """First matching rule wins (see README for the rationale)."""
    return (
        F.when((r >= 4) & (f >= 2) & (m >= 4), "Champions")
        .when((r >= 2) & (f >= 2), "Loyal")
        .when(r >= 4, "Potential")
        .when((r >= 2) & (m >= 4), "At risk")
        .when(r >= 2, "Hibernating")
        .otherwise("Lost")
    )


def build_customer_metrics(fact: DataFrame, as_of: date) -> DataFrame:
    """Customer value as of a date, from sales up to and including that date."""
    day = F.lit(as_of.isoformat()).cast("date")
    sales = fact.filter((F.col("order_purchase_date") <= day) & ~F.col("order_status").isin(*NOT_A_SALE))
    per = sales.groupBy("customer_unique_id").agg(
        F.sum("allocated_payment").alias("lifetime_value"),
        F.countDistinct("order_id").alias("order_count"),
        F.min("order_purchase_date").alias("first_order_date"),
        F.max("order_purchase_date").alias("last_order_date"),
    )
    per = (
        per.withColumn("lifetime_value", F.coalesce("lifetime_value", F.lit(0)).cast("decimal(14,2)"))
        .withColumn("avg_order_value", (F.col("lifetime_value") / F.col("order_count")).cast("decimal(12,2)"))
        .withColumn("days_since_last_order", F.datediff(day, "last_order_date"))
    )
    # Scores over all customers (a single global window: ~96k rows at full scale is fine on one task).
    # Ties are broken by id so the scores are deterministic.
    recency = Window.orderBy(F.col("days_since_last_order").desc(), "customer_unique_id")
    monetary = Window.orderBy(F.col("lifetime_value").asc(), "customer_unique_id")
    per = (
        per.withColumn("r_score", F.ntile(5).over(recency))
        .withColumn("f_score", frequency_band(F.col("order_count")))
        .withColumn("m_score", F.ntile(5).over(monetary))
    )
    return per.withColumn("rfm_segment", rfm_segment(F.col("r_score"), F.col("f_score"), F.col("m_score"))).withColumn(
        "as_of_date", day
    )
