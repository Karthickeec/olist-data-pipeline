"""silver.order_lines: one row per order item with order, customer, product, seller and payments."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def aggregate_payments(payments: DataFrame) -> DataFrame:
    """One row per order. The main payment type is the one with the largest value (ties: by name)."""
    return payments.groupBy("order_id").agg(
        F.sum("payment_value").alias("payment_total"),
        F.count(F.lit(1)).alias("payment_count"),
        F.max_by("payment_type", F.struct("payment_value", "payment_type")).alias("payment_type_main"),
        F.max("payment_installments").alias("payment_installments_max"),
    )


def allocate_payment(lines: DataFrame) -> DataFrame:
    """Split each order's payment_total across its lines by price + freight, to the cent.

    Every line but the last gets its rounded share; the last gets the remainder, so the
    lines of an order always sum exactly to payment_total (no double counting in Gold).
    """
    w = Window.partitionBy("order_id")
    last = Window.partitionBy("order_id").orderBy(F.col("order_item_id").desc())
    weight = F.col("line_total") / F.sum("line_total").over(w)
    n = F.count(F.lit(1)).over(w)
    share = F.when(F.sum("line_total").over(w) > 0, weight).otherwise(F.lit(1) / n)
    rounded = F.round(F.col("payment_total") * share, 2).cast("decimal(12,2)")
    lines = lines.withColumn("_rounded", rounded).withColumn("_is_last", F.row_number().over(last) == 1)
    others = F.sum(F.when(~F.col("_is_last"), F.col("_rounded")).otherwise(F.lit(0))).over(w)
    return lines.withColumn(
        "allocated_payment",
        F.when(F.col("_is_last"), (F.col("payment_total") - others).cast("decimal(12,2)")).otherwise(F.col("_rounded")),
    ).drop("_rounded", "_is_last")


def build_order_lines(
    orders: DataFrame,
    items: DataFrame,
    payments: DataFrame,
    customers: DataFrame,
    products: DataFrame,
    sellers: DataFrame,
) -> DataFrame:
    """orders/items/payments: already limited to the purchase-date partitions being rebuilt."""
    o = orders.select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "order_purchase_date",
    )
    i = items.select(
        "order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"
    )
    # items ⋈ orders and ⋈ payment aggregates: both sides grow with the data (100k+ rows at
    # full scale), so they use Spark's shuffle sort-merge join. Each batch only reads the
    # purchase-date partitions it rebuilds, which keeps the shuffle small.
    lines = i.join(o, "order_id").join(aggregate_payments(payments), "order_id", "left")
    # Dimension-like lookups are small (customers ≈99k ids, products 33k, sellers 3k: a few MB
    # each), so they are broadcast to every task and the large side is never shuffled for them.
    lines = (
        lines.join(
            F.broadcast(
                customers.select(
                    "customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"
                )
            ),
            "customer_id",
            "left",
        )
        .join(
            F.broadcast(products.select("product_id", "product_category_name", "product_category_name_english")),
            "product_id",
            "left",
        )
        .join(
            F.broadcast(sellers.select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")),
            "seller_id",
            "left",
        )
        .withColumn("line_total", (F.col("price") + F.col("freight_value")).cast("decimal(12,2)"))
    )
    lines = allocate_payment(lines)
    return lines.select(
        "order_id",
        "order_item_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "shipping_limit_date",
        "price",
        "freight_value",
        "line_total",
        "payment_total",
        "payment_count",
        "payment_type_main",
        "payment_installments_max",
        "allocated_payment",
        "order_purchase_date",
    )
