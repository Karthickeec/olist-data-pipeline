"""Olist CSV definitions and typed loading.

The column specs here mirror sql/001_schema.sql (tests/test_sources.py checks
that they agree). Empty CSV fields become None.
"""
import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

PARSERS = {
    "text": str,
    "ts": datetime.fromisoformat,
    "num": Decimal,
    "int": int,
    "float": float,
}
ROW_NUMBER = "rownum"  # synthetic column: 1-based data row number in the CSV


@dataclass(frozen=True)
class Table:
    name: str
    filename: str
    columns: tuple[tuple[str, str], ...]
    key: tuple[str, ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)


TABLES = {t.name: t for t in (
    Table("product_category_name_translation", "product_category_name_translation.csv", (
        ("product_category_name", "text"),
        ("product_category_name_english", "text"),
    ), ("product_category_name",)),
    Table("products", "olist_products_dataset.csv", (
        ("product_id", "text"),
        ("product_category_name", "text"),
        ("product_name_lenght", "int"),
        ("product_description_lenght", "int"),
        ("product_photos_qty", "int"),
        ("product_weight_g", "int"),
        ("product_length_cm", "int"),
        ("product_height_cm", "int"),
        ("product_width_cm", "int"),
    ), ("product_id",)),
    Table("sellers", "olist_sellers_dataset.csv", (
        ("seller_id", "text"),
        ("seller_zip_code_prefix", "text"),
        ("seller_city", "text"),
        ("seller_state", "text"),
    ), ("seller_id",)),
    Table("geolocation", "olist_geolocation_dataset.csv", (
        ("geolocation_row_id", ROW_NUMBER),
        ("geolocation_zip_code_prefix", "text"),
        ("geolocation_lat", "float"),
        ("geolocation_lng", "float"),
        ("geolocation_city", "text"),
        ("geolocation_state", "text"),
    ), ("geolocation_row_id",)),
    Table("customers", "olist_customers_dataset.csv", (
        ("customer_id", "text"),
        ("customer_unique_id", "text"),
        ("customer_zip_code_prefix", "text"),
        ("customer_city", "text"),
        ("customer_state", "text"),
    ), ("customer_id",)),
    Table("orders", "olist_orders_dataset.csv", (
        ("order_id", "text"),
        ("customer_id", "text"),
        ("order_status", "text"),
        ("order_purchase_timestamp", "ts"),
        ("order_approved_at", "ts"),
        ("order_delivered_carrier_date", "ts"),
        ("order_delivered_customer_date", "ts"),
        ("order_estimated_delivery_date", "ts"),
    ), ("order_id",)),
    Table("order_items", "olist_order_items_dataset.csv", (
        ("order_id", "text"),
        ("order_item_id", "int"),
        ("product_id", "text"),
        ("seller_id", "text"),
        ("shipping_limit_date", "ts"),
        ("price", "num"),
        ("freight_value", "num"),
    ), ("order_id", "order_item_id")),
    Table("order_payments", "olist_order_payments_dataset.csv", (
        ("order_id", "text"),
        ("payment_sequential", "int"),
        ("payment_type", "text"),
        ("payment_installments", "int"),
        ("payment_value", "num"),
    ), ("order_id", "payment_sequential")),
    Table("order_reviews", "olist_order_reviews_dataset.csv", (
        ("review_id", "text"),
        ("order_id", "text"),
        ("review_score", "int"),
        ("review_comment_title", "text"),
        ("review_comment_message", "text"),
        ("review_creation_date", "ts"),
        ("review_answer_timestamp", "ts"),
    ), ("review_id", "order_id")),
)}

REFERENCE_TABLES = ("product_category_name_translation", "products", "sellers", "geolocation")
TRANSACTIONAL_TABLES = ("customers", "orders", "order_items", "order_payments", "order_reviews")


def read_table(raw_dir: Path, name: str) -> list[dict]:
    """Read one CSV into typed dicts keyed by the table's column names."""
    table = TABLES[name]
    # utf-8-sig strips the BOM at the start of the category translation file.
    with open(Path(raw_dir) / table.filename, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        expected = {c for c, t in table.columns if t != ROW_NUMBER}
        missing = expected - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"{table.filename} is missing columns {sorted(missing)}")
        rows = []
        for rownum, raw in enumerate(reader, start=1):
            row = {}
            for col, typ in table.columns:
                if typ == ROW_NUMBER:
                    row[col] = rownum
                else:
                    value = raw[col]
                    row[col] = None if value == "" else PARSERS[typ](value)
            rows.append(row)
    return rows


def read_address_pool(raw_dir: Path) -> list[tuple[str, str, str]]:
    """Distinct (zip prefix, city, state) triples from geolocation, sorted for determinism."""
    with open(Path(raw_dir) / TABLES["geolocation"].filename, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        pool = {
            (r["geolocation_zip_code_prefix"], r["geolocation_city"], r["geolocation_state"])
            for r in reader
        }
    return sorted(pool)


@dataclass
class Sources:
    """The transactional CSVs, indexed the way the replay needs them."""
    customers: dict[str, dict]           # by customer_id
    orders: dict[str, dict]              # by order_id (final state)
    items: dict[str, list[dict]]         # by order_id
    payments: dict[str, list[dict]]      # by order_id
    reviews: list[dict]


def load_sources(raw_dir: Path) -> Sources:
    return Sources(
        customers={r["customer_id"]: r for r in read_table(raw_dir, "customers")},
        orders={r["order_id"]: r for r in read_table(raw_dir, "orders")},
        items=_group(read_table(raw_dir, "order_items"), "order_id"),
        payments=_group(read_table(raw_dir, "order_payments"), "order_id"),
        reviews=read_table(raw_dir, "order_reviews"),
    )


def _group(rows: list[dict], key: str) -> dict[str, list[dict]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[row[key]].append(row)
    return dict(grouped)
