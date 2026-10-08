import re
from datetime import datetime
from decimal import Decimal

import pytest

from olist_pipeline.config import PROJECT_ROOT, load_config
from olist_pipeline.replay_logic import build_index, change_days, order_as_of
from olist_pipeline.sources import TABLES, TRANSACTIONAL_TABLES, load_sources, read_table

RAW_DIR = load_config(environ={})["paths"]["raw_dir"]
needs_raw = pytest.mark.skipif(not (RAW_DIR / "olist_orders_dataset.csv").exists(), reason="Olist CSVs not in data/raw")


def schema_columns() -> dict[str, list[str]]:
    ddl = (PROJECT_ROOT / "sql" / "001_schema.sql").read_text()
    tables = {}
    for name, body in re.findall(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*?)\n\);", ddl, re.S):
        lines = [line.strip() for line in body.strip().splitlines()]
        tables[name] = [line.split()[0] for line in lines if not line.startswith("PRIMARY KEY (")]
    return tables


def test_specs_match_schema():
    ddl = schema_columns()
    assert set(ddl) == set(TABLES)
    for name, spec in TABLES.items():
        expected = list(spec.column_names) + (["updated_at"] if name in TRANSACTIONAL_TABLES else [])
        assert ddl[name] == expected, name


def test_read_table_types_bom_and_nulls(tmp_path, monkeypatch):
    (tmp_path / "product_category_name_translation.csv").write_text(
        "﻿product_category_name,product_category_name_english\nbeleza_saude,health_beauty\n", encoding="utf-8"
    )
    (tmp_path / "olist_order_items_dataset.csv").write_text(
        '"order_id","order_item_id","product_id","seller_id","shipping_limit_date","price","freight_value"\n'
        '"o1",1,"p1","s1",2017-09-19 09:45:35,58.90,\n'
    )
    assert read_table(tmp_path, "product_category_name_translation") == [
        {"product_category_name": "beleza_saude", "product_category_name_english": "health_beauty"}
    ]
    [item] = read_table(tmp_path, "order_items")
    assert item["order_item_id"] == 1
    assert item["shipping_limit_date"] == datetime(2017, 9, 19, 9, 45, 35)
    assert item["price"] == Decimal("58.90")
    assert item["freight_value"] is None


@needs_raw
def test_real_data_replays_to_final_state():
    """Without a DB: every order converges to its CSV row, and every row is scheduled once."""
    src = load_sources(RAW_DIR)
    idx = build_index(src)
    for o in src.orders.values():
        assert order_as_of(o, max(change_days(o))) == o
    assert sum(map(len, idx.purchased_on.values())) == len(src.orders)
    assert sum(map(len, idx.reviews_on.values())) == len(src.reviews)
    assert len(idx.known_ids) == len({c["customer_unique_id"] for c in src.customers.values()})
    assert str(idx.first_day) == "2016-09-04"
    assert str(idx.last_day) == "2018-10-17"
