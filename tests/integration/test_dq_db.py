"""DQ CLI end to end: an injected bad Silver batch fails the run and lands in dq_results."""
import os
import subprocess
import sys
from datetime import date, datetime
from decimal import Decimal

import psycopg
import pytest
from pyspark.sql import functions as F

from olist_pipeline.config import PROJECT_ROOT, load_config
from olist_pipeline.db import connect

pytestmark = pytest.mark.integration
PIPELINE = "pipeline_dq_test"
D = date(2017, 3, 1)
TABLES = ["silver.customers", "silver.orders", "silver.order_lines"]


@pytest.fixture(scope="module")
def conn():
    cfg = load_config()
    try:
        c = connect(cfg["pg"])
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    c.execute(f"DROP SCHEMA IF EXISTS {PIPELINE} CASCADE")
    yield c
    c.execute(f"DROP SCHEMA IF EXISTS {PIPELINE} CASCADE")
    c.close()


def meta(df):
    return (df.withColumn("_batch_date", F.lit(D).cast("date")).withColumn("_first_batch_date", F.lit(D).cast("date"))
              .withColumn("_processed_at", F.current_timestamp()))


def write_bad_batch(spark, root):
    meta(spark.createDataFrame([("c1", "u1", "SP"), ("c2", "u2", "XX")],         # XX: not a state
                               "customer_id string, customer_unique_id string, customer_state string")
         ).write.mode("overwrite").parquet(f"{root}/silver/customers")
    ts = datetime(2017, 3, 1, 10)
    orders = spark.createDataFrame([
        ("o1", "c1", "delivered", ts, D), ("o1", "c1", "delivered", ts, D),     # duplicate key
        ("o2", None, "delivered", ts, D),                                       # null customer id
    ], "order_id string, customer_id string, order_status string, order_purchase_timestamp timestamp, "
       "order_purchase_date date").withColumn("updated_at", F.lit(ts))
    meta(orders).write.mode("overwrite").parquet(f"{root}/silver/orders")
    lines = spark.createDataFrame([
        ("o1", 1, "c1", "u1", "SP", "health_beauty", "s1", Decimal("10.00"), Decimal("10.00"), Decimal("10.00")),
        ("o3", 1, "c9", None, None, "toys", "s1", Decimal("5.00"), Decimal("5.00"), Decimal("5.00")),  # orphan line
    ], "order_id string, order_item_id int, customer_id string, customer_unique_id string, customer_state string, "
       "product_category_name_english string, seller_id string, price decimal(10,2), "
       "payment_total decimal(20,2), allocated_payment decimal(12,2)")
    meta(lines).write.mode("overwrite").parquet(f"{root}/silver/order_lines")


def run_cli(root, *args):
    env = {**os.environ, "OLIST__LAKE__ROOT": root, "OLIST__PG__PIPELINE_SCHEMA": PIPELINE}
    return subprocess.run([sys.executable, str(PROJECT_ROOT / "scripts" / "dq.py"), "--date", str(D), *args],
                          env=env, capture_output=True, text=True, timeout=600)


def results(conn):
    return {r[0]: r[1:] for r in conn.execute(
        f"SELECT table_name || ' ' || check_name, status, failed_rows, sample::text "
        f"FROM {PIPELINE}.dq_results WHERE batch_date = %s", (D,))}


def test_injected_bad_batch_fails_the_run(spark, conn, tmp_path):
    root = str(tmp_path / "lake")
    write_bad_batch(spark, root)
    proc = run_cli(root, "--layer", "silver", *[a for t in TABLES for a in ("--table", t)])
    assert proc.returncode == 1, proc.stdout + proc.stderr
    assert "-> FAILED" in proc.stdout

    r = results(conn)
    assert r["silver.customers accepted_values:customer_state"][:2] == ("fail", 1)
    assert '"customer_state": "XX"' in r["silver.customers accepted_values:customer_state"][2]
    assert r["silver.orders unique:order_id"][:2] == ("fail", 2)
    assert r["silver.orders not_null:order_id,customer_id,order_status,order_purchase_timestamp"][:2] == ("fail", 1)
    assert r["silver.order_lines every_line_has_a_customer"][:2] == ("fail", 1)
    rel = r["silver.order_lines relationship:customer_id->silver.customers.customer_id"]
    assert rel[:2] == ("fail", 1) and '"customer_id": "c9"' in rel[2]

    # Rerun: results are replaced, not duplicated.
    n = conn.execute(f"SELECT count(*) FROM {PIPELINE}.dq_results WHERE batch_date = %s", (D,)).fetchone()[0]
    assert run_cli(root, "--layer", "silver", *[a for t in TABLES for a in ("--table", t)]).returncode == 1
    assert conn.execute(f"SELECT count(*) FROM {PIPELINE}.dq_results WHERE batch_date = %s",
                        (D,)).fetchone()[0] == n


def test_warn_only_failure_exits_zero(spark, conn, tmp_path):
    root = str(tmp_path / "lake")
    write_bad_batch(spark, root)
    suite = tmp_path / "warn.yaml"
    suite.write_text("tables:\n  silver.orders:\n    checks:\n"
                     "      - {type: unique, columns: [order_id], severity: warn}\n")
    proc = run_cli(root, "--layer", "silver", "--suite", str(suite))
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert results(conn)["silver.orders unique:order_id"][:2] == ("warn", 2)
