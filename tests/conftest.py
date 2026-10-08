import os
import shutil
import time
from pathlib import Path

# PySpark converts timestamps between Spark (UTC session) and Python using the *process* time zone.
# Pin it to UTC so values created in and collected from tests match what Spark stores.
os.environ["TZ"] = "UTC"
time.tzset()

import pytest  # noqa: E402  (after pinning TZ on purpose)

from olist_pipeline.config import load_config  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    """One local SparkSession for the whole test run (local[2], 2g driver, UTC)."""
    cfg = load_config()
    if not Path(cfg["spark"]["java_home"]).is_dir() and not shutil.which("java"):
        pytest.skip("no Java 17 found; run `make java`")
    from olist_pipeline.spark import build_spark

    session = build_spark(cfg, "pytest")
    yield session
    session.stop()


def assert_same_rows(a, b):
    """Same multiset of rows (order-insensitive), compared inside Spark."""
    assert a.columns == b.columns
    assert a.count() == b.count()
    assert a.exceptAll(b).isEmpty()
    assert b.exceptAll(a).isEmpty()


def require_raw_csvs(raw_dir: Path) -> None:
    """The Kaggle CSVs aren't in the repo (CI has none): tests that need them skip."""
    if not (Path(raw_dir) / "olist_orders_dataset.csv").exists():
        pytest.skip("Olist CSVs not in data/raw")


@pytest.fixture(scope="session")
def activity_index():
    """Customer index for the mock API, built from the real CSVs."""
    raw_dir = load_config()["paths"]["raw_dir"]
    require_raw_csvs(raw_dir)
    from olist_pipeline.api.activity import build_customer_index

    return build_customer_index(raw_dir)
