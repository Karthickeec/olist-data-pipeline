"""Checks for the replay: idempotency snapshots and full comparison with the CSVs."""

import hashlib
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from olist_pipeline.lake import list_dirs, path_exists
from olist_pipeline.sources import TABLES, read_table

SILVER_TABLES = (
    "geolocation",
    "products",
    "sellers",
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "customer_changes",
    "customer_activity",
    "order_lines",
)
# Columns that legitimately change on a rerun: when a row was (re)processed and, for the
# derived order_lines, which batch last rebuilt its partition.
PROCESSING_COLUMNS = {"_processed_at"}
REBUILT_COLUMNS = {"order_lines": {"_batch_date", "_first_batch_date"}}


def table_fingerprint(conn: psycopg.Connection, name: str) -> tuple[int, str]:
    """(row count, md5 of every row incl. updated_at in key order)."""
    key = sql.SQL(", ").join(sql.Identifier("t", c) for c in TABLES[name].key)
    query = sql.SQL("SELECT count(*), coalesce(md5(string_agg(t::text, E'\\n' ORDER BY {key})), '') FROM {t} t").format(
        key=key, t=sql.Identifier(name)
    )
    return tuple(conn.execute(query).fetchone())


def landing_checksum(landing_dir: Path) -> str:
    digest = hashlib.sha256()
    root = Path(landing_dir)
    for path in sorted(root.rglob("*")):
        if path.is_file():
            digest.update(str(path.relative_to(root)).encode() + b"\0" + path.read_bytes())
    return digest.hexdigest()


def snapshot(conn: psycopg.Connection, landing_dir: Path) -> dict:
    snap = {name: table_fingerprint(conn, name) for name in TABLES}
    snap["landing"] = landing_checksum(landing_dir)
    return snap


@dataclass
class TableDiff:
    csv_rows: int
    db_rows: int
    missing: list = field(default_factory=list)  # key in CSV, not in DB
    extra: list = field(default_factory=list)  # key in DB, not in CSV
    different: list = field(default_factory=list)  # same key, different values

    @property
    def ok(self) -> bool:
        return not (self.missing or self.extra or self.different)


def compare_table(conn: psycopg.Connection, raw_dir: Path, name: str) -> TableDiff:
    """Compare every source column (not updated_at) of a table with its CSV, by key."""
    spec = TABLES[name]

    def key_of(row):
        return tuple(row[c] for c in spec.key)

    expected = {key_of(r): r for r in read_table(raw_dir, name)}
    cols = sql.SQL(", ").join(map(sql.Identifier, spec.column_names))
    with conn.cursor(row_factory=dict_row) as cur:
        actual = {key_of(r): r for r in cur.execute(sql.SQL("SELECT {} FROM {}").format(cols, sql.Identifier(name)))}
    return TableDiff(
        csv_rows=len(expected),
        db_rows=len(actual),
        missing=sorted(expected.keys() - actual.keys()),
        extra=sorted(actual.keys() - expected.keys()),
        different=sorted(k for k in expected.keys() & actual.keys() if expected[k] != actual[k]),
    )


def lake_fingerprint(
    spark: SparkSession, path: str, ignore: set[str], partitioned: bool = False
) -> tuple[int, int] | None:
    """Order-independent content fingerprint of a lake table: (rows, sum of 64-bit row hashes)."""
    if not path_exists(spark, path) or (partitioned and not list_dirs(spark, path)):
        return None
    df = spark.read.parquet(path)
    cols = [c for c in df.columns if c not in ignore]
    row = df.agg(F.count(F.lit(1)), F.sum(F.xxhash64(*cols))).first()
    return row[0], row[1]


def silver_snapshot(spark: SparkSession, root: str) -> dict:
    """Fingerprints of every Silver table and its quarantine and pending areas."""
    from olist_pipeline.silver.common import pending_dir, quarantine_dir, silver_dir

    snap = {}
    for t in SILVER_TABLES:
        snap[f"silver.{t}"] = lake_fingerprint(
            spark, silver_dir(root, t), PROCESSING_COLUMNS | REBUILT_COLUMNS.get(t, set())
        )
        snap[f"quarantine.{t}"] = lake_fingerprint(spark, quarantine_dir(root, t), set(), partitioned=True)
        snap[f"pending.{t}"] = lake_fingerprint(spark, pending_dir(root, t), set(), partitioned=True)
    return snap
