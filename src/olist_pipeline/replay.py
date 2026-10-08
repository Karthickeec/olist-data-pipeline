"""Replay Olist history into Postgres one day at a time.

Each day runs in one transaction:
  * orders purchased that day are inserted as they looked at the end of the day;
  * earlier orders that hit a milestone that day are updated. The upsert only
    applies when the stored row is older (updated_at) and actually different, so
    rerunning a day, or replaying an older one, changes nothing;
  * the day's customers, items and payments are inserted, and reviews are
    released on max(creation date, purchase date). These rows never change.
After the commit, the day's synthetic address-change file is (re)written.
"""
import argparse
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import psycopg
from psycopg import sql

from olist_pipeline.config import load_config
from olist_pipeline.customer_changes import generate_changes, write_changes
from olist_pipeline.db import connect
from olist_pipeline.replay_logic import ReplayIndex, build_index, end_of_day, order_as_of
from olist_pipeline.sources import TABLES, Sources, load_sources, read_address_pool


def _insert_new(table: str) -> sql.Composed:
    """INSERT ... ON CONFLICT (key) DO NOTHING, with named placeholders."""
    spec = TABLES[table]
    cols = spec.column_names + ("updated_at",)
    return sql.SQL("INSERT INTO {t} ({cols}) VALUES ({vals}) ON CONFLICT ({key}) DO NOTHING").format(
        t=sql.Identifier(table),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(map(sql.Placeholder, cols)),
        key=sql.SQL(", ").join(map(sql.Identifier, spec.key)),
    )


def _upsert_orders() -> sql.Composed:
    """Insert, or update only when the incoming state is newer and different."""
    spec = TABLES["orders"]
    cols = spec.column_names + ("updated_at",)
    tracked = [c for c in spec.column_names if c not in spec.key]

    def row(alias: str) -> sql.Composed:
        return sql.SQL(", ").join(sql.Identifier(alias, c) for c in tracked)

    return sql.SQL(
        "INSERT INTO {t} ({cols}) VALUES ({vals}) "
        "ON CONFLICT ({key}) DO UPDATE SET {sets} "
        "WHERE {t}.updated_at < excluded.updated_at "
        "AND ({old}) IS DISTINCT FROM ({new})"
    ).format(
        t=sql.Identifier("orders"),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        vals=sql.SQL(", ").join(map(sql.Placeholder, cols)),
        key=sql.SQL(", ").join(map(sql.Identifier, spec.key)),
        sets=sql.SQL(", ").join(
            sql.SQL("{c} = excluded.{c}").format(c=sql.Identifier(c)) for c in tracked + ["updated_at"]
        ),
        old=row("orders"),
        new=row("excluded"),
    )


INSERT_CUSTOMERS = _insert_new("customers")
INSERT_ITEMS = _insert_new("order_items")
INSERT_PAYMENTS = _insert_new("order_payments")
INSERT_REVIEWS = _insert_new("order_reviews")
UPSERT_ORDERS = _upsert_orders()


@dataclass
class DayResult:
    day: date
    customers: int = 0
    orders: int = 0
    items: int = 0
    payments: int = 0
    reviews: int = 0
    changes: int = 0

    def __str__(self) -> str:
        return (f"{self.day}  customers={self.customers} orders={self.orders} items={self.items} "
                f"payments={self.payments} reviews={self.reviews} change_requests={self.changes}")


def _write(cur: psycopg.Cursor, query: sql.Composed, rows: list[dict], updated_at) -> int:
    """Run `query` for each row; returns the number of rows actually written."""
    if not rows:
        return 0
    cur.executemany(query, [{**r, "updated_at": updated_at} for r in rows])
    return cur.rowcount


def replay_day(conn: psycopg.Connection, src: Sources, idx: ReplayIndex, day: date,
               landing_dir: Path, address_pool: list, changes_cfg: dict) -> DayResult:
    stamp = end_of_day(day)
    purchased = idx.purchased_on.get(day, [])
    reviews = idx.reviews_on.get(day, [])
    # Every order a row written today points at is upserted too, so FKs hold
    # even if days are replayed out of order.
    order_ids = sorted(set(purchased) | set(idx.changed_on.get(day, []))
                       | {r["order_id"] for r in reviews})
    orders = [order_as_of(src.orders[oid], day) for oid in order_ids]
    customers = [src.customers[o["customer_id"]] for o in orders]
    items = [i for oid in purchased for i in src.items.get(oid, [])]
    payments = [p for oid in purchased for p in src.payments.get(oid, [])]

    result = DayResult(day)
    with conn.transaction(), conn.cursor() as cur:
        result.customers = _write(cur, INSERT_CUSTOMERS, customers, stamp)
        result.orders = _write(cur, UPSERT_ORDERS, orders, stamp)
        result.items = _write(cur, INSERT_ITEMS, items, stamp)
        result.payments = _write(cur, INSERT_PAYMENTS, payments, stamp)
        result.reviews = _write(cur, INSERT_REVIEWS, reviews, stamp)

    changes = generate_changes(
        day, len(purchased), idx.known_customers(day), address_pool,
        rate=changes_cfg["rate"], dirty_rate=changes_cfg["dirty_rate"],
    )
    write_changes(landing_dir, day, changes)
    result.changes = len(changes)
    return result


def daterange(start: date, end: date):
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)


def _require_reference_data(conn: psycopg.Connection) -> None:
    try:
        seeded = conn.execute("SELECT EXISTS (SELECT 1 FROM products)").fetchone()[0]
    except psycopg.errors.UndefinedTable:
        seeded = False
    if not seeded:
        sys.exit("Reference tables are empty or missing; run `make seed` first.")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Replay Olist history into the source Postgres.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=date.fromisoformat, help="replay a single day (YYYY-MM-DD)")
    g.add_argument("--start", type=date.fromisoformat, help="first day of a range (inclusive)")
    g.add_argument("--all", action="store_true", help="replay the full history")
    p.add_argument("--end", type=date.fromisoformat, help="last day of a range (inclusive)")
    args = p.parse_args(argv)
    if (args.start is None) != (args.end is None):
        p.error("--start and --end must be given together")
    if args.start and args.end < args.start:
        p.error("--end is before --start")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    cfg = load_config()
    raw_dir, landing_dir = cfg["paths"]["raw_dir"], cfg["paths"]["landing_dir"]

    src = load_sources(raw_dir)
    idx = build_index(src)
    pool = read_address_pool(raw_dir)
    if args.date:
        start = end = args.date
    elif args.all:
        start, end = idx.first_day, idx.last_day
    else:
        start, end = args.start, args.end

    with connect(cfg["pg"]) as conn:
        _require_reference_data(conn)
        for day in daterange(start, end):
            print(replay_day(conn, src, idx, day, landing_dir, pool, cfg["customer_changes"]), flush=True)
