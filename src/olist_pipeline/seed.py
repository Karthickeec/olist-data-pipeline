"""Load reference tables once. Insert-only: rows whose key already exists are left alone.

Never truncates; order_items references products and sellers.
"""
from pathlib import Path

import psycopg
from psycopg import sql

from olist_pipeline.sources import REFERENCE_TABLES, TABLES, read_table


def seed_table(conn: psycopg.Connection, raw_dir: Path, name: str) -> tuple[int, int]:
    """COPY the CSV into a temp table, then insert the missing keys. Returns (read, inserted)."""
    table = TABLES[name]
    rows = read_table(raw_dir, name)
    target = sql.Identifier(name)
    stage = sql.Identifier(f"stage_{name}")
    cols = sql.SQL(", ").join(map(sql.Identifier, table.column_names))
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(stage, target))
        with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(stage, cols)) as copy:
            for row in rows:
                copy.write_row([row[c] for c in table.column_names])
        cur.execute(sql.SQL(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage} ON CONFLICT DO NOTHING"
        ).format(target=target, cols=cols, stage=stage))
        inserted = cur.rowcount
    return len(rows), inserted


def seed_reference(conn: psycopg.Connection, raw_dir: Path,
                   tables: tuple[str, ...] = REFERENCE_TABLES) -> dict[str, tuple[int, int]]:
    return {name: seed_table(conn, raw_dir, name) for name in tables}
