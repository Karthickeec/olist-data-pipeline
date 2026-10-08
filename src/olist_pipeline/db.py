"""Postgres connection and schema helpers."""
import re

import psycopg
from psycopg import sql

from olist_pipeline.config import PROJECT_ROOT

SQL_DIR = PROJECT_ROOT / "sql"
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


def connect(pg: dict) -> psycopg.Connection:
    """Autocommit connection with search_path set to the configured schema.

    Callers group work with `conn.transaction()`.
    """
    schema = pg["schema"]
    if not _IDENTIFIER.match(schema):
        raise ValueError(f"invalid schema name: {schema!r}")
    return psycopg.connect(
        host=pg["host"], port=pg["port"], dbname=pg["dbname"],
        user=pg["user"], password=pg["password"],
        options=f"-c search_path={schema}",
        autocommit=True,
    )


def apply_schema(conn: psycopg.Connection, schema: str) -> None:
    """Create the schema and run sql/*.sql in name order (all statements are idempotent)."""
    with conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("SET LOCAL search_path TO {}").format(sql.Identifier(schema)))
        for path in sorted(SQL_DIR.glob("*.sql")):
            conn.execute(path.read_text(encoding="utf-8"))
