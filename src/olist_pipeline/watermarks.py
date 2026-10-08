"""Watermark, batch-window and reference-snapshot bookkeeping in Postgres."""
from dataclasses import dataclass
from datetime import date, datetime

import psycopg
from psycopg import sql

from olist_pipeline.db import SQL_DIR, apply_schema, check_identifier
from olist_pipeline.replay_logic import end_of_day


@dataclass(frozen=True)
class Window:
    """Extraction window on updated_at: (low, high]. low is None on the initial load."""
    low: datetime | None
    high: datetime

    def predicate(self) -> str:
        # Both bounds come from our own bookkeeping (datetimes), never from user input.
        high = f"updated_at <= TIMESTAMP '{self.high.isoformat(sep=' ')}'"
        if self.low is None:
            return high
        return f"updated_at > TIMESTAMP '{self.low.isoformat(sep=' ')}' AND {high}"

    def __str__(self) -> str:
        return f"({self.low or '-inf'}, {self.high}]"


class Bookkeeping:
    def __init__(self, conn: psycopg.Connection, schema: str):
        self.conn = conn
        self.schema = check_identifier(schema)

    def ensure_schema(self) -> None:
        apply_schema(self.conn, self.schema, SQL_DIR / "pipeline")

    def _t(self, name: str) -> sql.Identifier:
        return sql.Identifier(self.schema, name)

    def window(self, source: str, table: str, batch_date: date) -> Window:
        """Low bound = high_wm of the latest successful batch before batch_date.

        A rerun of D therefore reuses D's original window, and a batch after a
        gap covers every missed day.
        """
        row = self.conn.execute(
            sql.SQL("SELECT high_wm FROM {} WHERE source = %s AND table_name = %s AND batch_date < %s "
                    "ORDER BY batch_date DESC LIMIT 1").format(self._t("ingest_runs")),
            (source, table, batch_date),
        ).fetchone()
        return Window(low=row[0] if row else None, high=end_of_day(batch_date))

    def record_run(self, source: str, table: str, batch_date: date, window: Window, rows: int) -> None:
        """Called only after the Bronze write succeeded; run row and watermark commit together."""
        with self.conn.transaction():
            self.conn.execute(sql.SQL(
                "INSERT INTO {} (source, table_name, batch_date, low_wm, high_wm, row_count) "
                "VALUES (%s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (source, table_name, batch_date) DO UPDATE SET "
                "low_wm = excluded.low_wm, high_wm = excluded.high_wm, "
                "row_count = excluded.row_count, finished_at = now()"
            ).format(self._t("ingest_runs")), (source, table, batch_date, window.low, window.high, rows))
            self.conn.execute(sql.SQL(
                "INSERT INTO {t} (source, table_name, watermark) VALUES (%s, %s, %s) "
                "ON CONFLICT (source, table_name) DO UPDATE SET "
                "watermark = GREATEST({t}.watermark, excluded.watermark), updated_at = now()"
            ).format(t=self._t("watermarks")), (source, table, window.high))

    def watermark(self, source: str, table: str) -> datetime | None:
        row = self.conn.execute(
            sql.SQL("SELECT watermark FROM {} WHERE source = %s AND table_name = %s").format(
                self._t("watermarks")), (source, table)).fetchone()
        return row[0] if row else None

    def last_snapshot_hash(self, source: str, table: str, batch_date: date) -> str | None:
        """Hash of the latest snapshot written on or before batch_date."""
        row = self.conn.execute(
            sql.SQL("SELECT content_hash FROM {} WHERE source = %s AND table_name = %s "
                    "AND batch_date <= %s ORDER BY batch_date DESC LIMIT 1").format(
                self._t("reference_snapshots")),
            (source, table, batch_date),
        ).fetchone()
        return row[0] if row else None

    def record_snapshot(self, source: str, table: str, batch_date: date, content_hash: str, rows: int) -> None:
        self.conn.execute(sql.SQL(
            "INSERT INTO {} (source, table_name, batch_date, content_hash, row_count) "
            "VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (source, table_name, batch_date) DO UPDATE SET "
            "content_hash = excluded.content_hash, row_count = excluded.row_count, written_at = now()"
        ).format(self._t("reference_snapshots")), (source, table, batch_date, content_hash, rows))


@dataclass(frozen=True)
class IngestRange:
    """Bronze partitions (ingest_date) a layer batch processes: (low, high]. low None = all."""
    low: date | None
    high: date

    def __str__(self) -> str:
        return f"({self.low or '-inf'}, {self.high}]"


@dataclass
class LayerRun:
    rows_in: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    rows_duplicate: int = 0
    rows_written: int = 0
    rows_pending: int = 0
    detail: str = ""


def layer_range(book: Bookkeeping, layer: str, table: str, batch_date: date,
                full_refresh: bool = False) -> IngestRange:
    if full_refresh:
        return IngestRange(None, batch_date)
    row = book.conn.execute(
        sql.SQL("SELECT to_ingest_date FROM {} WHERE layer = %s AND table_name = %s AND batch_date < %s "
                "ORDER BY batch_date DESC LIMIT 1").format(book._t("layer_runs")),
        (layer, table, batch_date),
    ).fetchone()
    return IngestRange(row[0] if row else None, batch_date)


def record_layer_run(book: Bookkeeping, layer: str, table: str, batch_date: date,
                     rng: IngestRange, run: LayerRun) -> None:
    book.conn.execute(sql.SQL(
        "INSERT INTO {} (layer, table_name, batch_date, from_ingest_date, to_ingest_date, rows_in, "
        "rows_valid, rows_quarantined, rows_duplicate, rows_written, rows_pending, detail) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (layer, table_name, batch_date) DO UPDATE SET "
        "from_ingest_date = excluded.from_ingest_date, to_ingest_date = excluded.to_ingest_date, "
        "rows_in = excluded.rows_in, rows_valid = excluded.rows_valid, "
        "rows_quarantined = excluded.rows_quarantined, rows_duplicate = excluded.rows_duplicate, "
        "rows_written = excluded.rows_written, rows_pending = excluded.rows_pending, "
        "detail = excluded.detail, finished_at = now()"
    ).format(book._t("layer_runs")), (layer, table, batch_date, rng.low, rng.high, run.rows_in,
                                      run.rows_valid, run.rows_quarantined, run.rows_duplicate,
                                      run.rows_written, run.rows_pending, run.detail))
