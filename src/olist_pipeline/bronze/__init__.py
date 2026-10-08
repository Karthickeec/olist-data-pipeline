"""Bronze ingestion: raw source data to Parquet, with ingestion metadata, no cleaning."""

import argparse
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta


@dataclass
class TableResult:
    table: str
    action: str  # "written" or "skipped"
    rows: int
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.table:<36} {self.action:<8} rows={self.rows:>8}  {self.detail}"


def utc_now() -> datetime:
    """Run timestamp for _ingested_at (naive UTC, matching the Spark session time zone)."""
    return datetime.now(UTC).replace(tzinfo=None)


def parse_batch_date(argv=None, description: str = "") -> date:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--date", type=date.fromisoformat, required=True, help="batch date (YYYY-MM-DD)")
    return p.parse_args(argv).date


def parse_batch_dates(argv=None, description: str = "") -> list[date]:
    """--date D, or --start S --end E for every day of a range (catch-up runs)."""
    p = argparse.ArgumentParser(description=description)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=date.fromisoformat, help="batch date (YYYY-MM-DD)")
    g.add_argument("--start", type=date.fromisoformat, help="first day of a range (inclusive)")
    p.add_argument("--end", type=date.fromisoformat, help="last day of a range (inclusive)")
    args = p.parse_args(argv)
    if args.date:
        return [args.date]
    if args.end is None or args.end < args.start:
        p.error("--start needs an --end on or after it")
    return [args.start + timedelta(days=i) for i in range((args.end - args.start).days + 1)]
