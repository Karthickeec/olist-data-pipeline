"""Bronze ingestion: raw source data to Parquet, with ingestion metadata, no cleaning."""
import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone


@dataclass
class TableResult:
    table: str
    action: str   # "written" or "skipped"
    rows: int
    detail: str = ""

    def __str__(self) -> str:
        return f"{self.table:<36} {self.action:<8} rows={self.rows:>8}  {self.detail}"


def utc_now() -> datetime:
    """Run timestamp for _ingested_at (naive UTC, matching the Spark session time zone)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_batch_date(argv=None, description: str = "") -> date:
    p = argparse.ArgumentParser(description=description)
    p.add_argument("--date", type=date.fromisoformat, required=True, help="batch date (YYYY-MM-DD)")
    return p.parse_args(argv).date
