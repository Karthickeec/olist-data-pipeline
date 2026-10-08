"""Client for the customer-activity API: paging, retries with backoff, raw landing."""

import logging
import random
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx

log = logging.getLogger(__name__)

ENDPOINT = "/v1/customer-activity"
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


class ApiError(RuntimeError):
    """The API call failed for good: non-retryable status, or retries exhausted."""


@dataclass
class FetchStats:
    pages: int = 0
    records: int = 0
    retries: int = 0
    wait_seconds: float = 0.0


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After as seconds (delta-seconds or an HTTP date); None if absent or unparseable."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        return max(0.0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
    except (TypeError, ValueError):
        return None


class ActivityClient:
    def __init__(
        self,
        http: httpx.Client,
        api_key: str,
        page_size: int = 200,
        max_attempts: int = 5,
        backoff_base: float = 0.5,
        backoff_max: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        jitter: random.Random | None = None,
    ):
        self.http = http
        self.api_key = api_key
        self.page_size = page_size
        self.max_attempts = max_attempts
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.sleep = sleep
        self.jitter = jitter or random.Random()
        self.stats = FetchStats()

    def backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter for the wait after failed attempt `attempt` (1-based)."""
        return min(self.backoff_max, self.backoff_base * 2 ** (attempt - 1)) * self.jitter.uniform(0.5, 1.0)

    def get_page(self, day: date, page: int) -> tuple[bytes, dict]:
        """One page as (raw body, parsed JSON). Retries 429/5xx/transport errors, then raises ApiError."""
        params = {"date": day.isoformat(), "page": page, "page_size": self.page_size}
        for attempt in range(1, self.max_attempts + 1):
            retry_after = None
            try:
                resp = self.http.get(ENDPOINT, params=params, headers={"X-API-Key": self.api_key})
            except httpx.TransportError as e:
                reason = type(e).__name__
            else:
                if resp.status_code == 200:
                    return resp.content, resp.json()
                if resp.status_code not in RETRY_STATUSES:
                    raise ApiError(f"{day} page {page}: HTTP {resp.status_code} {resp.text[:200]}")
                reason = f"HTTP {resp.status_code}"
                retry_after = parse_retry_after(resp.headers.get("Retry-After"))

            if attempt == self.max_attempts:
                raise ApiError(f"{day} page {page}: giving up after {attempt} attempts (last: {reason})")
            wait = retry_after if retry_after is not None else self.backoff(attempt)
            source = "Retry-After" if retry_after is not None else "backoff"
            log.warning(
                "retry %s page %d: %s on attempt %d/%d, waiting %.2fs (%s)",
                day,
                page,
                reason,
                attempt,
                self.max_attempts,
                wait,
                source,
            )
            self.stats.retries += 1
            self.stats.wait_seconds += wait
            self.sleep(wait)
        raise AssertionError("unreachable")

    def fetch_day(self, day: date) -> list[tuple[int, bytes]]:
        """Every page for `day` as (page number, raw body). Checks the pages add up to total_records."""
        body, first = self.get_page(day, 1)
        pages = [(1, body)]
        total, records = first["total_records"], len(first["data"])
        for n in range(2, first["total_pages"] + 1):
            body, parsed = self.get_page(day, n)
            if parsed["total_records"] != total:
                raise ApiError(f"{day}: total_records changed between pages ({total} -> {parsed['total_records']})")
            pages.append((n, body))
            records += len(parsed["data"])
        if records != total:
            raise ApiError(f"{day}: got {records} records across {len(pages)} pages, expected {total}")
        self.stats.pages += len(pages)
        self.stats.records += records
        return pages


def landing_dir_for(landing_root: Path, day: date) -> Path:
    return Path(landing_root) / "customer_activity" / f"dt={day.isoformat()}"


def land_pages(landing_root: Path, day: date, pages: list[tuple[int, bytes]]) -> Path:
    """Write raw pages to dt=D/page_NNNN.json, replacing the folder only once every page is on disk."""
    final = landing_dir_for(landing_root, day)
    tmp = final.with_name(f".{final.name}.tmp")
    old = final.with_name(f".{final.name}.old")
    for leftover in (tmp, old):
        shutil.rmtree(leftover, ignore_errors=True)
    tmp.mkdir(parents=True)
    for n, body in pages:
        (tmp / f"page_{n:04d}.json").write_bytes(body)
    if final.exists():
        final.rename(old)
    tmp.rename(final)
    shutil.rmtree(old, ignore_errors=True)
    return final
