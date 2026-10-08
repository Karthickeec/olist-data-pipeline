"""FastAPI app for the mock customer-activity service."""

import math
import random
import secrets
import threading
from dataclasses import dataclass
from datetime import date
from functools import lru_cache

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

from olist_pipeline.api.activity import CustomerIndex, active_customers, activity_record

MAX_PAGE_SIZE = 500


@dataclass(frozen=True)
class ServerSettings:
    rate_limit_rate: float = 0.0  # share of requests answered with 429 + Retry-After
    error_rate: float = 0.0  # share of requests answered with 500
    retry_after_seconds: int = 1
    dirty_rate: float = 0.01  # share of records that are malformed on purpose
    seed: int | None = None  # fixes the failure sequence (tests); None = random


def create_app(index: CustomerIndex, api_key: str, settings: ServerSettings) -> FastAPI:
    if not api_key:
        raise ValueError("an API key is required")
    app = FastAPI(title="Olist customer activity (mock)", version="1")
    failures = random.Random(settings.seed)
    failures_lock = threading.Lock()  # sync endpoints run in a thread pool

    @lru_cache(maxsize=64)
    def records_for(day: date) -> tuple[dict, ...]:
        return tuple(activity_record(day, c, settings.dirty_rate) for c in active_customers(index, day))

    def require_api_key(x_api_key: str | None = Header(default=None)) -> None:
        if x_api_key is None or not secrets.compare_digest(x_api_key, api_key):
            raise HTTPException(401, "missing or invalid API key", headers={"WWW-Authenticate": "ApiKey"})

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok", "customers_indexed": len(index.ids)}

    @app.get("/v1/customer-activity", dependencies=[Depends(require_api_key)])
    def customer_activity(
        day: date = Query(alias="date"),
        page: int = Query(1, ge=1),
        page_size: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    ):
        with failures_lock:
            roll = failures.random()
        if roll < settings.rate_limit_rate:
            return JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(settings.retry_after_seconds)},
            )
        if roll < settings.rate_limit_rate + settings.error_rate:
            return JSONResponse({"detail": "internal server error"}, status_code=500)

        records = records_for(day)
        total_pages = math.ceil(len(records) / page_size)
        start = (page - 1) * page_size
        return JSONResponse(
            {
                "date": day.isoformat(),
                "page": page,
                "page_size": page_size,
                "total_records": len(records),
                "total_pages": total_pages,
                "next_page": page + 1 if page < total_pages else None,
                "data": list(records[start : start + page_size]),
            }
        )

    return app
