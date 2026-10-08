"""Shared helpers for the olist DAGs (no DAG is defined here, so importing it registers nothing)."""

import json
import logging
import os
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PY = REPO / ".venv" / "bin" / "python"
API_HEALTH = os.environ.get("OLIST_API_HEALTH_URL", "http://127.0.0.1:8000/healthz")
# Overridable for demos/tests; production-like defaults otherwise.
API_WAIT_SECONDS = int(os.environ.get("OLIST_API_WAIT_SECONDS", "600"))
RETRY_DELAY_SECONDS = int(os.environ.get("OLIST_RETRY_DELAY_SECONDS", "60"))
SPARK_POOL = "spark"

log = logging.getLogger("olist")


def _summary(context: dict, what: str) -> str:
    ti = context["ti"]
    exc = context.get("exception")
    return "\n".join(
        [
            "=" * 72,
            f"OLIST PIPELINE {what}",
            f"  dag:        {ti.dag_id}",
            f"  task:       {ti.task_id}",
            f"  run date:   {context.get('ds')}   (run_id {context['dag_run'].run_id})",
            f"  try:        {ti.try_number} of {ti.max_tries + 1}",
            f"  error:      {type(exc).__name__ + ': ' + str(exc).splitlines()[0] if exc else 'n/a'}",
            f"  log:        airflow/logs/dag_id={ti.dag_id}/run_id={context['dag_run'].run_id}/task_id={ti.task_id}/",
            "=" * 72,
        ]
    )


def on_failure(context: dict) -> None:
    log.error(_summary(context, "TASK FAILED (no retries left)"))


def on_retry(context: dict) -> None:
    log.warning(_summary(context, "TASK WILL BE RETRIED"))


def api_is_up() -> bool:
    try:
        with urllib.request.urlopen(API_HEALTH, timeout=5) as resp:
            body = json.load(resp)
            log.info("customer-activity API is up: %s", body)
            return resp.status == 200
    except OSError as e:
        log.warning("customer-activity API not reachable at %s (%s); will check again", API_HEALTH, e)
        return False


def job(script: str, *args: str) -> str:
    return f"cd {REPO} && {PY} scripts/{script} " + " ".join(args)
