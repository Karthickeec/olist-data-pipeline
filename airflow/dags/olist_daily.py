"""olist_daily: one run per business day, the whole pipeline from source replay to Gold.

    api_up (sensor) ─┐
    replay ──────────┴─> [bronze_postgres, bronze_files, bronze_api] -> dq_bronze
                          -> silver -> dq_silver -> gold -> dq_gold

Airflow only orchestrates: every task runs the project's own venv (.venv), so Airflow's
dependencies never mix with PySpark/FastAPI. {{ ds }} is the business day processed.

- catchup=True with end_date: unpausing replays every day from start_date to end_date, in order.
- max_active_runs=1: a day needs the previous day's source state and watermarks.
- Pool "spark" (1 slot): Spark tasks never run two 2 GB JVMs at once on an 8 GB laptop.
- Retries with exponential backoff; the API task's own HTTP retries come first.
- api_up waits (reschedule mode, no worker slot held) instead of failing when the API is down.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

from olist_common import (API_WAIT_SECONDS, RETRY_DELAY_SECONDS, SPARK_POOL, api_is_up, job, on_failure,
                          on_retry)

# The simulated history ends on 2018-10-17. Unpausing the DAG catches up to END_DATE one day at a
# time; set OLIST_DAG_END_DATE to stop earlier (e.g. a 5-day demo).
END_DATE = datetime.fromisoformat(os.environ.get("OLIST_DAG_END_DATE", "2018-10-17"))


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=RETRY_DELAY_SECONDS),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
}

with DAG(
    dag_id="olist_daily",
    description="Replay a day into the sources, then Bronze -> Silver -> Gold with DQ after each layer",
    schedule="@daily",
    start_date=datetime(2018, 1, 1),
    end_date=END_DATE,
    catchup=True,
    max_active_runs=1,
    # Created paused. Note: Airflow 3 schedules no task of a paused DAG, backfill runs included, so the
    # DAG must be unpaused to run; end_date bounds the catch-up that unpausing starts.
    is_paused_upon_creation=True,
    default_args=default_args,
    tags=["olist", "portfolio"],
) as dag:
    env = {"OLIST_API_KEY": os.environ.get("OLIST_API_KEY", "dev-local-key")}

    api_up = PythonSensor(task_id="api_up", python_callable=api_is_up, mode="reschedule",
                          poke_interval=30, timeout=API_WAIT_SECONDS)
    replay = BashOperator(task_id="replay", bash_command=job("replay.py", "--date {{ ds }}"))

    def spark_task(task_id: str, command: str) -> BashOperator:
        return BashOperator(task_id=task_id, bash_command=command, pool=SPARK_POOL, env=env, append_env=True)

    bronze = [
        spark_task("bronze_postgres", job("postgres_to_bronze.py", "--date {{ ds }}")),
        spark_task("bronze_files", job("files_to_bronze.py", "--date {{ ds }}")),
        spark_task("bronze_api", job("api_to_bronze.py", "--date {{ ds }}")),
    ]
    dq_bronze = spark_task("dq_bronze", job("dq.py", "--layer bronze --date {{ ds }}"))
    silver = spark_task("silver", job("silver.py", "--date {{ ds }}"))
    dq_silver = spark_task("dq_silver", job("dq.py", "--layer silver --date {{ ds }}"))
    gold = spark_task("gold", job("gold.py", "--date {{ ds }}"))
    dq_gold = spark_task("dq_gold", job("dq.py", "--layer gold --date {{ ds }}"))

    [api_up, replay] >> bronze[0]
    [api_up, replay] >> bronze[1]
    [api_up, replay] >> bronze[2]
    bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold
