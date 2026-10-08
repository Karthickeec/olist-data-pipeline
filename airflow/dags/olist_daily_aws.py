"""olist_daily_aws: the business day with Silver, Gold and their DQ on AWS Glue (ap-southeast-2).

    api_up, replay -> bronze_postgres, bronze_files, bronze_api -> dq_bronze -> publish_bronze
      -> silver -> dq_silver -> gold -> dq_gold -> pull_lake
    each Glue step: export_<task> (Postgres -> S3 state) -> glue_<task> (GlueJobOperator) -> import_<task>

- Sources and Bronze stay local (Postgres isn't reachable from AWS; no RDS on purpose) and are published
  to S3 with an incremental sync. Spark for Silver/Gold runs next to the data on Glue: from this laptop
  every S3 round trip to Sydney costs ~0.4 s (README, step 8).
- Glue can't reach the local Postgres either, so the bookkeeping travels as JSON (olist_pipeline.control):
  the control plane stays local, the data plane runs on AWS.
- One Glue job (olist-spark, Glue 5.0 Flex, 2 x G.1X, MaxConcurrentRuns 1) takes --TASK/--DATE.
- pull_lake copies Glue's output back so the local lake keeps matching S3 (verification, teardown).
- Same conventions as olist_daily: catchup with end_date, max_active_runs=1, created paused.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, chain

from olist_common import API_WAIT_SECONDS, RETRY_DELAY_SECONDS, SPARK_POOL, api_is_up, job, on_failure, on_retry

REGION = "ap-southeast-2"
GLUE_JOB = "olist-spark"
START = datetime.fromisoformat(os.environ.get("OLIST_AWS_DAG_START", "2018-01-11"))
END = datetime.fromisoformat(os.environ.get("OLIST_AWS_DAG_END", "2018-01-12"))


with DAG(
    dag_id="olist_daily_aws",
    description="Local sources and Bronze, then Silver/Gold/DQ as AWS Glue jobs on the S3 lake",
    schedule="@daily",
    start_date=START,
    end_date=END,
    catchup=True,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-eng",
        "retries": 2,
        "retry_delay": timedelta(seconds=RETRY_DELAY_SECONDS),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "on_failure_callback": on_failure,
        "on_retry_callback": on_retry,
    },
    tags=["olist", "portfolio", "aws"],
) as dag:
    env = {"OLIST_API_KEY": os.environ.get("OLIST_API_KEY", "dev-local-key")}

    def local(task_id: str, command: str, spark: bool = False) -> BashOperator:
        kw = {"pool": SPARK_POOL} if spark else {}
        return BashOperator(task_id=task_id, bash_command=command, env=env, append_env=True, **kw)

    api_up = PythonSensor(
        task_id="api_up", python_callable=api_is_up, mode="reschedule", poke_interval=30, timeout=API_WAIT_SECONDS
    )
    replay = local("replay", job("replay.py", "--date {{ ds }}"))
    bronze = [
        local("bronze_postgres", job("postgres_to_bronze.py", "--date {{ ds }}"), spark=True),
        local("bronze_files", job("files_to_bronze.py", "--date {{ ds }}"), spark=True),
        local("bronze_api", job("api_to_bronze.py", "--date {{ ds }}"), spark=True),
    ]
    dq_bronze = local("dq_bronze", job("dq.py", "--layer bronze --date {{ ds }}"), spark=True)
    publish = local("publish_bronze", job("aws.py", "publish"))

    def on_glue(task: str) -> list:
        export = local(f"export_{task}", job("glue_run.py", f"export {task} --date {{{{ ds }}}}"))
        run = GlueJobOperator(
            task_id=f"glue_{task}",
            job_name=GLUE_JOB,
            region_name=REGION,
            update_config=False,
            # The job derives its control prefix from its --LAKE default: no bucket name in Airflow.
            script_args={"--TASK": task, "--DATE": "{{ ds }}"},
            wait_for_completion=True,
            job_poll_interval=15,
            verbose=False,
            # A run that just SUCCEEDED briefly still counts against MaxConcurrentRuns=1, so the next
            # StartJobRun failed with ConcurrentRunsExceeded: wait 30 s before returning; retries stay as a net.
            sleep_before_return=30,
            retries=3,
            retry_delay=timedelta(seconds=30),
            retry_exponential_backoff=False,
        )
        imp = local(
            f"import_{task}",
            job(
                "glue_run.py",
                f"import {task} --date {{{{ ds }}}}",
                f"--run-id {{{{ ti.xcom_pull(task_ids='glue_{task}') }}}}",
            ),
        )
        return [export, run, imp]

    steps = [on_glue(t) for t in ("silver", "dq_silver", "gold", "dq_gold")]
    pull = local("pull_lake", job("aws.py", "pull"))

    [api_up, replay] >> bronze[0]
    [api_up, replay] >> bronze[1]
    [api_up, replay] >> bronze[2]
    bronze >> dq_bronze >> publish
    chain(publish, *[t for s in steps for t in s], pull)
