"""Run a pipeline task on AWS Glue, with the bookkeeping kept in the local Postgres.

    glue_run.py export TASK --date D           Postgres -> s3://<bucket>/control/TASK/D/state.json
    glue_run.py import TASK --date D [--run-id ID]   output.json -> Postgres (exit 1 on blocking DQ failures)
    glue_run.py run TASK --date D              export, start job olist-spark, wait, import (no Airflow)

TASK: silver, gold, dq_silver, dq_gold. Airflow runs export -> GlueJobOperator -> import instead of `run`.
Every finished run is appended to data/glue_runs.jsonl with its DPU-seconds and cost.
"""

import argparse
import json
import sys
import time
from datetime import UTC, date, datetime

from olist_pipeline.aws import account_id, bucket_name, session
from olist_pipeline.config import PROJECT_ROOT, load_config
from olist_pipeline.control import export_state, import_outputs
from olist_pipeline.db import connect
from olist_pipeline.watermarks import Bookkeeping

JOB = "olist-spark"
TASKS = ("silver", "gold", "dq_silver", "dq_gold")
FLEX_DPU_HOUR = 0.29  # ap-southeast-2, Glue 5.0 Flex (AWS Pricing API, 2026-10-09)
LEDGER = PROJECT_ROOT / "data" / "glue_runs.jsonl"


class Ctx:
    def __init__(self):
        self.cfg = load_config()
        self.region = self.cfg["aws"]["region"]
        self.account = account_id(self.region)
        self.bucket = bucket_name(self.cfg["aws"], self.account)
        self.s3 = session(self.region).client("s3")
        self.glue = session(self.region).client("glue")

    def key(self, task: str, day: date, name: str) -> str:
        return f"control/{task}/{day.isoformat()}/{name}"

    def masked(self, text: str) -> str:
        return text.replace(self.account, "<account>")


def book(ctx: Ctx, conn) -> Bookkeeping:
    return Bookkeeping(conn, ctx.cfg["pg"]["pipeline_schema"])


def cmd_export(ctx: Ctx, task: str, day: date) -> None:
    with connect(ctx.cfg["pg"]) as conn:
        state = export_state(book(ctx, conn), day)
    ctx.s3.put_object(Bucket=ctx.bucket, Key=ctx.key(task, day, "state.json"), Body=json.dumps(state).encode())
    ctx.s3.delete_object(Bucket=ctx.bucket, Key=ctx.key(task, day, "output.json"))  # no stale output on a rerun
    n = sum(len(t) for t in state["layer_runs"].values())
    print(
        f"exported state for {task} {day}: {n} layer/table runs, "
        f"{sum(len(c) for t in state['dq_observed'].values() for c in t.values())} DQ observations"
    )


def run_cost(ctx: Ctx, run_id: str) -> dict:
    run = ctx.glue.get_job_run(JobName=JOB, RunId=run_id)["JobRun"]
    dpu = run.get("DPUSeconds") or max(run.get("ExecutionTime", 0), 60) * 2
    return {
        "run_id": run_id,
        "state": run["JobRunState"],
        "execution_s": run.get("ExecutionTime", 0),
        "dpu_seconds": dpu,
        "usd": round(dpu / 3600 * FLEX_DPU_HOUR, 4),
    }


def cmd_import(ctx: Ctx, task: str, day: date, run_id: str | None) -> int:
    try:
        body = ctx.s3.get_object(Bucket=ctx.bucket, Key=ctx.key(task, day, "output.json"))["Body"].read()
    except ctx.s3.exceptions.NoSuchKey:
        print(f"no output.json for {task} {day}: the Glue run did not finish")
        return 1
    outputs = json.loads(body)
    with connect(ctx.cfg["pg"]) as conn:
        n_runs, n_dq = import_outputs(book(ctx, conn), outputs)
    cost = run_cost(ctx, run_id) if run_id else {}
    entry = {
        "task": task,
        "batch_date": day.isoformat(),
        "summary": outputs["summary"],
        "job_seconds": outputs["seconds"],
        "spark_version": outputs["spark_version"],
        **cost,
        "logged_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with open(LEDGER, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")
    print(
        f"imported {task} {day}: {n_runs} layer runs, {n_dq} DQ results; summary {outputs['summary']}; "
        f"Spark {outputs['spark_version']}, {outputs['seconds']} s in the job"
        + (f"; {cost['dpu_seconds']:.0f} DPU-s = ${cost['usd']:.4f}" if cost else "")
    )
    if task.startswith("dq_") and outputs["summary"].get("blocking"):
        print(f"DQ {task[3:]} {day}: {outputs['summary']['blocking']} blocking failures")
        return 1
    return 0


def start(ctx: Ctx, task: str, day: date, attempts: int = 20) -> str:
    """Start the job; MaxConcurrentRuns=1 can briefly still count a run that just SUCCEEDED, so retry."""
    for attempt in range(attempts):
        try:
            return ctx.glue.start_job_run(JobName=JOB, Arguments={"--TASK": task, "--DATE": day.isoformat()})[
                "JobRunId"
            ]
        except ctx.glue.exceptions.ConcurrentRunsExceededException:
            if attempt == attempts - 1:
                raise
            print("  previous run still finishing; retrying in 15 s")
            time.sleep(15)


def cmd_run(ctx: Ctx, task: str, day: date) -> int:
    cmd_export(ctx, task, day)
    run_id = start(ctx, task, day)
    t0, last = time.time(), None
    while True:
        run = ctx.glue.get_job_run(JobName=JOB, RunId=run_id)["JobRun"]
        if run["JobRunState"] != last:
            print(f"  {time.time() - t0:5.0f}s  {run['JobRunState']}")
            last = run["JobRunState"]
        if run["JobRunState"] in ("SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED", "ERROR"):
            break
        time.sleep(15)
    if run["JobRunState"] != "SUCCEEDED":
        print("error: " + ctx.masked(run.get("ErrorMessage", "")[:800]))
        print(json.dumps(run_cost(ctx, run_id)))
        return 1
    return cmd_import(ctx, task, day, run_id)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=("export", "import", "run"))
    p.add_argument("task", choices=TASKS)
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.add_argument("--run-id")
    a = p.parse_args()
    ctx = Ctx()
    if a.command == "export":
        cmd_export(ctx, a.task, a.date)
        rc = 0
    elif a.command == "import":
        rc = cmd_import(ctx, a.task, a.date, a.run_id)
    else:
        rc = cmd_run(ctx, a.task, a.date)
    sys.exit(rc)


if __name__ == "__main__":
    main()
