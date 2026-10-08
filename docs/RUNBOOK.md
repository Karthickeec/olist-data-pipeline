# Runbook

Commands assume the repo root, `make venv java up seed` done once, and `export OLIST_API_KEY=dev-local-key`
(the Makefile sets that default). Dates are business days (`YYYY-MM-DD`).

## Daily run

| What | Command |
|---|---|
| One day locally, no Airflow | `make api` (second terminal), then `make daily DATE=D` |
| Days locally with a DQ proof and S3 publish | `make run-days START=D1 END=D2` |
| Days with Airflow, local Spark | `OLIST_DAG_END_DATE=D2 make airflow`, unpause `olist_daily` |
| Days with Airflow, Silver/Gold on Glue | `OLIST_AWS_DAG_START=D1 OLIST_AWS_DAG_END=D2 make airflow`, unpause `olist_daily_aws` |
| One Glue task by hand | `make glue-run TASK=silver DATE=D` (`silver`, `dq_silver`, `gold`, `dq_gold`) |

Unpausing a DAG starts its catch-up immediately (Airflow 3 runs no task of a paused DAG, backfills included),
so always bound it with the end-date variable. Only one of the two DAGs should be unpaused at a time.

## Rerun a day

Every job is idempotent for its batch date; rerun in layer order:

```bash
make replay DATE=D            # source state for D (upserts guarded by updated_at)
make bronze DATE=D            # reuses D's recorded watermark window (ingest_runs)
make bronze-api DATE=D
make silver DATE=D            # reads the same Bronze range and the pending rows of the batch before D
make gold DATE=D
make dq LAYER=silver DATE=D   # and bronze / gold
```

- Check that a rerun changed nothing: `make verify-silver-idempotency DATE=D`, `make verify-gold-idempotency DATE=D`.
- Rebuild a whole layer: `make silver-full-refresh DATE=<last processed day>`, then `make gold-full-refresh DATE=…`.
- In Airflow: `make backfill START=D END=D REPROCESS=completed` (the DAG must be unpaused and bounded).
- Known limitation: rerunning an *older* day for a mutable table (orders) after later days re-extracts the
  rows' current state, so rows updated again later are missing from the old day's Bronze partition and live
  on in the later one. Nothing is lost (the latest version per key still equals Postgres; `make verify-bronze`
  checks it). See the README, "Known limitation: reruns of older days for mutable tables".

## Failures and recovery

| Symptom | Cause | Recovery |
|---|---|---|
| `api_up` sensor waits, then fails after `OLIST_API_WAIT_SECONDS` (`AirflowSensorTimeout`) | mock API not running | `make api`, then clear `api_up` and its downstream tasks. A timed-out sensor is not retried. |
| `bronze_api` logs `retrying in …s` lines | injected 429/500 (3% / 2% of requests) | nothing; the client retries with backoff and prints a summary line. It fails only after 5 attempts on one page. |
| `replay` or a Bronze task: connection refused | Postgres down | `make up`; the task retries (2 retries, exponential backoff) and the callback logs a "WILL BE RETRIED" block. |
| `dq_*` exits 1, "FAILED (n blocking)" | a check with severity `error` failed | read the report (failing rows and a sample) or `SELECT * FROM pipeline.dq_results WHERE batch_date = D AND status = 'fail'`. Fix the source or the rule, rerun the layer, then the DQ task. |
| Rows in `silver/_quarantine/<table>/batch_date=D` | unfixable rows (with `_reason`) | expected for injected dirt; investigate if the count differs from the source's dirty rate |
| Rows in `silver/_pending/<table>` | children whose parent order hasn't arrived | resolve on the next batches; after 7 days they're quarantined |
| A Silver job died mid-run, `silver/_staging/` not empty | interrupted staging swap | rerun the same day: staging is cleared first, and the touched partitions are rewritten from Bronze |
| `glue_*` fails with `ConcurrentRunsExceededException` | previous run still counted for a few seconds | retried automatically (3 × 30 s); `sleep_before_return=30` normally avoids it |
| `import_*` says "no output.json" | the Glue run failed or timed out (30 min) | Glue console → job `olist-spark` → run → logs (CloudWatch `/aws-glue/jobs/error`); fix, then clear `export_*` and downstream |
| `make` exits 2 with "no target given" | `make` with no target or a quoted `make "dq LAYER=…"` | pass the target and variables as separate words: `make dq LAYER=bronze DATE=D` |

All task failures and retries log one block with DAG, task, run date, try, error and log location
(`airflow/logs/dag_id=…/run_id=…/task_id=…/`).

## S3 and Athena

```bash
make aws-check                 # read-only probes; EMR Serverless shows "SCP deny" (expected)
make aws-status                # stack, bucket usage, tagged resources
make aws-publish               # local lake + landing -> S3 (changed files up, replaced files deleted)
make aws-pull                  # S3 lake -> local (after Glue wrote Silver/Gold)
make verify-s3-parity          # keys, sizes and MD5/ETags must match (about 8 s)
make athena-create             # (re)create the 31 tables after a schema change; make athena-ddl regenerates the DDL
make athena-check DATE=D       # Athena answers = Spark answers
```

If parity fails after a local run, `make aws-publish`; after a Glue run, `make aws-pull`. Never run local
Spark directly on `s3a://` paths from the laptop (see DECISIONS.md: about 0.4 s per S3 round trip).

## Costs and guards

- Budget `olist-pipeline-5usd`: email alerts at 50% and 100% of actual and 100% of forecast monthly spend.
- Athena workgroup `olist`: queries stop at 1 GB scanned.
- Glue job: Flex, 2 DPU, timeout 30 min, 0 job-level retries, max 1 concurrent run. Every run's DPU-seconds
  and cost are appended to `data/glue_runs.jsonl` (about $0.13 per day for the four tasks).

## AWS teardown

```bash
make aws-down DRY_RUN=1        # list everything that would be deleted
make aws-down                  # do it
```

In order: pull the S3 lake back into `data/lake` (`sync --delete`, so local = S3), delete the Glue jobs
(`olist-spark`, `olist-glue-test`) and their IAM roles (`olist-glue`, `olist-glue-test`), force-delete the
secret (no recovery window), delete the log groups `/aws-glue/jobs/*` (Glue creates them untagged on first
use), delete every object version and delete marker in the bucket, delete the CloudFormation stack (bucket, Athena workgroup, Glue database and its tables), then list what's still tagged
`project=olist-pipeline` (must be nothing). The $5 budget is free and is kept unless deleted by hand
(`aws budgets delete-budget --account-id … --budget-name olist-pipeline-5usd`).

To start again later: `make aws-up aws-sync glue-deploy athena-create`.
