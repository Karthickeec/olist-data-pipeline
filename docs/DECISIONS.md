# Design decisions

Each entry: the choice, why, and what was considered instead. Numbers come from the runs described in the
README and [PLAN.md](PLAN.md).

## Sources and ingestion

**Query-based CDC on `updated_at`, not log-based CDC (Debezium).**
- Why: the source is a simulated OLTP database replayed one day at a time. A watermark window
  `updated_at > previous high AND <= end of batch date` gives exact, rerunnable batches with plain JDBC, and
  a rerun of day D reuses D's recorded window (`ingest_runs`), so it reads the same rows.
- Alternative: Debezium + Kafka gives deletes and every intermediate version, but needs Kafka Connect, a
  replication slot and always-on infrastructure, which is too much for an 8 GB laptop and adds nothing for a daily batch.
- Cost of the choice: hard deletes are invisible, and only the last version per window is seen.

**Reference tables snapshotted only when their content hash changes.**
- Why: geolocation is 1M rows; snapshotting it daily would have written ~16 GB over the history for no new
  information. The hash is stored in `reference_snapshots`.

**API ingestion: land the raw pages first, then load to Bronze.**
- Why: retries (429 with `Retry-After`, 500 with exponential backoff and jitter) stay in the HTTP layer, the
  landed JSON is the audit trail, and a rerun reloads exactly the same bytes.

## Silver

**Plain Parquet with a staging swap, not Apache Iceberg (yet).**
- Why: no catalog or table-format dependency locally; the merge is "read the touched partitions, union the new
  rows, keep the latest per key, write to `_staging`, swap partition directories". Reruns are idempotent
  and `--full-refresh` rebuilds from Bronze.
- Alternative and upgrade path: **Iceberg on the Glue catalog**: ACID `MERGE INTO` instead of the swap,
  snapshot time travel, hidden partitioning, compaction (which would also fix the small files), and no
  reliance on S3 rename (a copy) for the swap. Athena reads and writes Iceberg natively.

**Late-arriving children go to a `_pending` area instead of quarantine.**
- Why: on 2017-03-02, 41 items and 38 payments arrived one batch before their orders. Quarantining them would
  have silently lost real data. Pending rows are retried by the next batch (for up to 7 days) and resolve
  normally; quarantine is only for rows that can't be fixed.

**As-of lookups with `_first_batch_date`.**
- Why: a rerun of an old day after later days must give the same result. Lookups (for example an order's
  purchase date for its items) only see rows whose first batch is not after the batch being rerun.

**Bad rows are quarantined with a `_reason`, never dropped.**
- Why: row accounting must balance per table and batch: in = valid + quarantined + duplicate + pending.
  Verification checks that the quarantine equals the dirt injected into the sources.

## Gold

**Star schema with hash surrogate keys (xxhash64 of the natural key and version).**
- Why: deterministic, so a rebuild gives the same keys and facts never need re-keying; no sequence to manage
  in a distributed job.

**SCD Type 2 for customers, built from CRM address-change requests.**
- Why: the fact must carry the address valid at purchase time (1,200 customers had more than one version
  by 2017-12-31). The fact joins on `valid_from <= purchase < valid_to` (a broadcast range join).

**RFM with quintiles for recency and monetary value, fixed bands for frequency.**
- Why: 97% of customers have one order (44,639 of 45,933 as of 2018-01-12), so frequency quintiles
  would be meaningless ties.

## Spark

**Skew: AQE first, salting when AQE can't apply.** ([SALTING.md](SALTING.md))
- On a skewed sort-merge join (SP = 39% of rows), the busiest task read 2.51M rows plain, 994k salted, and
  522k across 25 tasks with AQE's skew-join handling. Spark 3.5 already avoids skew for count-distinct and
  top-N windows. AQE only splits skewed partitions when both join sides are shuffled.

**`local[2]` with a 2 GB driver, one Spark JVM at a time.**
- Why: an 8 GB Mac running Postgres, the API and Airflow. Airflow's `spark` pool has one slot.

**Daily partitions, measured against monthly** ([SMALL_FILES.md](SMALL_FILES.md)).
- Files are already one per partition; they are small (4–43 KB) because the partitions are daily.
- A monthly layout of the fact table: 16 files of 322 KB instead of 383 of 21 KB; Athena full scan
  1.40 s → 0.47 s, one-month query 0.46 s → 0.36 s.
- Kept daily for now: switching changes the merge unit (a day rewrites its month), DQ scopes, verification
  and the DDL. Monthly partitions or Iceberg compaction are the fix (see Known limitations in the README).

## AWS

**Region ap-southeast-2 only; EMR not used.**
- The account is on the AWS Free plan inside an organization whose service control policy allows only the
  project's home region and denies EMR and EMR Serverless everywhere. The project doesn't edit the SCP or
  upgrade the plan.

**In step 8, compute stayed local and S3 was a published copy.**
- Measured: about 0.38 s TCP connect and 1.1 s to the first byte from the laptop to S3 in Sydney. Local Spark
  on `s3a://` took 330 s for a Bronze day (10 s locally); Silver was stopped after 51 minutes.
- So each day ran locally and `aws s3 sync --delete` published only the changed files (25–40 s).
- Parity is proven by listing keys, sizes and MD5/ETags (8 s), not by reading every file through Spark
  (stopped after 53 minutes).

**In step 9, Spark moved next to the data on AWS Glue.**
- Glue 5.0 (Spark 3.5.4, same minor version as local), Flex execution, 2 × G.1X: about $0.13 per day for
  Silver, Gold and two DQ suites (12 runs, 4,903 DPU-seconds, $0.395 in total).
- One generic job with `--TASK`/`--DATE` instead of four jobs: one definition to deploy and tear down.
- Alternative: EMR on EC2 (transient cluster) or EMR Serverless: both denied by the SCP; EMR would also add
  about 8–10 minutes of cluster start-up per run.

**Bookkeeping exported and imported as JSON around each Glue run.**
- Why: Postgres stays local (no RDS: cost, a public endpoint or a VPC connection for Glue). The jobs use a
  `FileBook` with the same interface as the Postgres bookkeeping, so the Spark code is unchanged.
- Alternative: RDS (db.t4g.micro) and a Glue connection in a VPC. More moving parts and a running database.

**Airflow stays local.**
- MWAA costs about $0.49/hour at minimum; a local standalone Airflow with SQLite and the LocalExecutor uses
  about 1.1 GB of RAM. `GlueJobOperator` (Amazon provider) triggers and monitors the Glue runs.

**`sleep_before_return=30` on the Glue tasks.**
- A Glue run that has just SUCCEEDED still counts against `MaxConcurrentRuns=1` for a few seconds, so the
  next `StartJobRun` failed with `ConcurrentRunsExceededException` (twice on 2018-01-11, absorbed by retries).
  With the pause, 2018-01-12 needed no retry.

**Secrets Manager, resolved at load time and masked.**
- `secret://olist/pipeline#pg_password` in config; one `GetSecretValue` per process; values are wrapped so
  `repr` shows `'***'`. Keys for AWS itself never appear anywhere: the default credential chain is used.

## Orchestration

**One DAG run per business day, catch-up bounded by `end_date`.**
- Airflow 3 schedules no task of a paused DAG, backfill runs included, so the DAGs are created paused and
  unpausing them *is* the catch-up; `end_date` (`OLIST_DAG_END_DATE`, `OLIST_AWS_DAG_END`) bounds it.

**`make` with no target fails.**
- A quoted `make "dq LAYER=bronze"` is read as a variable assignment with no target, which ran the default
  `help` target with exit 0 inside a loop. The default goal now exits 2, and `run-days` checks that
  `dq_results` got fresh rows for every layer.
