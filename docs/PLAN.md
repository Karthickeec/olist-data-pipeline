# Project plan: Olist multi-source e-commerce pipeline

Portfolio project for a data engineering role (Python, PySpark, SQL, Unix, Airflow).
Three simulated source systems feed a Bronze/Silver/Gold lake. The project is built locally
first (8 GB Mac) and moved to AWS afterwards: S3, Secrets Manager, Athena/Glue Catalog in
**ap-southeast-2**, with Spark on AWS Glue ETL jobs if the account allows them (EMR is blocked).

```
 Postgres (OLTP, daily replay) ──JDBC watermark──┐
 CRM JSONL drops (landing/)   ──files──────────┼──> Bronze ──> Silver ──> Gold ──> Athena
 Customer-activity REST API   ──paged, retried─┘      │          │         │
                                                   DQ checks  DQ checks  DQ checks
                     orchestrated by Airflow: one DAG run per business day
```

## Working rules

- **Before each step:** re-read this file.
- **After each step:**
  1. Run the whole test suite (`make test`) and the step's verification.
  2. Update the README.
  3. Tick the step here with its key numbers.
  4. Commit and push.
  5. Only then start the next step.
- **If a step's verification fails** and can't be fixed in a reasonable number of attempts, stop and report.
  Never skip ahead.
- **Steps 4–7** run without waiting for approval. **Stop before Step 8** (AWS costs money and needs the account).
- **Memory budget (8 GB):**
  - Spark runs `local[2]` with a 2 GB driver, one Spark JVM at a time.
  - Docker stays at its current memory limit and runs only Postgres.
- **API:** whenever a step needs it, start `make api` in the background and stop it afterwards.
- **Conventions carried forward:**
  - Lake root comes from config (`s3://` later).
  - Partitions are replaced with dynamic overwrite.
  - Metadata columns start with `_`.
  - Reruns are compared on contents, without wall-clock columns.
  - Every job takes `--date`, and range jobs take `--start/--end`.

## Steps 1–3 (done)

- [x] **Step 1: simulated sources.**
  - Postgres replay of 774 days.
  - Verification: all 9 CSVs reproduced exactly (1,550,921 rows); reruns change nothing.
  - CRM files: 2,984 change requests, 5.3% dirty. Commit `b6bb85b`.
- [x] **Step 2: Bronze from Postgres and files.**
  - Watermark windows on `updated_at`; reference tables snapshotted only when their hash changes (saves ~16 GB of geolocation).
  - Rerun of 2017-03-02: insert-only tables identical; orders 281 → 222, exactly the 59 orders that moved to 03-03.
  - 44 tests. Commit `14c8822`.
- [x] **Step 3: REST API source.**
  - Mock FastAPI service: median 377 records/day, 3% 429s, 2% 500s, 1% dirty records.
  - `api_to_bronze` with retries and backoff; reruns identical in landing bytes and Bronze rows.
  - 72 tests. Commit `ddcffb2`.

---

## Step 4: Silver (Bronze → Silver, PySpark)

- [x] **Done.**
  - **Verified on 2017-02-28..03-06:** Silver = Postgres for all 5 transactional tables (3,413 orders, 3,827 items, 3,615 payments, 2,252 reviews); 77 table-batches balance; geolocation 19,015 prefixes; `order_lines` 3,827 lines with allocations exact for 3,316 orders; quarantine equals the injected dirt (3 negative-session API records); reruns of 03-02 and 03-04 unchanged.
  - **Tests:** 85.
  - **Added beyond the plan:** a `_pending` area for late-arriving children (41 items and 38 payments from 03-02 resolved on 03-03); `_first_batch_date` with as-of lookups so reruns after later batches stay identical; reviews partitioned by `review_date`.

**Goal:** typed, cleaned, deduplicated tables; bad rows quarantined, never dropped; one joined
`order_lines` table; incremental and idempotent.

**Tables** (`<lake>/silver/<table>/`)

| Table | Key | Partitioned by | Notes |
|---|---|---|---|
| orders | order_id | `order_purchase_date` | latest version per key |
| order_items | (order_id, order_item_id) | `order_purchase_date` (from the order) | same partitions as orders, so they join partition by partition |
| order_payments | (order_id, payment_sequential) | `order_purchase_date` | |
| order_reviews | (review_id, order_id) | `review_date` (date of `review_creation_date`) | |
| customers | customer_id | none (≈99k rows) | keeps `customer_unique_id` |
| products | product_id | none | English category; manual names for `pc_gamer` and `portateis_cozinha_e_preparadores_de_alimentos`; blank → `unknown` |
| sellers | seller_id | none | |
| geolocation | zip prefix | none | one row per prefix (19,015): exact median lat/lng and the most common city/state |
| customer_changes | change_id | `requested_date` | cleaned CRM requests |
| customer_activity | (customer_unique_id, activity_date) | `activity_date` | cleaned API records |
| order_lines | (order_id, order_item_id) | `order_purchase_date` | see below |

**Latest row per key:** `row_number()` over the key, ordered by `updated_at desc, _ingested_at desc`.
Files and API rows have no `updated_at`, so they use `_ingested_at desc, _source_file`.
The tie-breaks are deterministic, so reruns pick the same row.

**Cleaning and quarantine.** Rows that can't be fixed go to `<lake>/silver/_quarantine/<table>/batch_date=D/`
with every original column plus `_reason`. They're never silently dropped.

- **customer_changes:**
  - Trim and uppercase `new_state`; map full state names (`São Paulo` → `SP`).
  - Exact duplicates collapse on `change_id`.
  - A null city is filled from Silver geolocation by zip prefix.
  - Quarantine: `unknown_customer` (no matching `customer_unique_id`), `invalid_state` (not one of the 27 Brazilian state codes after cleaning), `unfixable_city`, `corrupt_record`.
- **customer_activity:**
  - Parse `last_seen_at` from both ISO `…Z` and `dd/MM/yyyy HH:mm:ss`; cast the counts to int.
  - Quarantine: `negative_sessions`, `missing_required_field` (id, date, sessions), `unparseable_timestamp`, `unknown_customer`, `corrupt_record`.
  - Missing optional fields (`page_views`, `device`) stay null and are counted.
- **Postgres tables:** already typed. Quarantine catches orphans, e.g. an item whose order isn't in Silver.

**`order_lines`.** One row per order item:
- **Joined columns:** order and customer attributes, product (English category), seller, and payments aggregated per order (total, number of payments, main payment type, max installments).
- **`allocated_payment`:** the order's payments split across its lines by `price + freight`, so summing lines never double-counts payments.
- **Join strategy**, with each choice explained in code comments:
  - products, sellers, geolocation and customers are **broadcast**: they're small (<10 MB), so broadcasting avoids shuffling the large side;
  - items ⋈ orders and ⋈ payment aggregates use a **shuffle (sort-merge) join**: both sides are big, and they're co-partitioned by purchase date.
- **Orders without items** (775 of them) produce no lines and are counted in the job log.

**Incremental and idempotent.**
- `pipeline.layer_runs` records, per Silver table, the newest Bronze `ingest_date` it has processed.
- Batch D processes the Bronze partitions in `(last processed, D]`. That covers normal days, catch-up runs and reruns (a rerun reuses its own range, like Bronze windows).
- **Merge:** read the existing Silver partitions touched by the new rows, union them with the new rows, keep the latest per key, write to `<lake>/silver/_staging/…`, then swap each partition into place.
- Unpartitioned tables are swapped whole.
- `--full-refresh` rebuilds a table from all of Bronze. Bronze is the source of truth.

**Verification.**
1. Reset, then run daily cycles 2017-03-01..03-06 through Silver.
2. Rerun Silver for 03-04: contents identical, apart from processing-time columns.
3. Silver orders, items, payments, reviews and customers equal Postgres (up to the watermark) row for row after type mapping. Every Bronze row is accounted for: Silver plus quarantine plus removed duplicates equals Bronze.
4. Geolocation has 19,015 prefixes.
5. Quarantine counts match the dirty records injected into the CRM and API data.

**Tests:**
- Unit tests on small DataFrames: dedup tie-breaks, state normalisation, both timestamp formats, every quarantine reason, payment allocation summing to the order total, and a broadcast join visible in the plan.
- Integration test: 3 days, then a rerun, checked to be identical.

## Step 5: Data quality framework

- [x] **Done.**
  - **Real data, 2017-02-28..03-06:** 32 Bronze and 47 Silver checks per day; 0 blocking failures. Genuine warnings: 7 geolocation prefixes outside Brazil, missing optional API fields, a row-count drop after the initial load.
  - **Injected bad batch:** 5 `fail` rows with samples, exit 1. Warn-only failures exit 0. Reruns replace their results.
  - **Tests:** 99.
  - **Added beyond the plan:** an `expression` check type, a `latest` scope for snapshot tables, `optional` tables, and `--start/--end` and `--suite` options.

**Goal:** declarative checks, run after every layer; `error` fails the run, `warn` logs.

- **Config:** `config/dq/{bronze,silver,gold}.yaml`, one block per table. Example:
  ```yaml
  silver.orders:
    scope: batch            # rows written by this batch (_batch_date = D), or "table"
    checks:
      - {type: not_null, columns: [order_id, customer_id, order_status], severity: error}
      - {type: unique, columns: [order_id], scope: table, severity: error}
      - {type: accepted_values, column: order_status, values: [created, approved, invoiced, processing, shipped, delivered, canceled, unavailable], severity: error}
      - {type: range, column: price, min: 0, max: 10000, severity: warn}
      - {type: row_count_vs_previous, min_ratio: 0.2, max_ratio: 5.0, severity: warn}
      - {type: schema, columns: {order_id: string, order_purchase_timestamp: timestamp, ...}, severity: error}
      - {type: relationship, column: customer_id, ref: silver.customers.customer_id, severity: error}
  ```
- **Check types:** `not_null`, `unique` (any column set), `accepted_values`, `range`, `row_count_vs_previous`
  (this batch's count vs the previous batch's, with ratio bounds), `schema` (expected columns and Spark types), and
  `relationship` (referential integrity: a left anti-join counts orphans, e.g. every order_line has a customer).
- **Engine:** `olist_pipeline/dq/`. Each check returns
  `(status pass|fail|warn|error_running, failed_rows, sample)`, with up to 5 failing rows as JSON.
- **Results:** `pipeline.dq_results (batch_date, layer, table_name, check_name, severity, status, failed_rows, sample jsonb, checked_at)`.
  Upserted by (batch_date, layer, table, check), so reruns replace their own results.
- **CLI:** `make dq LAYER=silver DATE=…`. It exits 1 if any error-severity check failed, and prints a
  per-check table plus a summary.
- **Coverage:** Bronze (schema, not_null keys, row counts vs previous), Silver (all types, including relationships between Silver tables), Gold (added in Step 6).

**Verification:**
- dq passes for Bronze, Silver and (in Step 6) Gold on real days.
- An **injected bad batch** (duplicate keys, null ids, an orphan order line, a state outside the accepted list) produces `fail` rows with the right counts and samples, and the run exits 1.
- A warn-only failure exits 0.

**Tests:** each check type passes and fails on small DataFrames; YAML validation (unknown check type or
missing column → clear error); the exit-code behaviour; the dq_results upsert on rerun.

## Step 6: Gold (star schema) + salting demo

- [x] **Done.**
  - **Catch-up to 2017-12-31 in ~5 min:** replay 14 s, Postgres→Bronze 10 s, files 55 s, API 113 s (485 pages, 30 retries), DQ Bronze 13 s, Silver 77 s, DQ Silver 23 s.
  - **Gold rows:** fact 51,234 lines; dim_customer 45,255 versions for 44,034 customers (1,200 with history); customer_metrics 43,316.
  - **Verified:** fact = Silver; aggregate = fact; SCD2 has no gaps or overlaps; every fact row is on the version valid at purchase; LTV equals fact payments (R$7,143,826.57); Gold DQ passes (31 checks); a Gold rerun is identical.
  - **Salting:** the plain join's busiest task read 2.51M rows; salting cut it to 994k, and AQE skew-join to 522k across 25 tasks.
  - **Tests:** 105.
  - **Changed from the plan:**
    - The salting demo uses a skewed sort-merge join instead of median and count-distinct. An exact median can't be salted, count-distinct is already shuffled by (state, customer), and Spark 3.5 pre-limits `row_number() <= N` windows.
    - Dimensions are rebuilt in full each batch instead of only for the customers touched (simpler, deterministic, and cheap at this size).

**6.0: build up history first (catch-up).**
1. Add `--start/--end` to `files_to_bronze` and `api_to_bronze`: one Spark session, one partition per day.
2. Replay 2017-03-07..2017-12-31 into Postgres (≈300 days).
3. Run **one** `postgres_to_bronze` batch for 2017-12-31. Its watermark window covers the whole gap.
4. Run the files and API ingestion for every day of the range.
5. Run Silver + dq for 2017-12-31; its range covers every new Bronze partition.
6. Record how long each part took.

Expected volume: about 45k orders through 2017-12-31.
Trade-off, documented: the catch-up batch keeps only the latest state of each order, not the intermediate daily states.

**Gold tables** (`<lake>/gold/…`):

| Table | Grain | Partitioned by | Notes |
|---|---|---|---|
| fact_order_lines | one order item | `order_purchase_date` | measures: price, freight, allocated_payment. Keys to dims; `customer_sk` is the SCD2 version valid at purchase time |
| dim_customer | one version of a customer_unique_id | none | **SCD Type 2**: `customer_sk, customer_unique_id, zip, city, state, valid_from, valid_to, is_current` |
| dim_product | product_id | none | English category, dimensions, weight |
| dim_seller | seller_id | none | with geolocation lat/lng |
| dim_date | date | none | calendar attributes, 2016–2018 |
| agg_daily_category_sales | (date, category) | `order_purchase_date` | orders, items, revenue, freight, avg item price |
| customer_metrics | (as_of_date, customer_unique_id) | `as_of_date` | LTV, order count, AOV, days since last order, RFM scores and segment |

- **SCD2:**
  - The first version comes from the customer's earliest order address, valid from the first order date.
  - Each clean `customer_changes` row closes the current version (`valid_to = requested_at`) and opens a new one. A request that doesn't change the address is skipped.
  - Incremental: history is rebuilt only for the customers touched in the batch, which makes it deterministic and rerun-safe.
  - Invariants checked in dq: exactly one current version per customer, and no gaps or overlaps between versions.
- **RFM:**
  - Recency and monetary scores use `ntile(5)`.
  - About 97% of customers ordered exactly once, so frequency uses fixed bands (1, 2, 3, 4–5, 6+) rather than ntile.
  - Segments: Champions, Loyal, Potential, At risk, Hibernating, Lost (mapping documented).
- **Salting demo** (`docs/SALTING.md`), aggregating fact_order_lines by `customer_state`, where SP is about 42% of rows:
  - **(a) Sum and count.** Spark combines these per partition before the shuffle, so skew barely matters.
  - **(b) Exact median and count of distinct customers per state.** These can't be combined early, so SP's rows all land on one task.
  - Each is run without and with salting (a random suffix 0..N-1, aggregate, then re-aggregate).
  - Adaptive query execution is off for the comparison, then on.
  - Recorded: wall time (median of 3 runs), task time per stage (max vs median), shuffle partition sizes, and `explain("formatted")` plans.
  - With `local[2]`, salting may not speed things up locally. The write-up reports the real numbers either way and explains where salting pays off on a real cluster.

**Verification:**
- `sum(fact.price)` and the line count equal Silver order_lines, and `agg_daily_category_sales` totals equal the fact table.
- Every fact row joins to exactly one dim_customer version valid at purchase time.
- SCD2 invariants hold; customers with changes have more than one version.
- customer_metrics LTV totals equal the fact payments per customer.
- dq Gold passes.
- A Gold rerun for one date is identical.

## Step 7: Airflow locally

- [x] **Done.**
  - **Setup:** Airflow 3.3.2 standalone in `.venv-airflow` with SQLite + LocalExecutor (~1.1 GB RSS).
  - **Catch-up:** 5 days (2018-01-01..05) all succeeded in order, 2:19–2:31 per run.
  - **Backfill:** reran 2018-01-03 in place (all tasks on try 2) and it succeeded.
  - **Failure demo, 2018-01-06:** `replay` retried and recovered after a Postgres outage; `api_up` timed out after 62.5 s with the failure block logged; after a clear, the run succeeded in 2:17.
  - **Checks:** `verify-bronze`, `verify-silver` and `verify-gold` pass through 2018-01-06; 0 blocking DQ failures over 18 layer-days.
  - **Tests:** 105.
  - **Changed from the plan:**
    - Airflow 3 schedules no task of a paused DAG (backfill runs included), so the DAG has an `end_date` (`OLIST_DAG_END_DATE`) and unpausing performs the catch-up.
    - Sensors don't retry on timeout, so the demo shows the retry on `replay` (a Postgres outage) and the timeout on `api_up` (an API outage).

**Goal:** one daily DAG that runs the whole pipeline with retries, catch-up/backfill and clear failure logs,
light enough for 8 GB.

- **Setup:**
  - Airflow (latest stable 3.x that's at least two weeks old, installed with its constraints file) in its own venv, `.venv-airflow`, so its dependencies never clash with PySpark or FastAPI.
  - Started with `airflow standalone` and `AIRFLOW_HOME=./airflow`.
  - Metadata DB: SQLite with LocalExecutor if this Airflow version supports it. Otherwise an `airflow` database inside the existing Postgres container (no extra containers).
  - `make airflow` / `make airflow-stop`.
- **Tasks** call the project's own venv through BashOperators, e.g. `.venv/bin/python scripts/… --date {{ ds }}`. Airflow only orchestrates.
- **DAG `olist_daily`:**
  ```
  api_up (sensor) ─┐
  replay ──────────┴─> [bronze_postgres, bronze_files, bronze_api] -> dq_bronze -> silver -> dq_silver -> gold -> dq_gold
  ```
  - `schedule="@daily"`, `start_date=2018-01-01` (the day after the catch-up history), `catchup=True`; the DAG is kept paused and driven by backfill.
  - `max_active_runs=1`, because each day depends on the previous day's source state and watermarks.
  - **Memory:** a pool `spark` with 1 slot. The three Bronze tasks are parallel in the graph but never run three JVMs at once. Non-Spark tasks run freely.
  - **Retries:** 2, with `retry_exponential_backoff=True` starting at 1 minute and capped at 10 minutes. The API task's own HTTP retries come first.
  - **`api_up`:** a sensor polling `/healthz` (poke every 30 s, timeout 10 min, reschedule mode), so a down API waits instead of failing the whole run.
  - **Failure callback:** logs DAG, task, run date, try number, the exception and the log location in one clear block. `on_retry_callback` logs the retries too.
- **Verification:**
  - Backfill 2018-01-01..2018-01-05 with the API running in the background.
  - All 5 runs succeed, with every task state shown via `airflow dags list-runs` / `airflow tasks states-for-dag-run`.
  - Then force a failure (stop the API) for one run and show the sensor waiting, the retry and the failure log.
  - `verify-bronze` and the dq results for those dates pass.
  - Wall time per run recorded.

---

## Steps 8–10

### AWS account constraints (probed read-only on 2026-10-09)
- **The account is on the AWS Free plan** ($100 credits until 2027-04-08; it can't be billed beyond them).
  It belongs to an AWS Organization whose service control policies (SCPs) are not edited by this project.
- **Region: `ap-southeast-2` only.** The SCP allows the project's home region and denies the others:
  - S3, Athena, Glue (Data Catalog and ETL jobs), Secrets Manager, CloudFormation, CloudWatch and the
    tagging API all answered read-only calls in ap-southeast-2.
  - All of them were denied in ap-south-1, eu-north-1 and us-west-2.
- **EMR and EMR Serverless are denied** even in ap-southeast-2. We accept that and don't touch the SCPs.
  Spark on AWS moves to **AWS Glue ETL jobs** (Step 9).
- **Rules for every AWS step:**
  - every resource is tagged `project=olist-pipeline`;
  - a cost estimate comes before anything that costs money;
  - each step has its own teardown command, and teardown is run at the end;
  - credentials are never printed, logged or committed.
    Spark and boto3 use the default credential chain (the local AWS profile), never keys in config.

### Step 8: lake on S3, Secrets Manager, Athena (ap-southeast-2)

- [x] **Done.**
  - **Verified:**
    - Stack + secret + $5 budget alert created and tagged.
    - Lake and landing uploaded (4,162 + 990 files, 7.6 min).
    - 2018-01-07..09 run with DQ proven for all three layers (32/47/31 checks per day, 0 blocking) and published.
    - S3 identical to local after 01-09: 4,217 lake + 999 landing files, keys, sizes and MD5/ETags.
    - `verify-bronze/silver/gold` pass (53,401 fact lines; LTV = payments, R$7,437,125.17).
    - 31 Athena tables; Athena = Spark on 7/7 checks (1.9 MB scanned); 4 KPI named queries; partition-registration
      fallback (385 partitions, 47,358 rows = projected table).
    - Glue test job SUCCEEDED (Glue 5.0 / Spark 3.5.4, Flex, 2 DPU, 93 s, 171 DPU-s = $0.014).
    - Tests: 114 pass (incl. 9 new AWS/Athena unit tests).
  - **Deviations from the plan below:**
    - **Compute stays local; S3 is published with an incremental `aws s3 sync --delete`.** Local Spark against
      Sydney S3 was impractical: about 0.38 s TCP connect and 1.1 s to the first byte; Bronze took 330 s instead
      of 10 s; Silver was stopped after 51 min. The aborted run's Bronze/Silver for 01-07 was pulled back before
      rerunning locally.
    - **Parity is a listing comparison** (keys, sizes, MD5/ETags), not a Spark read of every file; that read was
      stopped after 53 min.
    - **Athena vs Spark:** Spark reads the local lake, which the listing check proves byte-identical to S3.
    - **Spark stays `local[2]` with a 2 GB driver,** as agreed (a short-lived `local[8]` for S3 was reverted).
    - **`make` with no goal now fails (exit 2).** A quoted `make "dq LAYER=bronze"` had silently run the help
      target with exit 0 in an ad-hoc loop. `run-days` additionally checks `dq_results` for fresh rows per layer.
    - **Small files:** already one file per partition; the files are small because partitions are daily.
      Measured monthly vs daily on Athena (docs/SMALL_FILES.md: 383 → 16 files, full scan 1.40 → 0.47 s). Daily
      partitions are kept for now: switching changes the merge unit, DQ scopes, verification and DDL. It's listed
      under Known limitations, with Iceberg compaction as the upgrade.
    - **Not needed:** moving `verify.lake_fingerprint` to Hadoop listing (it already reads through Spark).

**Resources** (one CloudFormation stack, `olist-pipeline-step8`, in `infra/step8.yaml`; stack tags propagate):

| Resource | Settings |
|---|---|
| S3 bucket `olist-pipeline-<account>-apse2` | private (Block Public Access on), SSE-S3, versioning on, TLS-only bucket policy |
| Bucket lifecycle | `landing/` → Glacier Instant Retrieval after 90 days; noncurrent versions expire after 7 days; `athena-results/` expire after 7 days; incomplete multipart uploads aborted after 1 day |
| Athena workgroup `olist` | results in `s3://…/athena-results/`, enforced settings, **1 GB scan cutoff per query** (cost guard) |
| Glue database `olist_lake` | holds the external tables (Glue databases and tables can't carry tags; they're deleted with the stack) |
| Secrets Manager secret `olist/pipeline` | JSON `{pg_password, api_key}`, created by script from local values through a 0600 temp file that is deleted right after; never echoed |

The account ID stays out of git: the bucket name is built at runtime from `aws sts get-caller-identity`.

**Code changes:**
1. **Spark on S3:**
   - `hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`, the pair that matches Spark 3.5's Hadoop 3.3.4.
   - S3A settings: endpoint `s3.ap-southeast-2.amazonaws.com`, the default credential-provider chain,
     and no keys in Spark conf.
   - **Committer (deviation from the first plan):** the S3A "magic"/staging committers don't support
     Spark's dynamic partition overwrite, so writes keep the classic `FileOutputCommitter`. Its rename is
     a copy on S3, which is slower but correct at this data size. Iceberg is the real fix (below).
   - The lake root becomes `s3a://<bucket>/lake` when `OLIST_TARGET=aws` is set (`make … AWS=1`); the
     default stays local.
2. **`secret://<id>#<field>` references in config:**
   - Resolved at load time with boto3, one call per secret, cached; values are never logged.
   - `OLIST__PG__PASSWORD=secret://olist/pipeline#pg_password`, and a new `api.key` that, when set, takes
     precedence over `key_env`. Local runs keep today's behaviour.
3. **Lake-agnostic verification:** `verify.lake_fingerprint` uses Python file walking today; it moves to
   Hadoop FileSystem listing so `verify-*` runs against an `s3a://` root.
4. **Athena DDL generated from the data:**
   - `scripts/athena_ddl.py` reads each table's Parquet schema with Spark and writes
     `sql/athena/<layer>/<table>.sql` (CREATE EXTERNAL TABLE … STORED AS PARQUET), so the DDL never
     drifts from the files.
   - About 29 tables: Bronze 11, Silver 11, Gold 7.
   - **Partition projection** (`type=date`, `yyyy-MM-dd`, range 2016-09-01..NOW) on `ingest_date`,
     `order_purchase_date`, `review_date`, `requested_date`, `activity_date`, `as_of_date`. New partitions
     need no registration; days with no data (reference-table snapshots) just return nothing.
   - **Partition-registration fallback:** `scripts/athena.py register-partitions` (`ALTER TABLE … ADD IF
     NOT EXISTS PARTITION`) for a table that has projection turned off; demonstrated on one table.
5. **`scripts/athena.py`:**
   - Runs the DDL and saves the Gold KPI queries as Athena named queries: daily revenue by category,
     RFM segment mix, top sellers, customers who moved state (SCD2).
   - A `check` subcommand compares Athena answers with Spark answers on the same S3 data.
6. **Make targets:** `aws-check` (read-only probes), `aws-up` (deploy the stack and the secret),
   `aws-sync` (upload the lake), `athena-ddl`, `athena-check`, `aws-down` (teardown).

**Table format: plain Parquet (decided).**
- The README (Step 8) and `docs/DECISIONS.md` (Step 10) describe **Apache Iceberg** on the Glue
  catalog as the upgrade path.
- Iceberg would bring ACID `MERGE INTO` instead of the staging swap, snapshot time travel, hidden
  partitioning and Athena-side compaction. It would also make the S3 committer limitation irrelevant.

**Execution:**
1. Deploy the stack and the secret, then check the tags with the tagging API.
2. Upload the local lake (through 2018-01-06) and landing to S3 with `aws s3 sync`: about 5,100 objects,
   about 160 MB.
3. **Parity check:** Spark reads each Silver/Gold table from local and from S3; content fingerprints must
   match.
4. **Make S3 the lake of record** and run the pipeline for **2018-01-07..2018-01-09**
   (`make daily AWS=1`). Postgres, files and the API stay local; Spark writes to S3.
   `verify-bronze`, `verify-silver` and `verify-gold` must pass against S3, with 0 blocking DQ failures.
5. **Athena:**
   - Create the tables and run the KPI queries.
   - `athena-check` must match Spark exactly: fact lines, payments total, segment counts.
   - Report bytes scanned per query.

**Verification:** steps 3–5 above, plus `make test`, plus a test proving that `secret://` values never
appear in logs or config dumps.

**Teardown, `make aws-down STEP=8`** (`scripts/aws_teardown.py`):
1. Pull the lake back: `aws s3 sync s3://…/lake data/lake`, so the local lake matches the Postgres
   bookkeeping again (it will be at 2018-01-09).
2. Delete the secret with `--force-delete-without-recovery`.
3. Empty the bucket, including every object version and delete marker.
4. `aws cloudformation delete-stack`, then wait until it's gone.
5. Show what's left with the tagging API (`project=olist-pipeline` must return nothing).
- `--dry-run` lists everything without deleting.
- **When it runs:** the S3 lake is needed by Step 9, so the teardown runs after Step 9 (or right after
  Step 8 if Step 9 is dropped).

**Cost estimate** (live ap-southeast-2 prices from the AWS Pricing API, 2026-10-09):

| Item | Price | Expected use | Cost |
|---|---|---|---|
| S3 storage | $0.025/GB-month | ~0.3 GB incl. noncurrent versions and Athena results | < $0.01/month |
| S3 PUT/COPY/LIST | $0.0055 per 1,000 | ~5k (upload) + ~20k (3 daily runs; renames are copies) | ~$0.15 |
| S3 GET | $0.00044 per 1,000 | ~100k | ~$0.05 |
| Data transfer out (teardown pull-back) | first 100 GB/month free | ~0.2 GB | $0 |
| Athena | $5/TB scanned (10 MB minimum per query) | ~60 queries, ≤ 2 GB | ~$0.01 |
| Glue Data Catalog | $1 per 100k objects/month (first 1M free); $1 per 1M requests | ~30 tables, a few thousand requests | ~$0 |
| Secrets Manager | $0.40 per secret-month; $0.05 per 10k calls | 1 secret, ~1 month, ~200 calls | ≤ $0.40 |
| CloudFormation, SSE-S3, tagging API | free | | $0 |
| **Total for Step 8, kept for up to one month** | | | **≈ $0.60, worst case < $1 (of the $100 credit)** |

### Step 9: Spark on AWS Glue ETL (EMR is blocked by the Free plan)

- [x] **Done.**
  - **What runs where:**
    - Silver, Gold and their DQ run as one Glue job (`olist-spark`, Glue 5.0 Flex, 2 × G.1X) with the
      project wheel.
    - Sources and Bronze stay local and are published to S3.
    - Bookkeeping travels as JSON: export → Glue `FileBook` → import (`olist_pipeline/control.py`).
    - The Airflow DAG `olist_daily_aws` uses `GlueJobOperator`.
  - **Verified:**
    - 2018-01-10 Glue output identical to a local rerun: 11 Silver tables + pending/quarantine, 7 Gold tables.
    - 2018-01-11/12 via Airflow, both successful (20.3 / 22.6 min).
    - DQ 47 + 31 checks per day, 0 blocking.
    - `verify-bronze/silver/gold` pass through 01-12 (54,307 fact lines; LTV = payments, R$7,565,259.72).
    - S3 = local (4,511 + 1,008 files); Athena = Spark 7/7.
    - Tests: 118.
  - **Cost:** 12 Glue runs, 4,903 DPU-s = $0.395 (about $0.13 per day).
  - **Deviations:**
    - **One generic job instead of one per task.**
    - **Bookkeeping exported and imported as JSON;** this wasn't in the plan, which assumed the job could
      record directly.
    - **`sleep_before_return=30` on the Glue tasks,** after `ConcurrentRunsExceededException` right after a
      finished run.
    - **The parity check accepts one-part multipart ETags** (written by Glue).
    - **`requires-python` lowered to >=3.11** for Glue 5.0 (the code already compiled under 3.11).
- **Read-only check (2026-10-09):** `glue get-jobs`, `list-jobs`, `get-job-runs`, `list-sessions`,
  `get-crawlers` and `get-connections` all succeed in ap-southeast-2. Read access doesn't prove that
  `CreateJob`/`StartJobRun` are allowed, so a single test job comes first.
- **Smallest test job (needs a separate OK before it's created):**
  - Glue 5.0 (Spark 3.5, the same minor version as the local Spark) with the **Flex** execution class.
  - 2 × G.1X workers, which is 2 DPU, the minimum for a Spark job.
  - Timeout 5 min, 0 retries, max concurrency 1.
  - It reads `gold/dim_date` from S3 and writes a one-row count.
  - It needs an IAM role `olist-glue-test` (tagged, with S3 access to the bucket only).
  - **Billing:** per second with a 1-minute minimum. Flex costs $0.29 per DPU-hour; the standard class
    costs $0.44.

  | Run | Formula | Cost |
  |---|---|---|
  | Expected (~2 min, Flex) | 2 DPU × 2/60 h × $0.29 | ≈ $0.02 |
  | Hard cap (5-min timeout, Flex) | 2 DPU × 5/60 h × $0.29 | $0.048 |
  | Hard cap (5-min timeout, standard) | 2 DPU × 5/60 h × $0.44 | $0.073 |

  - Logs to CloudWatch: a few KB, about $0.
  - **Teardown:** delete the job, the role and its policy, and the script's S3 prefix.
- **If Glue jobs are allowed:**
  - Silver, DQ and Gold run as Glue jobs: the package ships as a wheel to S3 (`--additional-python-modules`).
  - Airflow triggers them with `GlueJobOperator`.
  - Postgres → Bronze, the CRM files and the API stay local and write Bronze to S3 (Postgres stays local,
    no RDS).
  - Glue 5.0 runs Python 3.11, so the code must stay 3.11-compatible there (checked at Step 9).
  - The cost is planned at Step 9 per daily run, roughly 2–4 DPU for 3–5 min ≈ $0.03–0.10 per day.
- **If Glue jobs are denied:** Spark stays local and writes to S3, and Athena reads it. That is
  Step 8's setup, documented as the final architecture, with a note on what a Glue/EMR deployment would
  change.
- Airflow stays local (MWAA costs about $0.49/h at minimum).

### Step 10: CI and project documentation

- [x] **Done** (CI workflow committed locally; pushing it needs a GitHub token with the `workflow` scope).
  - **Lint:** ruff (E, F, W, I, B, UP; 120 columns; py311 target), the code base formatted, `make lint`.
  - **CI:** `.github/workflows/ci.yml` runs lint, then pytest with Java 17 and a postgres:16 service.
    Simulated locally against an empty database without the Kaggle CSVs: 86 passed, 32 skipped (CSV-dependent
    fixtures now skip). Locally with the data: 118 passed.
  - **Docs:** docs/ARCHITECTURE.md, docs/DECISIONS.md, docs/RUNBOOK.md; README CI badge, doc links and
    Known limitations; no interview wording anywhere.
  - **AWS teardown run:**
    - The lake was pulled back (4,511 files; `verify-gold` passes locally).
    - Deleted: Glue jobs and roles, the secret, 12,421 object versions, the stack, the Glue log groups.
    - 0 resources tagged `project=olist-pipeline` remain; the free $5 budget is kept.
    - AWS spend: Glue $0.41 (13 runs), plus S3/Athena/Secrets Manager requests and storage measured in cents.
      All of it is covered by the Free-plan credits (balance $120).
- **GitHub Actions:**
  - `ruff check` + `ruff format --check`;
  - `pytest` with Java 17 (`setup-java`) and a `postgres:16` service container for integration tests;
  - caches for pip and Ivy;
  - a badge in the README.
- **`docs/ARCHITECTURE.md`:**
  - sources, layers and data flow (Mermaid diagram);
  - where each job runs (local vs AWS) and the S3/Athena/Glue layout;
  - bookkeeping tables (watermarks, ingest_runs, layer_runs, dq_results).
- **`docs/DECISIONS.md`:** design choices with the reasons and the alternatives considered:
  - query-based CDC vs Debezium;
  - plain Parquet with a staging swap vs Apache Iceberg (the upgrade path);
  - the pending area and as-of lookups for late-arriving data;
  - SCD2 with hash surrogate keys;
  - salting vs AQE;
  - daily vs monthly partitions (docs/SMALL_FILES.md);
  - compute local plus S3 publish in Step 8 (measured latency);
  - Glue ETL vs EMR (EMR denied by the Free plan's SCP);
  - local Airflow vs MWAA.
- **`docs/RUNBOOK.md`:**
  - rerun a day, `--full-refresh`, backfill through Airflow;
  - what each failure looks like and how to recover (API down, Postgres down, DQ blocking failure, half-written
    Silver staging);
  - publish to S3, the parity check;
  - AWS teardown per step (`make aws-down`, `DRY_RUN=1`).
- **README:**
  - quick start, layer summaries and links to the docs above;
  - a **Known limitations** section (for example: small daily files, no ACID on Parquet, reruns of older days for
    mutable tables, local compute in Step 8, a single-machine Airflow).

## Risks and how they're handled
- **Memory:** one Spark JVM at a time (Airflow pool, sequential Make targets); API and Airflow run only while needed.
- **Small files:** one file per partition, but daily partitions keep files at 4–43 KB (docs/SMALL_FILES.md);
  monthly partitions or Iceberg compaction are the fix.
- **Parquet has no ACID:** the staging swap plus `--full-refresh` from Bronze; Iceberg is the documented upgrade.
- **Time zones:** every Spark session and JVM is pinned to UTC (already in place).
