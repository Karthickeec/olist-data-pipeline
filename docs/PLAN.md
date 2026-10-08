# Project plan: Olist multi-source e-commerce pipeline

Portfolio project for a data engineering role (Python, PySpark, SQL, Unix, Airflow).
Three simulated source systems feed a Bronze/Silver/Gold lake. The project is built locally
first (8 GB Mac) and moved to AWS (S3, EMR, Athena) afterwards.

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

## Steps 8–10 (plan only; stop before executing Step 8)

### Step 8: lake on S3, Secrets Manager, Athena
- **S3:** one private bucket with versioning, SSE-S3 encryption and public access blocked. Lifecycle rule: landing/ → Glacier Instant Retrieval after 90 days. `lake.root=s3a://<bucket>/lake`.
- **Spark on S3:** `hadoop-aws:3.3.4` plus the matching AWS SDK bundle (Spark 3.5 ships Hadoop 3.3.4). The S3A committer is used for reliable partition overwrites.
- **Credentials:**
  - an AWS profile locally, an IAM role on EMR;
  - the DB password and API key live in **Secrets Manager** and are resolved at runtime with boto3 (config holds `secret://olist/api-key`-style references);
  - no secrets in git.
- **Athena:**
  - a Glue database `olist_lake` with external Parquet tables for Bronze, Silver and Gold;
  - **partition projection** on `ingest_date`/business dates, so new partitions need no registration;
  - a fallback `register_partitions` task (`ALTER TABLE … ADD IF NOT EXISTS PARTITION`) for unprojected tables;
  - saved queries for the Gold KPIs.
- **Decision for later:** keep plain Parquet, or move Silver/Gold to **Apache Iceberg** (Glue catalog), which brings ACID MERGE and makes the staging swap unnecessary. Plain Parquet is the plan unless Iceberg is chosen at Step 8.
- **Cost estimate:** S3 <1 GB is about $0.03/month; Athena costs $5 per TB scanned, so cents for this data volume.

### Step 9: Spark on AWS (transient EMR vs EMR Serverless)
- **Option A, EMR on EC2:** Airflow creates the cluster, adds steps and waits on a sensor (`EmrCreateJobFlowOperator`, `EmrAddStepsOperator`, `EmrStepSensor`), then terminates it, with auto-termination as a safety net. 1 master + 2 core on-demand m5.xlarge comes to about $0.72/h including the EMR fee. With ~8–10 minutes of startup, that's roughly $0.15–0.20 per daily run.
- **Option B, EMR Serverless:** `EmrServerlessStartJobRunOperator`; billed per vCPU-hour and GB-hour with no idle cost. A 10-minute job at 4 vCPU / 16 GB is roughly $0.05 per run, and there's no cluster startup to wait for.
- **Source access:** EMR can't reach the laptop's Postgres. Either run Bronze-from-Postgres locally and write to S3, or move the source to RDS (free-tier db.t4g.micro if the account is eligible). Decided at Step 9.
- Airflow stays local (MWAA costs about $0.49/h at minimum). Prices are to be re-checked at execution time.

### Step 10: CI, docs, interview notes
- **GitHub Actions:**
  - `ruff check` + `ruff format --check`;
  - `pytest` with Java 17 (`setup-java`) and a `postgres:16` service container for integration tests;
  - caches for pip and Ivy;
  - a badge in the README.
- **README:** architecture diagram (Mermaid), quick start, layer docs.
- **`docs/INTERVIEW_NOTES.md`:** what was built, key numbers per step, design decisions and alternatives
  (query-based CDC vs Debezium, Parquet vs Iceberg, salting vs AQE, transient EMR vs Serverless), and known limitations.

## Risks and how they're handled
- **Memory:** one Spark JVM at a time (Airflow pool, sequential Make targets); API and Airflow run only while needed.
- **Small files:** Silver and Gold write `coalesce(1–4)` per partition; partitions stay daily and small.
- **Parquet has no ACID:** the staging swap plus `--full-refresh` from Bronze; Iceberg is the documented upgrade.
- **Time zones:** every Spark session and JVM is pinned to UTC (already in place).
