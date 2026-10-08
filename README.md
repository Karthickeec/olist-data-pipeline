# Olist e-commerce pipeline

[![CI](https://github.com/Karthickeec/olist-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Karthickeec/olist-data-pipeline/actions/workflows/ci.yml)

A multi-source batch pipeline on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Target stack: Python, PySpark, PostgreSQL, FastAPI, Airflow, AWS S3/Athena/Glue/Secrets Manager, Parquet, Bronze/Silver/Gold layers.

**Status: all 10 steps done.** Sources, Bronze, Silver, DQ, Gold and Airflow locally; S3, Athena, Secrets Manager
and Spark on Glue in AWS; CI on GitHub Actions. See [docs/PLAN.md](docs/PLAN.md) for each step's numbers.

Docs: [Architecture](docs/ARCHITECTURE.md) · [Design decisions](docs/DECISIONS.md) · [Runbook](docs/RUNBOOK.md)
(rerun, backfill, failures, AWS teardown) · [Salting](docs/SALTING.md) · [Small files](docs/SMALL_FILES.md)

**AWS region: `ap-southeast-2`.** The AWS account is on the Free plan, and its organization's service control
policy allows only the project's home region (ap-southeast-2); every other region, and EMR everywhere, is denied.
So the lake goes to S3 + Athena in ap-southeast-2, and Spark on AWS runs as Glue ETL jobs instead of EMR
(step 9).
Tables stay plain Parquet; Apache Iceberg on the Glue catalog is the documented upgrade path (ACID MERGE,
time travel, no staging swap).

| Source | What it simulates | Where |
|---|---|---|
| Postgres `olist` schema | the shop's OLTP database, filled one day at a time | `docker compose` service `postgres` |
| Address-change requests | a CRM portal dropping daily JSONL files (some deliberately dirty) | `data/landing/customer_changes/dt=YYYY-MM-DD/changes.jsonl` |
| Customer-activity REST API | a paginated, rate-limited, sometimes failing web service (FastAPI mock) | `make api` → `http://127.0.0.1:8000/v1/customer-activity` |

## Quick start

```bash
# Needs Docker, Python 3.12 and uv. Put the Kaggle CSVs in data/raw/.
make venv          # .venv + dependencies
make up            # Postgres 16, waits until healthy
make seed          # schema + reference tables (insert-only, safe to rerun)
make replay DATE=2017-03-01
make replay-range START=2017-01-01 END=2017-01-31
make replay-all    # 2016-09-04 .. 2018-10-17
make verify        # every table equals its CSV
make verify-idempotency DATES="2017-03-01 2016-10-04"
make test          # unit tests; integration tests run when Postgres is up
```

Bronze (needs Java 17 for Spark; `make java` installs Temurin 17 without sudo):

```bash
make spark-smoke              # Spark + Parquet + JDBC sanity check
make reset-source             # empty transactional tables, pipeline state, landing, lake
make replay-range START=2016-09-04 END=2017-02-28
make bronze DATE=2017-02-28   # first run = initial load
make api                      # in a second terminal: the mock customer-activity API
make daily DATE=2017-03-01    # replay one day, then Postgres, files and API into Bronze
make bronze-api DATE=2017-03-01
make silver DATE=2017-03-01       # Bronze -> Silver (also part of make daily)
make verify-silver               # Silver vs Postgres, accounting, quarantine vs injected dirt
make verify-silver-idempotency DATE=2017-03-04
make dq LAYER=silver DATE=2017-03-06   # data-quality checks (also part of make daily)
make gold DATE=2017-12-31         # Silver -> Gold (star schema, metrics)
make verify-gold DATE=2017-12-31  # totals, SCD2 invariants, LTV
make salting-demo                 # skew experiment -> docs/SALTING.md

# Airflow (own venv, SQLite + LocalExecutor, ~1.1 GB RAM)
make airflow-venv airflow-setup   # once: Airflow 3.3.2 + the 1-slot spark pool
make api                          # in a second terminal (the DAG's api_up sensor waits for it)
make airflow                      # standalone in the background, UI on http://localhost:8080
OLIST_DAG_END_DATE=2018-01-05 make airflow   # bound the catch-up, then unpause olist_daily
make backfill START=2018-01-03 END=2018-01-03 REPROCESS=completed
make airflow-stop
make verify-bronze            # Bronze vs Postgres and landing files
```

Config is in `config/pipeline.yaml`. Any value can be overridden with
`OLIST__<SECTION>__<KEY>`, e.g. `OLIST__PG__PORT=5433 make up seed`.

## How the replay works

`scripts/replay.py` replays history day by day. Each day is one transaction:

- **New orders** (purchased on D) are inserted as they looked at the end of D:
  milestone timestamps after D are null. While milestones are pending, the status is
  derived from the latest known one (`created` → `approved` → `shipped` → `delivered`).
  Once nothing is pending, the order shows its real final status (including
  `canceled`, `unavailable`, …).
- **Earlier orders** that hit a milestone on D are updated. The upsert only applies
  when the stored `updated_at` is older *and* the row actually differs, so an older
  day can never overwrite newer state.
- **Customers, items, payments** are inserted on the purchase day. **Reviews** are
  released on max(review creation date, purchase date). These rows never change
  (`ON CONFLICT DO NOTHING`).
- `updated_at` is the logical end of the replay day (`D 23:59:59`), not the wall
  clock, so reruns are byte-identical and later incremental extracts can use it as a
  watermark.
- After the commit, the day's address-change file is rewritten atomically. The RNG
  is seeded from the date, so rerunning a day produces the same bytes.

Reference tables (products, sellers, category translation, geolocation) are loaded
once by `scripts/seed_reference.py`. It only inserts missing keys and never truncates,
because `order_items` references products and sellers.

## Dataset gotchas handled

- `customer_id` is one per order; `customer_unique_id` is the person (96,096 people,
  99,441 customer_ids). Address changes are keyed on `customer_unique_id`.
- `review_id` is not unique (98,410 ids over 99,224 rows); the key is `(review_id, order_id)`.
  64 reviews are dated before their order was purchased.
- Geolocation has many rows per zip prefix and ~262k exact duplicates, so
  `geolocation_row_id` (CSV row number) is added as the key.
- Milestones are not always in order (~1.4k orders reached the carrier before
  approval, 23 were delivered before carrier pickup). Status follows the furthest
  milestone known, not a fixed sequence.
- Two product categories have no English translation, so there is no FK from
  products to the translation table.
- The translation CSV starts with a UTF-8 BOM.

## Bronze ingestion (step 2)

Two PySpark jobs, run locally (`local[2]`, 2 GB driver). Bronze keeps source columns exactly
as they are, with no cleaning, and adds `_ingested_at` (run time), `_batch_date` and `_source`:

```
<lake_root>/bronze/olist_postgres/<table>/ingest_date=YYYY-MM-DD/*.parquet
<lake_root>/bronze/crm/customer_changes/ingest_date=YYYY-MM-DD/*.parquet
```

`lake.root` in `config/pipeline.yaml` defaults to `data/lake`; set `OLIST__LAKE__ROOT=s3://bucket/prefix`
to switch to S3. Paths are plain strings and deletes go through Hadoop's FileSystem, so local and S3 work the same way.

- **`postgres_to_bronze`: transactional tables** are read over JDBC in the window
  `updated_at > previous high watermark AND updated_at <= end of batch date`, with the filter pushed
  down to Postgres. `pipeline.ingest_runs` stores each batch's window and `pipeline.watermarks` the
  current high mark. Both are committed in one transaction, and only after the Parquet write
  succeeded. A rerun of day D reuses D's original window. A batch after skipped days covers all of
  them, and the watermark never moves backwards. The first run has no lower bound (initial load).
- **`postgres_to_bronze`: reference tables** are hashed in Postgres every batch (md5 over all
  rows in key order). A new snapshot partition is written only when the hash differs from the latest
  snapshot on or before the batch date (`pipeline.reference_snapshots`); otherwise the job logs
  `unchanged, skipped`. Geolocation alone is 1M rows (21 MB of Parquet per snapshot). A full load each
  day would write about 16 GB of identical data over the 774 days.
- **`files_to_bronze`** reads `data/landing/customer_changes/dt=D/` with an all-string schema in
  permissive mode. Duplicates, messy values and malformed lines are all kept; unparseable lines land
  in `_corrupt_record`. `_source_file` records where each row came from.
- **Idempotency:** writes use dynamic partition overwrite, so a run replaces only partition D. An empty
  batch writes no files, which would leave a stale partition behind, so that case clears the partition
  explicitly. Reruns are compared on contents, not file bytes, and without `_ingested_at`, which
  records the new run time by design.
- **Time zones:** Postgres `timestamp` has no zone. The Spark session and JVM are pinned to UTC so values
  arrive unshifted, and Parquet timestamps are written as `TIMESTAMP_MICROS`. Note that PySpark's
  `collect()` converts timestamps to the machine's local zone.

### Known limitation: reruns of older days for mutable tables

Watermark extraction reads the **current** state of each row. If an order was written on day 2 and
updated again on day 3, its `updated_at` is now day 3, so rerunning day 2 *after* day 3 no longer finds
it. The day-2 partition comes out smaller, and the order lives on in the day-3 partition. Nothing is
lost: the latest version per key across all partitions always equals Postgres, which
`make verify-bronze` checks. The four insert-only tables are unaffected.

Verified on 2017-03-01..03 with a rerun of 03-02: customers, items, payments and reviews were
identical, and orders went from 281 to 222 rows, exactly the 59 orders that changed again on 03-03.

| | Query-based (this project) | Log-based CDC (e.g. Debezium) |
|---|---|---|
| Reads | current rows with `updated_at` in a window | every committed change from the WAL |
| Intermediate versions | lost if a row changes twice between batches | every version kept |
| Deletes | invisible (row is gone) | captured as delete events |
| Reruns of old windows | can differ once rows have moved on | replayable from the change log/Kafka |
| Source requirements | an indexed `updated_at` the app maintains | logical replication, a connector, Kafka (or similar) |
| Load on the source | one range query per table per batch | reads the WAL, no table scans |

For a daily batch pipeline over an OLTP source that only updates in place, the query-based approach is
the pragmatic choice. When full change history, deletes or replayable reruns matter, log-based CDC is
the right tool.

## REST API source and api_to_bronze (step 3)

**The service** (`src/olist_pipeline/api/`, FastAPI, run with uvicorn from the venv):
`GET /v1/customer-activity?date=YYYY-MM-DD&page=N&page_size=M` returns paged daily activity per
`customer_unique_id`: `sessions`, `page_views`, `cart_adds`, `support_tickets`, `last_seen_at`, `device`.

- **Who appears:** only customers who had ordered by that date. A customer is active with 15% probability in
  the week after an order, 2% within 30 days, and 0.1% otherwise. That gives a median of 377 records a day
  from mid-2017 (range 162–505) and a handful a day in 2016.
- **Deterministic:** everything is seeded by sha256(date, customer), so a date always returns the
  same records, whatever the paging. Records are sorted by id, so pages are stable.
- **Auth:** the `X-API-Key` header is compared in constant time against `$OLIST_API_KEY`. Missing or wrong → 401.
  The server refuses to start without a key. The Makefile uses a local-only default (`dev-local-key`); the
  key moves to AWS Secrets Manager later.
- **Failure injection** (`api.server` in `pipeline.yaml`, on by default): 3% of requests → 429 with
  `Retry-After`, 2% → 500, and 1% of records are malformed (missing field, negative `sessions`,
  `last_seen_at` as `dd/mm/yyyy HH:MM:SS`). Failures never change a page's content.

**The job** (`api_to_bronze --date D`):
1. Pages through the API with httpx. 429, 5xx, timeouts and connection errors are retried: the client waits
   for `Retry-After` when the server sends it, and otherwise uses exponential backoff with jitter (0.5 s × 2ⁿ, capped at 30 s).
   The run fails after 5 attempts at the same request. 401 and other 4xx errors fail immediately. Every
   retry is logged with status, attempt and wait. The record total across pages must match `total_records`.
2. Lands each raw response body in `data/landing/customer_activity/dt=D/page_NNNN.json`. The folder is
   built next to the old one and swapped in only once every page is on disk. A failed run leaves the
   previous landing intact, and a rerun with fewer pages leaves no stale pages.
3. Spark reads the pages (`multiLine`, permissive, record fields as strings) into
   `bronze/api/customer_activity/ingest_date=D/`, one row per record, plus `_page`, `_source_file`,
   `_corrupt_record` and the usual metadata. A page that is not valid JSON is kept as one row with
   `_corrupt_record`.
4. Prints a summary line, e.g. `api_to_bronze 2017-03-05: pages=20 records=97 retries=1 total_wait=1.0s bronze_rows=97`.

Example retry from a real run:
`WARNING retry 2017-03-05 page 19: HTTP 429 on attempt 1/5, waiting 1.00s (Retry-After)`

Because the API is a pure function of the date, reruns are always identical: the same landing bytes
and the same Bronze rows apart from `_ingested_at`, even when different requests failed and were retried.

## Silver (step 4)

`make silver DATE=D` builds typed, cleaned, deduplicated tables in `<lake>/silver/<table>/`:

| Table | Partitioned by | Built from |
|---|---|---|
| orders, order_items, order_payments | `order_purchase_date` | Postgres Bronze (items and payments take the order's date) |
| order_reviews | `review_date` | Postgres Bronze |
| customers, products, sellers, geolocation | none | Postgres Bronze (reference tables from the latest snapshot) |
| customer_changes | `requested_date` | CRM files |
| customer_activity | `activity_date` | API |
| order_lines | `order_purchase_date` | orders + items + payments (per order) + customers + products + sellers |

- **Incremental:** each table processes the Bronze partitions in `(last processed, D]` (`pipeline.layer_runs`),
  so normal days, catch-up batches and reruns use one rule. Reference tables are rebuilt only when a new
  snapshot arrived.
- **Latest row per key:** `row_number()` over the key, ordered by `updated_at`, then Bronze partition, then
  processing time. **Merge** on plain Parquet works like this: read the touched partitions, union, keep the latest per key,
  write to `_staging`, then swap partitions in. `--full-refresh` rebuilds from Bronze.
- **Cleaning:**
  - **States:** trimmed, uppercased, and full names mapped to codes (`São Paulo` → `SP`).
  - **Cities:** a null city is filled from geolocation by zip prefix.
  - **Timestamps:** both API formats (ISO `…Z` and `dd/MM/yyyy HH:mm:ss`) are parsed.
  - **Counts:** cast to int.
  - **Duplicates:** CRM duplicates collapse on `change_id`.
  - **Categories:** every product gets an English category (2 manual translations, blank → `unknown`).
  - **Geolocation:** one row per zip prefix (19,015) with exact median lat/lng and the most common city/state.
  - **Column names:** the source typos are fixed (`product_name_lenght` → `product_name_length`).
- **Quarantine, not drop:** rows that can't be fixed go to `silver/_quarantine/<table>/batch_date=D/` with all
  original columns plus `_reason` (`negative_sessions`, `unknown_customer`, `invalid_state`, `unparseable_timestamp`,
  `corrupt_record`, …).
- **Late-arriving rows:** an item or payment whose order is not in Silver yet (e.g. the order's newer version
  landed in a later Bronze partition) waits in `silver/_pending/` and is retried each batch for 7 days before
  being quarantined as an orphan. On the real data, 41 items and 38 payments from 2017-03-02 waited one batch
  and resolved on 03-03.
- **As-of lookups:** every Silver row carries `_first_batch_date`, and a batch only treats parents first seen
  by that batch as known. That keeps reruns of old days identical even after later batches have run.
- **`order_lines`:** one row per item, with payments aggregated per order. The aggregate is also split across lines
  (`allocated_payment`, to the cent, the last line takes the remainder), so summing lines never double-counts payments.
  Customers, products and sellers are broadcast (small). Items ⋈ orders ⋈ payments use sort-merge joins, scoped
  to the rebuilt partitions. The reasons are written as comments in `silver/order_lines.py`.
- **Row accounting:** every table-batch satisfies `rows_in = valid + quarantined + duplicate + pending`
  (asserted in the job and checked by `make verify-silver`).

Verified on 2017-02-28..03-06:
- Silver equals Postgres for all 5 transactional tables (3,413 orders, 3,827 items).
- 77 table-batches balance.
- `order_lines` allocations sum exactly to each order's payments (3,316 orders).
- Quarantine counts equal the dirt counted independently from the landing files.
- Reruns of 03-02 and 03-04 leave every Silver, quarantine and pending area unchanged.

## Data quality (step 5)

Declarative checks per table in `config/dq/{bronze,silver}.yaml` (Gold in step 6). The engine is
`olist_pipeline/dq/` and the CLI is `make dq LAYER=… DATE=…` (or `--start/--end` for a range in one Spark session).

```yaml
silver.orders:
  key: [order_id]
  checks:
    - {type: unique, columns: [order_id], scope: table, severity: error}
    - {type: accepted_values, column: order_status, values: [created, approved, ...], severity: error}
    - {type: relationship, column: customer_id, ref: silver.customers.customer_id, severity: error}
    - {type: row_count_vs_previous, min_ratio: 0.1, max_ratio: 10, severity: warn}
```

- **Check types:** `not_null`, `unique`, `accepted_values`, `range`, `schema` (expected Spark types),
  `relationship` (left anti-join to the parent), `row_count_vs_previous` (vs the previous batch's count in
  `dq_results`), and `expression` (any SQL predicate every row must satisfy).
- **Scope:** `batch` (rows this batch wrote: `ingest_date` in Bronze, `_batch_date` in Silver), `table`, or `latest`
  (the newest Bronze snapshot, for reference tables).
- **Severity:** `error` fails the run (exit 1), and so does a check that cannot run (`error_running`, e.g. a missing column).
  `warn` only logs. Tables can be marked `optional`.
- **Results:** `pipeline.dq_results (batch_date, layer, table_name, check_name, severity, status, failed_rows,
  observed, sample, message)`. `sample` holds up to 5 failing rows as JSON. A rerun replaces the batch's rows.
- **Validation:** the YAML is checked before anything runs. Unknown check types, missing or misspelled
  parameters, bad severities or scopes, and duplicate names are all clear errors.
- **In `make daily`:** `dq bronze` runs after Bronze and `dq silver` after Silver.

On the real data (2017-02-28..03-06), 32 Bronze and 47 Silver checks run per day. There are no blocking failures, and the warnings are genuine:
- 7 zip prefixes whose median coordinates lie outside Brazil (one has longitude 121, in the Philippines);
- API records missing optional fields;
- the first daily batch after the initial load is 3% of its size.

An injected bad batch (an invalid state, a duplicate order, a null customer id, an order line whose customer doesn't exist)
produces 5 `fail` rows with samples, and the run exits 1.

## Gold (step 6)

### Building up history (catch-up)

The local source was at 2017-03-06, so I caught it up to 2017-12-31 in one go before building Gold. This is a
catch-up run: Postgres → Bronze is **one** batch whose watermark window covers the whole gap, while the CRM files and
the API are ingested per day (one Spark session each, `--start/--end`). Wall time for 300 days, about 5 minutes:

| Part | Time | Volume |
|---|---|---|
| Replay 2017-03-07..12-31 into Postgres | 14 s | 42,017 new orders |
| `postgres_to_bronze` (one catch-up window) | 10 s | 42,926 orders, 47,407 items |
| `files_to_bronze` (300 partitions) | 55 s | 1,235 change requests |
| `api_to_bronze` (300 days) | 113 s | 485 pages, 66,815 records, 30 retries (15×429, 15×500), 21 s waiting |
| DQ Bronze | 13 s | 32 checks |
| Silver (one batch over all new partitions) | 77 s | 45,430 orders, 51,234 order lines |
| DQ Silver | 23 s | 47 checks |

Trade-off: the catch-up batch keeps only each order's latest state, not the daily states in between.

### Star schema (`<lake>/gold/`)

| Table | Grain | Rows (to 2017-12-31) |
|---|---|---|
| `fact_order_lines` | order item, partitioned by purchase date | 51,234 |
| `dim_customer` | SCD Type 2 version of a `customer_unique_id` | 45,255 versions, 44,034 customers |
| `dim_product`, `dim_seller`, `dim_date` | product / seller / day | 32,951 / 3,095 / 1,096 |
| `agg_daily_category_sales` | day × English category, sales only (no canceled/unavailable) | 10,027 |
| `customer_metrics` | customer, snapshot per `as_of_date` | 43,316 |

- **Surrogate keys** are 64-bit hashes of the natural key (plus `valid_from` for SCD2 versions), so rebuilding a
  dimension never changes the keys facts point to.
- **`dim_customer` (SCD2):**
  - Version 1 is the address on the customer's first order.
  - Each clean CRM address change opens a new version at `requested_at` and closes the previous one; requests that keep the same address are skipped.
  - `valid_to` is exclusive and current rows end at 9999-12-31.
  - 1,200 customers have more than one version.
  - The fact's `customer_sk` (and `customer_state`) is the version valid at purchase time: a broadcast range join.
- **`customer_metrics`:** lifetime value (allocated payments), order count, average order value, days since the last order, and RFM.
  - Recency and monetary scores are quintiles (`ntile(5)`).
  - Frequency uses fixed bands (1, 2, 3, 4–5, 6+), because 97% of customers ordered once.
  - Segments, first match wins:
    - **Champions:** R≥4, F≥2, M≥4.
    - **Loyal:** R≥2, F≥2.
    - **Potential:** R≥4.
    - **At risk:** R≥2, M≥4.
    - **Hibernating:** R≥2.
    - **Lost:** everyone else.
  - As of 2017-12-31: Champions 416, Loyal 642, Potential 16,790, At risk 6,368, Hibernating 10,436, Lost 8,664.
- **Incremental:** dimensions are rebuilt in full (small, deterministic); the fact and the daily aggregate rebuild only the
  purchase-date partitions Silver batch D rewrote; `customer_metrics` writes the `as_of_date=D` snapshot.
- **DQ** (`config/dq/gold.yaml`, 31 checks) covers:
  - every fact key resolving to its dimension;
  - one current version per customer;
  - `valid_from < valid_to`, and current rows being open-ended;
  - the RFM ranges and segment values.

  One genuine warning: 7 sellers whose zip prefix isn't in the geolocation data.

Verified (`make verify-gold DATE=2017-12-31`):
- the fact equals Silver order_lines (51,234 lines, R$6,205,592.90 in item prices);
- the daily aggregate equals the fact's sales;
- SCD2 has no gaps or overlaps;
- all 51,234 fact rows point to the version valid at purchase;
- LTV equals the fact payments for all 43,316 customers (R$7,143,826.57 in total);
- a Gold rerun leaves all 7 tables unchanged.

### Salting demo

[`docs/SALTING.md`](docs/SALTING.md) measures skew on `customer_state` (SP is 39% of rows) over 5.1M rows.
- **Sum/count:** salting is pointless. Partial aggregation means at most 82 rows reach any post-shuffle task.
- **Sort-merge join on state:** the busiest task reads 2.51M rows. Salting cuts it to 994k rows (and its time about in half).
- **AQE's skew-join handling** splits it with no code change: 25 tasks, the busiest at 522k rows.
- **Gotcha:** AQE only does this when *both* join sides are shuffled. My first attempt used a small side that was already hash-partitioned on the key, and AQE silently did nothing.

## Airflow (step 7)

One DAG, `olist_daily` ([`airflow/dags/olist_daily.py`](airflow/dags/olist_daily.py)), runs a business day end to end:

```
api_up (sensor) ─┐
replay ──────────┴─> [bronze_postgres, bronze_files, bronze_api] -> dq_bronze -> silver -> dq_silver -> gold -> dq_gold
```

- **Light setup for 8 GB:**
  - Airflow 3.3.2 (`airflow standalone`) in its own `.venv-airflow`, installed with the official constraints file.
  - SQLite metadata with the LocalExecutor (Airflow 3 accepts the combination), so there are no extra containers.
  - About 1.1 GB RSS across 11 processes.
  - Tasks are BashOperators that run the project's own `.venv`, so Airflow's dependencies never mix with PySpark or FastAPI.
- **One Spark JVM at a time:** every Spark task uses the pool `spark`, which has 1 slot. The three Bronze tasks are parallel in the graph but run one after another.
- **`max_active_runs=1`:** each day depends on the previous day's source state and watermarks.
- **Retries:** 2, with exponential backoff (1 min base, 10 min cap). The API task's own HTTP retries come first.
- **`api_up`:** a sensor in reschedule mode that checks `/healthz` every 30 s for up to 10 min without holding a worker slot.
  Note that Airflow never retries a sensor that times out.
- **Callbacks:** `on_retry` / `on_failure` callbacks log one clear block with the DAG, task, run date, try number, error and log location.
- **Catch-up and backfill:** `catchup=True` from 2018-01-01, with an `end_date` (default 2018-10-17, the end of the history;
  `OLIST_DAG_END_DATE` overrides it). Airflow 3 schedules no task of a paused DAG, backfill runs included (I checked the
  scheduler source), so the DAG is created paused and *unpausing it is the catch-up*. `make backfill` reruns chosen days.

Verified:
- **Catch-up:** with `OLIST_DAG_END_DATE=2018-01-05`, unpausing ran 2018-01-01..05 in order, one at a time; all 5 succeeded
  (2:19–2:31 each), and no run for 01-06 was created.
- **Backfill:** `make backfill START=2018-01-03 END=2018-01-03 REPROCESS=completed` reprocessed that run in place
  (`run_type=backfill`, all 10 tasks on try 2) and it succeeded in 2:23.
- **Failure demo** ([`scripts/airflow_failure_demo.sh`](scripts/airflow_failure_demo.sh), 2018-01-06), with Postgres and the API stopped:
  - `replay` failed (connection refused), logged the retry block, retried after about 30 s once Postgres was back, and succeeded on try 2.
  - `api_up` checked three times, 30 s apart, and timed out after 62.5 s (demo timeout 60 s); the failure block was logged.
  - All downstream tasks became `upstream_failed` and the run failed.
  - After starting the API and clearing `api_up` and its downstream tasks, the run succeeded (2:17).
  - The two blocks from the task logs:
  ```
  OLIST PIPELINE TASK WILL BE RETRIED
    task:       replay
    try:        1 of 3
    error:      AirflowException: Bash command failed. The command returned a non-zero exit code 1.
  OLIST PIPELINE TASK FAILED (no retries left)
    task:       api_up
    try:        1 of 3
    error:      AirflowSensorTimeout: Sensor has timed out; run duration of 62.518907 seconds exceeds the specified timeout of 60.0.
  ```
- **Results of the Airflow-run days:** `verify-bronze`, `verify-silver` and `verify-gold` pass through 2018-01-06:
  46,617 orders and 52,575 order lines; 154 Silver table-batches balance; LTV equals fact payments for 44,458 customers.
  DQ found 0 blocking failures over 18 layer-days.

## AWS: S3 lake, Secrets Manager, Athena (step 8)

Everything is in **ap-southeast-2** (the only region the account's Free-plan SCP allows) and tagged
`project=olist-pipeline`. One CloudFormation stack ([`infra/step8.yaml`](infra/step8.yaml)) holds:
- **The bucket** `olist-pipeline-<account>-apse2`. The account id is looked up at run time, never committed.
  - Private (Block Public Access), SSE-S3, versioning, TLS-only policy.
  - Lifecycle: landing/ to Glacier IR after 90 days; Athena results and noncurrent versions expire after 7 days.
- **The Athena workgroup** `olist`, with a 1 GB scan cutoff per query.
- **The Glue database** `olist_lake`.

`scripts/aws.py` creates the secret `olist/pipeline` (Postgres password and API key) separately, so no secret
value ever sits in a template. A $5/month AWS Budget alerts by email at 50% and 100% of actual spend and at 100%
of forecast spend.

```bash
make aws-check            # read-only probes (EMR Serverless shows "SCP deny")
make aws-up               # stack + secret
make aws-sync             # first upload of the lake and landing files
make run-days START=2018-01-07 END=2018-01-09   # local pipeline, DQ proof, publish per day
make verify-s3-parity     # S3 == local: keys, sizes, MD5/ETags
make athena-create athena-queries athena-register-partitions
make athena-check DATE=2018-01-09
make small-files-experiment
make aws-down DRY_RUN=1   # teardown preview (see docs/RUNBOOK.md)
```

**Credentials:**
- Spark and boto3 use the default AWS credential chain (the local profile); keys never appear in config or Spark conf.
- With `OLIST_TARGET=aws` (`make … AWS=1`), config values written as `secret://olist/pipeline#pg_password` are read
  from Secrets Manager once per process.
- Resolved values are wrapped so that a config dump or traceback shows `'***'` (tested).

### Why compute stays local in this step

The plan was to point the local Spark jobs straight at `s3a://` and run 2018-01-07..09 on S3. Measured from this Mac:
- **Latency:** a TCP connect to `s3.ap-southeast-2.amazonaws.com` takes about **0.38 s**, and the first response
  byte arrives after about **1.1 s**.
- **What that does to Spark:** each Parquet file costs a metadata request, a footer read and a data read. Spark
  waits on the network, not on CPU or memory.
- **Bronze** for one day took **330 s** on S3, against about 10 s locally.
- **Silver** for one day was still running after **51 min** (under a minute locally) and was stopped.
- **A Spark content comparison** of the whole lake (local vs S3) was stopped after 53 min.

So in step 8:
- **Compute stays local and S3 is the published copy.** `make run-days` runs each day locally (`local[2]`, 2 GB
  driver), checks that `pipeline.dq_results` got fresh rows for all three layers with no blocking failures, then
  runs `aws.py publish`. That's an incremental `aws s3 sync` that also deletes replaced part-files, so S3 keeps
  exactly the local lake; versioning keeps old objects for 7 days.
- **Parity is proven from listings,** not by reading data: every local file must exist on S3 with the same key,
  size and MD5/ETag (multipart ETags are recomputed with the CLI's 8 MiB parts).
- **Athena reads S3.**
- **Step 9 moves compute next to the data** (Glue in ap-southeast-2), where these round trips take milliseconds.

The aborted S3 run had already written Bronze and part of Silver for 2018-01-07 (and Postgres had recorded it).
That was pulled back with `aws s3 sync --delete` first, so the local lake matched the bookkeeping again; the day
was then rerun locally, which reruns allow.

### Results

| | |
|---|---|
| First upload | 4,162 lake files (123.9 MB) + 990 landing files (14.4 MB) in 7.6 min; parity identical in 7.4 s |
| 2018-01-07 / 08 / 09 | pipeline 130 / 134 / 127 s, publish 25 / 38 / 40 s (104–221 files up, 92–197 replaced files deleted) |
| DQ per day | Bronze 32, Silver 47, Gold 31 checks, 0 blocking failures (fresh rows checked for every layer) |
| After 2018-01-09 | S3 identical to local: 4,217 lake files (130.9 MB), 999 landing files; `verify-bronze/silver/gold` pass (53,401 fact lines; LTV = fact payments, R$7,437,125.17 over 45,178 customers) |
| Athena tables | 31 external Parquet tables (Bronze 11, Silver 11 + 2 quarantine, Gold 7), DDL generated from the Parquet schemas into `sql/athena/` |
| Athena vs Spark | 7/7 answers identical (fact lines and payments, last-day lines, RFM segments, SCD2 versions, orders by status, Bronze ingest count, quarantine), 1.9 MB scanned in total |
| KPI named queries | daily revenue by category, RFM segment mix, top sellers, customers who moved state: 0.01–2.4 MB scanned, 0.8–2.5 s each |
| Partition projection | new days need no registration. Fallback demo: a copy of `silver_orders` without projection returns 0 rows until 385 partitions are added with `ALTER TABLE … ADD IF NOT EXISTS PARTITION`, then 47,358 rows, the same as the projected table |
| Glue test job | Glue 5.0 (Spark 3.5.4-amzn-0), Flex, 2 × G.1X: read `gold/dim_date` from S3 and wrote a count, SUCCEEDED in 93 s; 171 DPU-seconds = **$0.014** |

**Small files** ([docs/SMALL_FILES.md](docs/SMALL_FILES.md)):
- Silver/Gold event tables already write one file per partition, but the partitions are daily: about 380 per
  table, at 4–43 KB per file. `coalesce` or `maxRecordsPerFile` can't help.
- A monthly copy of the fact table has 16 files of 322 KB instead of 383 files of 21 KB.
- Athena's full scan went from 1.40 s to 0.47 s (median of 5); a one-month query went from 0.46 s to 0.36 s.
- The pipeline keeps daily partitions for now (see Known limitations).

**Table format:**
- Plain Parquet with the staging swap.
- **Apache Iceberg** on the Glue catalog is the upgrade path: ACID `MERGE INTO` instead of the swap, snapshot
  time travel, hidden partitioning (which would also fix the small-files issue through compaction) and no
  dependence on S3 rename semantics.

## Spark on AWS Glue, orchestrated by Airflow (step 9)

EMR and EMR Serverless are denied by the account's Free-plan SCP, so Spark on AWS runs as **AWS Glue ETL**
jobs. One job, `olist-spark` (Glue 5.0 = Spark 3.5.4 / Python 3.11, **Flex**, 2 × G.1X, timeout 30 min,
MaxConcurrentRuns 1), takes `--TASK silver|gold|dq_silver|dq_gold` and `--DATE`.
- `make glue-deploy` builds the project wheel and uploads it with [`infra/glue_job.py`](infra/glue_job.py) and
  the DQ suites.
- The wheel is installed with `--additional-python-modules`.
- The job runs the **same** `SilverJob`, `GoldJob` and DQ `Runner` code as the local runs.

**Control plane local, data plane on AWS.** Sources and Bronze stay local, because Postgres stays local by
choice (no RDS). Glue can't reach Postgres either, so the bookkeeping travels as JSON
([`olist_pipeline/control.py`](src/olist_pipeline/control.py)):
1. **`glue_run.py export`** writes the latest layer run per table and the latest DQ observation per check
   (before the batch date) to `s3://…/control/<task>/<date>/state.json`.
2. **The Glue job** uses a `FileBook` with the same interface as the Postgres `Bookkeeping`. It writes what it
   would have recorded (layer runs, DQ results) to `output.json`.
3. **`glue_run.py import`** writes those records into Postgres, logs DPU-seconds and cost to
   `data/glue_runs.jsonl`, and fails on blocking DQ failures.

The DAG [`olist_daily_aws`](airflow/dags/olist_daily_aws.py):

```
api_up, replay -> bronze_postgres, bronze_files, bronze_api -> dq_bronze -> publish_bronze
  -> export_silver -> glue_silver -> import_silver -> export_dq_silver -> glue_dq_silver -> import_dq_silver
  -> export_gold -> glue_gold -> import_gold -> export_dq_gold -> glue_dq_gold -> import_dq_gold -> pull_lake
```

- **Glue tasks:** `GlueJobOperator` from the Amazon provider, installed into `.venv-airflow` with the official
  constraints. They pass only task and date; the job derives its S3 paths from its own defaults, so the bucket
  name and account id never appear in Airflow.
- **`publish_bronze` / `pull_lake`:** incremental `aws s3 sync` in each direction, so the local lake keeps
  matching S3.
- **Concurrent-run race:**
  - A Glue run that has just SUCCEEDED still counts against MaxConcurrentRuns=1 for a few seconds.
  - On 2018-01-11 the next `StartJobRun` twice failed with `ConcurrentRunsExceededException` (the retries
    absorbed it).
  - The Glue tasks now use `sleep_before_return=30`; 2018-01-12 ran without a single retry.

```bash
make glue-deploy
make glue-run TASK=silver DATE=2018-01-10      # one task without Airflow (export, run, wait, import)
OLIST_AWS_DAG_START=2018-01-11 OLIST_AWS_DAG_END=2018-01-12 make airflow   # then unpause olist_daily_aws
```

### Results

| | |
|---|---|
| Glue output = local Spark output | 2018-01-10 ran Silver and Gold on Glue, then `verify-silver-idempotency` / `verify-gold-idempotency` reran the day locally and compared contents: all 11 Silver tables + pending/quarantine and all 7 Gold tables identical (e.g. 53,718 order lines, 47,465 SCD2 versions, 487,051 metric rows) |
| Airflow on Glue | `olist_daily_aws` caught up 2018-01-11 and 01-12: both succeeded (20.3 and 22.6 min) |
| Task times (avg) | glue_silver 329 s, glue_gold 296 s, glue_dq_silver 250 s, glue_dq_gold 234 s (Flex start-up included); publish 25 s, pull 21 s |
| DQ on Glue | Silver 47 and Gold 31 checks per day, 0 blocking failures, imported into `pipeline.dq_results` |
| After 2018-01-12 | `verify-bronze/silver/gold` pass (54,307 fact lines; LTV = fact payments, R$7,565,259.72 over 45,933 customers); S3 = local (4,511 lake + 1,008 landing files, keys/sizes/ETags); Athena = Spark 7/7 |
| Cost | 12 Glue runs, 4,903 DPU-seconds billed in total: **$0.395** (about $0.13 per day: Silver $0.04–0.06, Gold $0.03, each DQ suite $0.02–0.03); plus the step-8 test job $0.014 |

The parity check had to learn one thing: Glue (EMRFS) uploads even small files as one-part multipart uploads,
so their ETag is `md5(md5(file))-1`, not the file's MD5. Keys and sizes matched and the bytes were the same.

**Why not Glue for Bronze too:** Bronze reads the local Postgres (JDBC), the local landing files and the
local mock API. Moving it would mean RDS and a hosted API: more cost and setup for no change in the Spark logic.

## Known limitations

- **Small files.** Silver/Gold event tables are partitioned by day, so files are 4–43 KB (about 380 per table).
  Monthly partitions cut the fact table to 16 files and Athena's full scan from 1.40 s to 0.47 s
  ([SMALL_FILES.md](docs/SMALL_FILES.md)), but the pipeline still writes daily partitions. Fix: monthly partitions
  or Iceberg with compaction.
- **No ACID on Parquet.** Silver/Gold merges are a staging write plus a partition-directory swap. A reader
  during the swap can see a partition missing, and on S3 the swap is a copy. Reruns repair an interrupted swap.
  Upgrade path: Apache Iceberg on the Glue catalog (`MERGE INTO`, snapshots, time travel).
- **Query-based CDC.** Hard deletes are invisible, only the last version of a row per batch window is seen,
  and rerunning an older day for orders after later days moves rows that changed later to the later
  partition (nothing is lost; see "Known limitation: reruns of older days for mutable tables").
- **Local sources and Bronze.** Postgres, the CRM files and the mock API are local, so Bronze always runs on
  the laptop; Glue runs Silver/Gold only. Moving Bronze needs RDS (or a reachable database) and a hosted API.
- **Bookkeeping through JSON on Glue.** The control state is exported before and imported after each Glue
  run; if an import step is skipped, Postgres doesn't know about that run (rerun the import or the task).
- **Local compute against S3 is impractical from here.** About 0.4 s per S3 round trip to Sydney; local
  Spark on `s3a://` paths is only for small checks.
- **Single-machine Airflow.** Standalone with SQLite and the LocalExecutor: no HA, one Spark task at a time
  (pool `spark` with 1 slot). Fine for a daily batch on one laptop; MWAA or a managed scheduler otherwise.
- **One region, no EMR.** The Free-plan account's SCP allows only ap-southeast-2 and denies EMR/EMR Serverless.
- **CI runs without the Kaggle CSVs.** They aren't in the repo (license and size), so 32 CSV-dependent tests
  skip on GitHub Actions; all 118 run locally.

## Layout

```
docs/                    PLAN.md, ARCHITECTURE.md, DECISIONS.md, RUNBOOK.md, SALTING.md, SMALL_FILES.md
.github/workflows/       ci.yml (ruff + pytest with Java 17 and Postgres 16)
infra/                   step8.yaml (CloudFormation), glue_job.py (Glue entry point), glue_test_job.py
airflow/dags/            olist_daily (local), olist_daily_aws (Glue), olist_common (shared helpers);
                         the rest of airflow/ is local state, gitignored
config/pipeline.yaml     defaults (env overrides: OLIST__SECTION__KEY)
config/dq/                data-quality suites per layer
sql/001_schema.sql       source schema (idempotent DDL)
sql/pipeline/            watermarks, ingest_runs, reference_snapshots, layer_runs, dq_results
sql/athena/              generated Athena DDL per layer, kpis/ (named queries)
src/olist_pipeline/      config, db, sources (CSV specs), replay_logic (pure rules),
                         replay, customer_changes, seed, verify,
                         spark, lake, watermarks, bronze/ (postgres_to_bronze, files_to_bronze,
                         api_to_bronze), api/ (activity, app, server, client),
                         silver/ (common, clean, order_lines, job), dq/ (suite, checks, engine),
                         gold/ (model, job), aws (secrets, bucket, S3A), athena (catalog, DDL),
                         control (Glue bookkeeping)
scripts/                 seed_reference.py, replay.py, verify_replay.py, install_java.sh,
                         spark_smoke.py, postgres_to_bronze.py, files_to_bronze.py,
                         api_to_bronze.py, silver.py, gold.py, dq.py, salting_demo.py,
                         verify_bronze.py, verify_silver.py, verify_gold.py, airflow_failure_demo.sh,
                         reset_source.py, aws.py, athena.py, run_days.sh, verify_s3_parity.py,
                         small_files_experiment.py, glue_run.py
tests/                   unit tests (incl. Spark); tests/integration needs Postgres
```
