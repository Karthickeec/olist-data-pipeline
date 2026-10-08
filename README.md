# Olist e-commerce pipeline

A multi-source batch pipeline on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Target stack: Python, PySpark, PostgreSQL, FastAPI, Airflow, AWS S3/EMR/Athena, Parquet, Bronze/Silver/Gold layers.

**Status: step 4 done (Silver). Plan for the remaining steps: [docs/PLAN.md](docs/PLAN.md).**

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

## Layout

```
config/pipeline.yaml     defaults (env overrides: OLIST__SECTION__KEY)
sql/001_schema.sql       source schema (idempotent DDL)
sql/pipeline/            watermarks, ingest_runs, reference_snapshots, layer_runs
src/olist_pipeline/      config, db, sources (CSV specs), replay_logic (pure rules),
                         replay, customer_changes, seed, verify,
                         spark, lake, watermarks, bronze/ (postgres_to_bronze, files_to_bronze,
                         api_to_bronze), api/ (activity, app, server, client),
                         silver/ (common, clean, order_lines, job)
scripts/                 seed_reference.py, replay.py, verify_replay.py, install_java.sh,
                         spark_smoke.py, postgres_to_bronze.py, files_to_bronze.py,
                         api_to_bronze.py, silver.py, verify_bronze.py, verify_silver.py,
                         reset_source.py
tests/                   unit tests (incl. Spark); tests/integration needs Postgres
```
