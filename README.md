# Olist e-commerce pipeline

A multi-source batch pipeline on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Target stack: Python, PySpark, PostgreSQL, FastAPI, Airflow, AWS S3/EMR/Athena, Parquet, Bronze/Silver/Gold layers.

**Status: step 2 done. Step 1: simulated source systems. Step 2: Bronze ingestion with PySpark.**

| Source | What it simulates | Where |
|---|---|---|
| Postgres `olist` schema | the shop's OLTP database, filled one day at a time | `docker compose` service `postgres` |
| Address-change requests | a CRM portal dropping daily JSONL files (some deliberately dirty) | `data/landing/customer_changes/dt=YYYY-MM-DD/changes.jsonl` |

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
make daily DATE=2017-03-01    # replay one day into the source, then ingest it
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

## Layout

```
config/pipeline.yaml     defaults (env overrides: OLIST__SECTION__KEY)
sql/001_schema.sql       source schema (idempotent DDL)
sql/pipeline/            watermarks, ingest_runs, reference_snapshots
src/olist_pipeline/      config, db, sources (CSV specs), replay_logic (pure rules),
                         replay, customer_changes, seed, verify,
                         spark, lake, watermarks, bronze/ (postgres_to_bronze, files_to_bronze)
scripts/                 seed_reference.py, replay.py, verify_replay.py, install_java.sh,
                         spark_smoke.py, postgres_to_bronze.py, files_to_bronze.py,
                         verify_bronze.py, reset_source.py
tests/                   unit tests (incl. Spark); tests/integration needs Postgres
```
