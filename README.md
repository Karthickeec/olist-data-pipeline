# Olist e-commerce pipeline

A multi-source batch pipeline on the [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).
Target stack: Python, PySpark, PostgreSQL, FastAPI, Airflow, AWS S3/EMR/Athena, Parquet, Bronze/Silver/Gold layers.

**Status: step 1, the simulated source systems.**

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

## Layout

```
config/pipeline.yaml     defaults (env overrides: OLIST__SECTION__KEY)
sql/001_schema.sql       source schema (idempotent DDL)
src/olist_pipeline/      config, db, sources (CSV specs), replay_logic (pure rules),
                         replay, customer_changes, seed, verify
scripts/                 seed_reference.py, replay.py, verify_replay.py
tests/                   unit tests; tests/integration needs Postgres
```
