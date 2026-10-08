-- One row per (layer, table, batch). A batch processes the Bronze partitions with
-- ingest_date in (from_ingest_date, to_ingest_date]; a rerun of D reuses D's range:
-- from = to_ingest_date of the latest run before D.
CREATE TABLE IF NOT EXISTS layer_runs (
    layer            text NOT NULL,
    table_name       text NOT NULL,
    batch_date       date NOT NULL,
    from_ingest_date date,              -- exclusive; NULL = from the beginning
    to_ingest_date   date NOT NULL,     -- inclusive
    rows_in          bigint NOT NULL,   -- Bronze rows read
    rows_valid       bigint NOT NULL,   -- passed cleaning (before dedup)
    rows_quarantined bigint NOT NULL,
    rows_duplicate   bigint NOT NULL,   -- superseded versions / exact duplicates in the batch
    rows_written     bigint NOT NULL,   -- rows in the Silver partitions rewritten by the merge
    detail           text,
    finished_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (layer, table_name, batch_date)
);

-- Late-arriving rows (e.g. an item whose order lands in a later batch) wait in
-- silver/_pending and are retried each batch before being quarantined.
ALTER TABLE layer_runs ADD COLUMN IF NOT EXISTS rows_pending bigint NOT NULL DEFAULT 0;
