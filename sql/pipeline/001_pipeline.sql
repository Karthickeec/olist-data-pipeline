-- Pipeline bookkeeping (schema from pg.pipeline_schema, set via search_path by
-- db.apply_schema). Bookkeeping times are wall clock; watermarks are source time.

-- Current high-water mark per incrementally extracted table. Only moves forward.
CREATE TABLE IF NOT EXISTS watermarks (
    source      text NOT NULL,
    table_name  text NOT NULL,
    watermark   timestamp NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, table_name)
);

-- One row per successful incremental batch. A rerun of batch D reuses its
-- window: low = high_wm of the latest successful batch before D.
CREATE TABLE IF NOT EXISTS ingest_runs (
    source      text NOT NULL,
    table_name  text NOT NULL,
    batch_date  date NOT NULL,
    low_wm      timestamp,          -- exclusive; NULL on the initial load
    high_wm     timestamp NOT NULL, -- inclusive
    row_count   bigint NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, table_name, batch_date)
);

-- One row per reference snapshot written to Bronze. A batch writes a new
-- snapshot only when the content hash differs from the latest one on/before it.
CREATE TABLE IF NOT EXISTS reference_snapshots (
    source       text NOT NULL,
    table_name   text NOT NULL,
    batch_date   date NOT NULL,
    content_hash text NOT NULL,
    row_count    bigint NOT NULL,
    written_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, table_name, batch_date)
);
