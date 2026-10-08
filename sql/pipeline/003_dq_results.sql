-- One row per data-quality check per batch. A rerun of (batch_date, layer) replaces
-- that batch's rows, so the table always shows the latest verdict.
CREATE TABLE IF NOT EXISTS dq_results (
    batch_date  date NOT NULL,
    layer       text NOT NULL,          -- bronze | silver | gold
    table_name  text NOT NULL,          -- e.g. silver.orders
    check_name  text NOT NULL,
    check_type  text NOT NULL,
    severity    text NOT NULL,          -- error | warn
    status      text NOT NULL,          -- pass | fail | warn | error_running | skipped
    failed_rows bigint NOT NULL DEFAULT 0,
    observed    numeric,                -- e.g. the row count for row_count_vs_previous
    sample      jsonb,                  -- up to 5 failing rows
    message     text,
    checked_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_date, layer, table_name, check_name)
);
