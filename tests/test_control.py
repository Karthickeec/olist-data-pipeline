"""Control plane for Glue jobs: FileBook gives the same answers as Postgres, and its records round-trip."""
import json
from datetime import date

import psycopg
import pytest

from olist_pipeline.config import load_config
from olist_pipeline.control import FileBook, export_state, import_outputs
from olist_pipeline.db import connect
from olist_pipeline.watermarks import Bookkeeping, IngestRange, LayerRun, layer_range, record_layer_run

D = date(2018, 1, 10)
STATE = {"batch_date": "2018-01-10",
         "layer_runs": {"silver": {"orders": ["2018-01-09", "2018-01-09"]}},
         "dq_observed": {"silver": {"silver.orders": {"row_count_vs_previous": 47358.0}}}}


def test_filebook_answers_from_state():
    book = FileBook(STATE)
    assert book.last_layer_run("silver", "orders", D) == (date(2018, 1, 9), date(2018, 1, 9))
    assert book.last_layer_run("silver", "customers", D) is None
    assert layer_range(book, "silver", "orders", D) == IngestRange(date(2018, 1, 9), D)
    assert book.previous_observed("silver", "silver.orders", "row_count_vs_previous", D) == 47358.0
    assert book.previous_observed("gold", "gold.x", "c", D) is None
    # The state only describes the batch it was exported for.
    with pytest.raises(ValueError):
        book.last_layer_run("silver", "orders", date(2018, 1, 11))


def test_filebook_records_are_json():
    book = FileBook(STATE)
    record_layer_run(book, "silver", "orders", D, IngestRange(date(2018, 1, 9), D),
                     LayerRun(rows_in=5, rows_valid=4, rows_quarantined=1, rows_written=4, detail="x"))
    book.record_dq("silver", D, [("silver.orders", "unique:order_id", "unique", "error", "pass", 0, None, None, None)],
                   {"silver.orders"})
    out = json.loads(json.dumps(book.outputs()))
    assert out["layer_runs"][0]["run"]["rows_quarantined"] == 1 and out["layer_runs"][0]["low"] == "2018-01-09"
    assert out["dq_results"][0]["rows"][0][1] == "unique:order_id"


@pytest.mark.integration
def test_export_import_round_trip():
    """Postgres -> state -> FileBook -> outputs -> Postgres gives what the job would have written directly."""
    schema = "pipeline_control_test"
    try:
        conn = connect(load_config()["pg"])
    except psycopg.OperationalError as e:
        pytest.skip(f"Postgres not reachable: {e}")
    conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    try:
        pg = Bookkeeping(conn, schema)
        pg.ensure_schema()
        for d, to in ((date(2018, 1, 8), date(2018, 1, 8)), (date(2018, 1, 9), date(2018, 1, 9))):
            pg.record_layer("silver", "orders", d, IngestRange(None, to), LayerRun(rows_written=1))
        pg.record_dq("silver", date(2018, 1, 9), [("silver.orders", "rows", "row_count_vs_previous", "warn", "pass",
                                                   0, 100.0, None, None)], {"silver.orders"})
        state = json.loads(json.dumps(export_state(pg, D)))
        file_book = FileBook(state)
        assert file_book.last_layer_run("silver", "orders", D) == pg.last_layer_run("silver", "orders", D)
        assert file_book.previous_observed("silver", "silver.orders", "rows", D) == \
            pg.previous_observed("silver", "silver.orders", "rows", D) == 100.0

        record_layer_run(file_book, "silver", "orders", D, layer_range(file_book, "silver", "orders", D),
                         LayerRun(rows_in=3, rows_written=3))
        file_book.record_dq("silver", D, [("silver.orders", "rows", "row_count_vs_previous", "warn", "pass",
                                           0, 103.0, None, None)], {"silver.orders"})
        assert import_outputs(pg, json.loads(json.dumps(file_book.outputs()))) == (1, 1)
        row = conn.execute(f"SELECT from_ingest_date, to_ingest_date, rows_written FROM {schema}.layer_runs "
                           "WHERE batch_date = %s", (D,)).fetchone()
        assert row == (date(2018, 1, 9), D, 3)
        assert pg.previous_observed("silver", "silver.orders", "rows", date(2018, 1, 11)) == 103.0
    finally:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.close()
