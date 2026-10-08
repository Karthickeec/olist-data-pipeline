from datetime import date

import pytest
from pyspark.sql import functions as F

from olist_pipeline.dq import checks as C
from olist_pipeline.dq.engine import Runner
from olist_pipeline.dq.suite import Check, DQConfigError, load_suite

D = date(2017, 3, 1)
PREV = date(2017, 2, 28)


def write_yaml(tmp_path, text: str):
    path = tmp_path / "suite.yaml"
    path.write_text(text)
    return path


@pytest.fixture
def lake(spark, tmp_path):
    """silver.items: one clean batch (PREV) and one dirty batch (D); silver.parents: the keys."""
    root = str(tmp_path / "lake")
    items = spark.createDataFrame([
        ("a", "p1", 10.0, "SP", PREV), ("b", "p1", 20.0, "RJ", PREV),
        ("c", "p1", 5.0, "SP", D),
        ("c", "p2", 7.0, "SP", D),          # duplicate id
        ("d", None, 3.0, "XX", D),           # null parent, bad state
        ("e", "p9", -1.0, "MG", D),          # orphan parent, negative price
    ], "id string, parent string, price double, state string, _batch_date date")
    items.write.parquet(f"{root}/silver/items")
    (spark.createDataFrame([("p1",), ("p2",)], "parent_id string")
     .withColumn("_batch_date", F.lit(PREV).cast("date"))
     .write.parquet(f"{root}/silver/parents"))
    return root


SUITE = """
defaults: {batch_column: _batch_date, scope: batch}
tables:
  silver.items:
    key: [id]
    checks:
      - {type: not_null, columns: [parent], severity: error}
      - {type: unique, columns: [id], severity: error}
      - {type: accepted_values, column: state, values: [SP, RJ, MG], severity: error}
      - {type: range, column: price, min: 0, max: 100, severity: warn}
      - {type: relationship, column: parent, ref: silver.parents.parent_id, severity: error}
      - {type: schema, columns: {id: string, price: double, state: string}, severity: error}
      - {type: expression, name: price_positive, expr: "price > 0", severity: error}
      - {type: row_count_vs_previous, min_ratio: 0.5, max_ratio: 1.5, severity: warn}
      - {type: unique, name: unique_all_time, columns: [id], scope: table, severity: error}
  silver.missing:
    optional: true
    checks:
      - {type: unique, columns: [id]}
  silver.also_missing:
    checks:
      - {type: unique, columns: [id]}
"""


def run(spark, root, tmp_path, day=D, text=SUITE):
    suite = load_suite("silver", write_yaml(tmp_path, text))
    return {(r.table, r.check.name): r for r in Runner(spark, root, suite, day).run()}


def test_each_check_fails_on_the_dirty_batch(spark, lake, tmp_path):
    r = run(spark, lake, tmp_path)
    t = "silver.items"
    assert (r[t, "not_null:parent"].status, r[t, "not_null:parent"].outcome.failed_rows) == ("fail", 1)
    assert r[t, "not_null:parent"].outcome.sample == [{"id": "d"}]
    assert (r[t, "unique:id"].status, r[t, "unique:id"].outcome.failed_rows) == ("fail", 2)
    assert r[t, "unique:id"].outcome.sample == [{"id": "c", "count": 2}]
    assert (r[t, "accepted_values:state"].status, r[t, "accepted_values:state"].outcome.failed_rows) == ("fail", 1)
    assert (r[t, "range:price"].status, r[t, "range:price"].outcome.failed_rows) == ("warn", 1)
    rel = r[t, "relationship:parent->silver.parents.parent_id"]
    assert (rel.status, rel.outcome.failed_rows, rel.outcome.sample) == ("fail", 1, [{"id": "e", "parent": "p9"}])
    assert r[t, "schema:columns"].status == "pass"
    assert (r[t, "price_positive"].status, r[t, "price_positive"].outcome.failed_rows) == ("fail", 1)
    assert r[t, "row_count_vs_previous"].outcome.observed == 4      # no conn: no baseline
    assert r[t, "unique_all_time"].status == "fail"
    assert r["silver.missing", "unique:id"].status == "skipped"
    assert r["silver.also_missing", "unique:id"].status == "error_running"


def test_clean_batch_passes(spark, lake, tmp_path):
    r = run(spark, lake, tmp_path, day=PREV)
    batch_checks = {k: v for k, v in r.items() if k[0] == "silver.items" and k[1] != "unique_all_time"}
    assert {v.status for v in batch_checks.values()} == {"pass"}


def test_schema_mismatch_and_missing_column(spark, lake, tmp_path):
    text = """
tables:
  silver.items:
    checks:
      - {type: schema, columns: {id: string, price: int, extra: string}, severity: error}
      - {type: not_null, columns: [no_such_column], severity: warn}
"""
    r = run(spark, lake, tmp_path, text=text)
    schema = r["silver.items", "schema:columns"]
    assert schema.status == "fail" and schema.outcome.failed_rows == 2
    assert {"column": "price", "expected": "int", "actual": "double"} in schema.outcome.sample
    missing = r["silver.items", "not_null:no_such_column"]
    assert missing.status == "error_running" and "no_such_column" in missing.outcome.message
    assert missing.blocking                       # even a warn check that cannot run blocks the run


def test_row_count_vs_previous():
    check = Check("row_count_vs_previous", {"min_ratio": 0.5, "max_ratio": 2}, "warn", None, "rc")

    class Df:
        def __init__(self, n): self.n = n
        def count(self): return self.n

    assert C.row_count_vs_previous(Df(10), check, None).failed is False
    assert C.row_count_vs_previous(Df(10), check, 8).failed is False
    assert C.row_count_vs_previous(Df(10), check, 100).failed is True
    assert "ratio 0.10" in C.row_count_vs_previous(Df(10), check, 100).message


@pytest.mark.parametrize("text, message", [
    ("tables: {t: {checks: [{type: not_a_check}]}}", "unknown type 'not_a_check'"),
    ("tables: {t: {checks: [{type: unique}]}}", "missing ['columns']"),
    ("tables: {t: {checks: [{type: unique, columns: [a], colums: [b]}]}}", "unknown parameters ['colums']"),
    ("tables: {t: {checks: [{type: unique, columns: [a], severity: fatal}]}}", "severity must be error or warn"),
    ("tables: {t: {checks: [{type: range, column: a}]}}", "needs min and/or max"),
    ("tables: {t: {scope: weekly, checks: []}}", "unknown scope 'weekly'"),
    ("tables: {t: {checks: [{type: unique, columns: [a]}, {type: unique, columns: [a]}]}}", "duplicate check name"),
])
def test_invalid_suites_are_rejected(tmp_path, text, message):
    with pytest.raises(DQConfigError, match=message.replace("[", r"\[").replace("]", r"\]")):
        load_suite("silver", write_yaml(tmp_path, text))


def test_real_suites_load():
    for layer in ("bronze", "silver"):
        suite = load_suite(layer)
        assert suite.tables and all(t.checks for t in suite.tables)
