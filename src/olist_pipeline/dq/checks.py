"""Check implementations. Each takes the scoped DataFrame and returns a CheckOutcome."""
import json
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from olist_pipeline.dq.suite import Check

SAMPLE_ROWS = 5


@dataclass
class CheckOutcome:
    failed_rows: int = 0
    observed: float | None = None
    sample: list = field(default_factory=list)
    message: str = ""
    failed: bool | None = None      # set explicitly by checks that are not row based


class ColumnMissing(Exception):
    pass


def require_columns(df: DataFrame, columns) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ColumnMissing(f"column(s) {missing} not in table (has {sorted(df.columns)})")


def sample_of(failing: DataFrame, columns: list[str]) -> list[dict]:
    cols = [c for c in dict.fromkeys(columns) if c in failing.columns]
    rows = failing.select(F.to_json(F.struct(*cols)).alias("j")).limit(SAMPLE_ROWS).collect()
    return [json.loads(r.j) for r in rows]


def failing_rows(df: DataFrame, condition, columns: list[str]) -> CheckOutcome:
    """Rows matching `condition` fail the check."""
    failing = df.filter(condition)
    n = failing.count()
    return CheckOutcome(failed_rows=n, sample=sample_of(failing, columns) if n else [])


def not_null(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    cols = check["columns"]
    require_columns(df, cols)
    cond = F.lit(False)
    for c in cols:
        cond = cond | F.col(c).isNull()
    return failing_rows(df, cond, key + cols)


def unique(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    cols = check["columns"]
    require_columns(df, cols)
    dupes = df.groupBy(*cols).count().filter("count > 1")
    groups = dupes.count()
    if not groups:
        return CheckOutcome()
    rows = dupes.agg(F.sum("count")).first()[0]
    sample = [json.loads(r.j) for r in dupes.select(F.to_json(F.struct(*cols, "count")).alias("j"))
              .orderBy(F.col("count").desc()).limit(SAMPLE_ROWS).collect()]
    return CheckOutcome(failed_rows=rows, sample=sample, message=f"{groups} duplicated key(s)")


def accepted_values(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    col = check["column"]
    require_columns(df, [col])
    bad = ~F.col(col).isin(*check["values"])
    if not check.get("allow_null", True):
        bad = bad | F.col(col).isNull()
    return failing_rows(df, F.coalesce(bad, F.lit(False)), key + [col])


def range_check(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    col = check["column"]
    require_columns(df, [col])
    cond = F.lit(False)
    if "min" in check.params:
        cond = cond | (F.col(col) < F.lit(check["min"]))
    if "max" in check.params:
        cond = cond | (F.col(col) > F.lit(check["max"]))
    return failing_rows(df, F.coalesce(cond, F.lit(False)), key + [col])


def expression(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    """Every row must satisfy the SQL expression (null counts as a failure)."""
    return failing_rows(df, ~F.coalesce(F.expr(check["expr"]), F.lit(False)), key + list(df.columns[:8]))


def schema(df: DataFrame, check: Check, key: list[str]) -> CheckOutcome:
    actual = dict(df.dtypes)
    problems = []
    for col, expected in check["columns"].items():
        if col not in actual:
            problems.append({"column": col, "expected": expected, "actual": None})
        elif actual[col] != expected:
            problems.append({"column": col, "expected": expected, "actual": actual[col]})
    if not check.get("allow_extra", True):
        problems += [{"column": c, "expected": None, "actual": t} for c, t in actual.items()
                     if c not in check["columns"]]
    return CheckOutcome(failed_rows=len(problems), sample=problems[:SAMPLE_ROWS],
                        message=f"{len(problems)} column problem(s)" if problems else "")


def relationship(df: DataFrame, check: Check, key: list[str], parent: DataFrame, ref_column: str) -> CheckOutcome:
    """Every non-null `column` must exist in the parent's `ref_column` (left anti-join)."""
    col = check["column"]
    require_columns(df, [col])
    require_columns(parent, [ref_column])
    keys = parent.select(F.col(ref_column).alias(col)).distinct()
    orphans = df.filter(F.col(col).isNotNull()).join(keys, col, "left_anti")
    n = orphans.count()
    return CheckOutcome(failed_rows=n, sample=sample_of(orphans, key + [col]) if n else [])


def row_count_vs_previous(df: DataFrame, check: Check, previous: float | None) -> CheckOutcome:
    n = df.count()
    lo, hi = check.get("min_ratio", 0.0), check.get("max_ratio", float("inf"))
    if not previous:
        return CheckOutcome(observed=n, failed=False, message="no previous batch to compare with")
    ratio = n / previous
    return CheckOutcome(observed=n, failed=not (lo <= ratio <= hi),
                        message=f"{n} rows vs {int(previous)} in the previous batch (ratio {ratio:.2f}, "
                                f"allowed {lo}-{hi})")


ROW_CHECKS = {
    "not_null": not_null,
    "unique": unique,
    "accepted_values": accepted_values,
    "range": range_check,
    "expression": expression,
    "schema": schema,
}
