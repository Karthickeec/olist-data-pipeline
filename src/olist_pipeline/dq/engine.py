"""Run a DQ suite for one layer and batch date; record results; fail on error-severity checks.

Table names map to lake paths: bronze.<source>.<table>, silver.<table>, gold.<table>.
Scopes: batch (rows of this batch), latest (the newest Bronze partition on or before the
batch, for snapshot tables), table (everything up to the batch).
"""
import argparse
import json
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame, SparkSession
from pyspark.errors import AnalysisException
from pyspark.sql import functions as F

from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.dq import checks as C
from olist_pipeline.dq.suite import Check, Suite, TableChecks, load_suite
from olist_pipeline.lake import path_exists, table_path
from olist_pipeline.spark import build_spark
from olist_pipeline.watermarks import Bookkeeping


@dataclass
class Result:
    table: str
    check: Check
    status: str                     # pass | fail | warn | error_running | skipped
    outcome: C.CheckOutcome

    @property
    def blocking(self) -> bool:
        return self.status in ("fail", "error_running")


def lake_path(root: str, name: str) -> str:
    parts = name.split(".")
    if parts[0] == "bronze" and len(parts) == 3:
        return table_path(root, "bronze", parts[1], parts[2])
    if parts[0] in ("silver", "gold") and len(parts) == 2:
        return f"{root}/{parts[0]}/{parts[1]}"
    raise ValueError(f"cannot map table name {name!r} to the lake (bronze.<source>.<table>, silver.<t>, gold.<t>)")


class Runner:
    def __init__(self, spark: SparkSession, root: str, suite: Suite, batch_date: date,
                 conn: psycopg.Connection | None = None, pipeline_schema: str = "pipeline"):
        self.spark, self.root, self.suite, self.batch_date = spark, root, suite, batch_date
        self.conn, self.pipeline_schema = conn, pipeline_schema
        self.tables: dict[str, DataFrame | None] = {}

    def table(self, name: str) -> DataFrame | None:
        """The whole lake table, or None if it does not exist (or has no data files yet)."""
        if name not in self.tables:
            path = lake_path(self.root, name)
            df = None
            if path_exists(self.spark, path):
                try:
                    df = self.spark.read.parquet(path)
                except AnalysisException:
                    df = None
            self.tables[name] = df
        return self.tables[name]

    def scoped(self, df: DataFrame, scope: str) -> DataFrame:
        col = self.suite.batch_column
        day = F.lit(self.batch_date.isoformat()).cast("date")
        if col not in df.columns:
            if scope == "table":            # e.g. dimensions rebuilt whole: no batch column
                return df
            raise C.ColumnMissing(f"batch column {col!r} not in table")
        if scope == "batch":
            return df.filter(F.col(col) == day)
        if scope == "latest":
            latest = df.filter(F.col(col) <= day).agg(F.max(col)).first()[0]
            return df.filter(F.col(col) == F.lit(latest)) if latest else df.limit(0)
        return df.filter(F.col(col) <= day)

    def previous_observed(self, table: str, check: Check) -> float | None:
        if self.conn is None:
            return None
        row = self.conn.execute(sql.SQL(
            "SELECT observed FROM {} WHERE layer = %s AND table_name = %s AND check_name = %s "
            "AND batch_date < %s AND observed IS NOT NULL ORDER BY batch_date DESC LIMIT 1"
        ).format(sql.Identifier(self.pipeline_schema, "dq_results")),
            (self.suite.layer, table, check.name, self.batch_date)).fetchone()
        return float(row[0]) if row else None

    def run_check(self, tc: TableChecks, df: DataFrame, check: Check) -> C.CheckOutcome:
        scoped = self.scoped(df, check.scope or tc.scope)
        if check.get("where"):
            scoped = scoped.filter(check["where"])
        key = list(tc.key)
        if check.type == "row_count_vs_previous":
            return C.row_count_vs_previous(scoped, check, self.previous_observed(tc.name, check))
        if check.type == "relationship":
            ref_table, ref_col = check["ref"].rsplit(".", 1)
            parent = self.table(ref_table)
            if parent is None:
                raise C.ColumnMissing(f"referenced table {ref_table} does not exist")
            if check.get("ref_where"):
                parent = parent.filter(check["ref_where"])
            return C.relationship(scoped, check, key, parent, ref_col)
        return C.ROW_CHECKS[check.type](scoped, check, key)

    def run(self, only: set[str] | None = None) -> list[Result]:
        results = []
        for tc in self.suite.tables:
            if only and tc.name not in only:
                continue
            df = self.table(tc.name)
            if df is None:
                status = "skipped" if tc.optional else "error_running"
                for check in tc.checks:
                    results.append(Result(tc.name, check, status, C.CheckOutcome(message="table does not exist")))
                continue
            df = df.cache()
            for check in tc.checks:
                try:
                    outcome = self.run_check(tc, df, check)
                except Exception as e:  # noqa: BLE001 - recorded as error_running, which fails the run
                    results.append(Result(tc.name, check, "error_running",
                                          C.CheckOutcome(message=f"{type(e).__name__}: {str(e).splitlines()[0]}")))
                    continue
                failed = outcome.failed if outcome.failed is not None else outcome.failed_rows > 0
                status = "pass" if not failed else ("fail" if check.severity == "error" else "warn")
                results.append(Result(tc.name, check, status, outcome))
            df.unpersist()
        return results


def record(conn: psycopg.Connection, schema: str, layer: str, batch_date: date, results: list[Result],
           tables: set[str]) -> None:
    """Replace this batch's results for the tables that ran."""
    t = sql.Identifier(schema, "dq_results")
    with conn.transaction():
        conn.execute(sql.SQL("DELETE FROM {} WHERE batch_date = %s AND layer = %s AND table_name = ANY(%s)")
                     .format(t), (batch_date, layer, sorted(tables)))
        with conn.cursor() as cur:
            cur.executemany(sql.SQL(
                "INSERT INTO {} (batch_date, layer, table_name, check_name, check_type, severity, status, "
                "failed_rows, observed, sample, message) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            ).format(t), [(batch_date, layer, r.table, r.check.name, r.check.type, r.check.severity, r.status,
                           r.outcome.failed_rows, r.outcome.observed,
                           json.dumps(r.outcome.sample, default=str) if r.outcome.sample else None,
                           r.outcome.message or None) for r in results])


def report(results: list[Result], layer: str, batch_date: date) -> None:
    print(f"DQ {layer} {batch_date}")
    for r in results:
        detail = r.outcome.message or (f"{r.outcome.failed_rows} failing row(s)" if r.outcome.failed_rows else "")
        if r.outcome.sample and r.status != "pass":
            detail += f"  e.g. {json.dumps(r.outcome.sample[0], default=str)[:160]}"
        print(f"  {r.status.upper():<13} {r.check.severity:<5} {r.table:<36} {r.check.name:<48} {detail}")
    counts = {s: sum(r.status == s for r in results) for s in ("pass", "warn", "fail", "error_running", "skipped")}
    blocking = sum(r.blocking for r in results)
    print(f"DQ {layer} {batch_date}: {len(results)} checks, "
          + ", ".join(f"{k}={v}" for k, v in counts.items() if v)
          + (f" -> FAILED ({blocking} blocking)" if blocking else " -> OK"), flush=True)


def run_layer(spark: SparkSession, cfg: dict, layer: str, batch_date: date, suite: Suite | None = None,
              only: set[str] | None = None) -> list[Result]:
    suite = suite or load_suite(layer)
    with connect(cfg["pg"]) as conn:
        book = Bookkeeping(conn, cfg["pg"]["pipeline_schema"])
        book.ensure_schema()
        results = Runner(spark, cfg["lake"]["root"], suite, batch_date, conn, book.schema).run(only)
        record(conn, book.schema, layer, batch_date, results, {r.table for r in results})
    report(results, layer, batch_date)
    return results


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Run the data-quality checks for one layer and batch date(s).")
    p.add_argument("--layer", choices=("bronze", "silver", "gold"), required=True)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=date.fromisoformat)
    g.add_argument("--start", type=date.fromisoformat, help="with --end: every day of a range, one Spark session")
    p.add_argument("--end", type=date.fromisoformat)
    p.add_argument("--table", action="append", help="only these tables (repeatable)")
    p.add_argument("--suite", type=Path, help="suite YAML (default: config/dq/<layer>.yaml)")
    args = p.parse_args(argv)
    if (args.start is None) != (args.end is None):
        p.error("--start and --end go together")
    days = [args.date] if args.date else [args.start + timedelta(days=i)
                                          for i in range((args.end - args.start).days + 1)]
    cfg = load_config()
    spark = build_spark(cfg, f"dq_{args.layer}")
    suite = load_suite(args.layer, args.suite)
    blocking = False
    try:
        for day in days:
            results = run_layer(spark, cfg, args.layer, day, suite, only=set(args.table) if args.table else None)
            blocking |= any(r.blocking for r in results)
    finally:
        spark.stop()
    sys.exit(1 if blocking else 0)


if __name__ == "__main__":
    main()
