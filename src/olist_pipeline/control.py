"""Control plane for Spark jobs that run on AWS Glue, which can't reach the local Postgres.

Before a Glue job: `export_state` reads the little bookkeeping the job needs (latest layer run and
latest DQ observation per table before the batch date) into JSON. The job uses a `FileBook`, the
same interface as watermarks.Bookkeeping, and writes what it would have recorded to JSON. After the
job: `import_outputs` writes those records into Postgres. The job code itself doesn't change.
"""
from dataclasses import asdict
from datetime import date

from psycopg import sql

from olist_pipeline.watermarks import Bookkeeping, IngestRange, LayerRun


def _d(value: str | None) -> date | None:
    return date.fromisoformat(value) if value else None


def _iso(value) -> str | None:
    return value.isoformat() if value is not None else None


class FileBook:
    """Bookkeeping from an exported state dict; records are kept for export instead of written."""

    schema = "pipeline"

    def __init__(self, state: dict):
        self.batch_date = _d(state["batch_date"])
        self.layer_state = state.get("layer_runs", {})
        self.dq_state = state.get("dq_observed", {})
        self.layer_runs: list[dict] = []
        self.dq_results: list[dict] = []

    def ensure_schema(self) -> None:
        pass

    def _check_before(self, before: date) -> None:
        # The state holds the latest rows before the exported batch date only.
        if before != self.batch_date:
            raise ValueError(f"state was exported for {self.batch_date}, asked about {before}")

    def last_layer_run(self, layer: str, table: str, before: date) -> tuple[date, date | None] | None:
        self._check_before(before)
        row = self.layer_state.get(layer, {}).get(table)
        return (_d(row[0]), _d(row[1])) if row else None

    def record_layer(self, layer: str, table: str, batch_date: date, rng: IngestRange, run: LayerRun) -> None:
        self.layer_runs.append({"layer": layer, "table": table, "batch_date": _iso(batch_date),
                                "low": _iso(rng.low), "high": _iso(rng.high), "run": asdict(run)})

    def previous_observed(self, layer: str, table: str, check_name: str, before: date) -> float | None:
        self._check_before(before)
        value = self.dq_state.get(layer, {}).get(table, {}).get(check_name)
        return float(value) if value is not None else None

    def record_dq(self, layer: str, batch_date: date, rows: list[tuple], tables: set[str]) -> None:
        self.dq_results.append({"layer": layer, "batch_date": _iso(batch_date), "tables": sorted(tables),
                                "rows": [list(r) for r in rows]})

    def outputs(self) -> dict:
        return {"batch_date": _iso(self.batch_date), "layer_runs": self.layer_runs, "dq_results": self.dq_results}


def export_state(book: Bookkeeping, batch_date: date) -> dict:
    """Latest layer run per (layer, table) and latest observed DQ value per check, before batch_date."""
    layer_runs: dict = {}
    for layer, table, bd, to_ingest in book.conn.execute(sql.SQL(
            "SELECT DISTINCT ON (layer, table_name) layer, table_name, batch_date, to_ingest_date FROM {} "
            "WHERE batch_date < %s ORDER BY layer, table_name, batch_date DESC").format(book._t("layer_runs")),
            (batch_date,)):
        layer_runs.setdefault(layer, {})[table] = [_iso(bd), _iso(to_ingest)]
    dq: dict = {}
    for layer, table, check, observed in book.conn.execute(sql.SQL(
            "SELECT DISTINCT ON (layer, table_name, check_name) layer, table_name, check_name, observed FROM {} "
            "WHERE batch_date < %s AND observed IS NOT NULL "
            "ORDER BY layer, table_name, check_name, batch_date DESC").format(book._t("dq_results")),
            (batch_date,)):
        dq.setdefault(layer, {}).setdefault(table, {})[check] = float(observed)
    return {"batch_date": _iso(batch_date), "layer_runs": layer_runs, "dq_observed": dq}


def import_outputs(book: Bookkeeping, outputs: dict) -> tuple[int, int]:
    """Write a Glue job's records into Postgres. Returns (layer runs, DQ results) written."""
    for r in outputs["layer_runs"]:
        book.record_layer(r["layer"], r["table"], _d(r["batch_date"]),
                          IngestRange(_d(r["low"]), _d(r["high"])), LayerRun(**r["run"]))
    n_dq = 0
    for d in outputs["dq_results"]:
        book.record_dq(d["layer"], _d(d["batch_date"]), [tuple(r) for r in d["rows"]], set(d["tables"]))
        n_dq += len(d["rows"])
    return len(outputs["layer_runs"]), n_dq
