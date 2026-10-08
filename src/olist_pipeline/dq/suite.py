"""Load and validate a DQ suite (config/dq/<layer>.yaml)."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from olist_pipeline.config import PROJECT_ROOT

DQ_DIR = PROJECT_ROOT / "config" / "dq"
SEVERITIES = ("error", "warn")
SCOPES = ("batch", "table", "latest")
# Required parameters per check type; anything else must be in OPTIONAL.
REQUIRED = {
    "not_null": ("columns",),
    "unique": ("columns",),
    "accepted_values": ("column", "values"),
    "range": ("column",),
    "row_count_vs_previous": (),
    "schema": ("columns",),
    "relationship": ("column", "ref"),
    "expression": ("expr",),
}
OPTIONAL = {
    "name",
    "severity",
    "scope",
    "where",
    "min",
    "max",
    "min_ratio",
    "max_ratio",
    "allow_null",
    "allow_extra",
    "ref_where",
    "description",
}


class DQConfigError(ValueError):
    """The suite file is invalid; raised before anything runs."""


@dataclass(frozen=True)
class Check:
    type: str
    params: dict
    severity: str
    scope: str | None
    name: str

    def __getitem__(self, key):
        return self.params[key]

    def get(self, key, default=None):
        return self.params.get(key, default)


@dataclass(frozen=True)
class TableChecks:
    name: str  # e.g. silver.orders, bronze.olist_postgres.orders
    key: tuple[str, ...]
    scope: str
    optional: bool
    checks: tuple[Check, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Suite:
    layer: str
    batch_column: str  # column that marks the batch: ingest_date (Bronze) or _batch_date
    tables: tuple[TableChecks, ...]


def _check_name(spec: dict) -> str:
    if "name" in spec:
        return spec["name"]
    target = spec.get("columns") or spec.get("column") or spec.get("expr") or ""
    if isinstance(target, dict):
        target = "columns"
    elif isinstance(target, list):
        target = ",".join(target)
    if spec["type"] == "relationship":
        target = f"{target}->{spec['ref']}"
    return f"{spec['type']}:{target}" if target else spec["type"]


def load_suite(layer: str, path: Path | None = None) -> Suite:
    path = path or DQ_DIR / f"{layer}.yaml"
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    defaults = raw.get("defaults", {})
    batch_column = defaults.get("batch_column", "_batch_date")
    default_scope = defaults.get("scope", "batch")
    tables = []
    for tname, tspec in (raw.get("tables") or {}).items():
        where = f"{path.name}: {tname}"
        scope = tspec.get("scope", default_scope)
        if scope not in SCOPES:
            raise DQConfigError(f"{where}: unknown scope {scope!r} (expected one of {SCOPES})")
        checks, names = [], set()
        for i, spec in enumerate(tspec.get("checks") or []):
            ctype = spec.get("type")
            if ctype not in REQUIRED:
                raise DQConfigError(
                    f"{where}: check #{i + 1} has unknown type {ctype!r} (expected one of {sorted(REQUIRED)})"
                )
            missing = [p for p in REQUIRED[ctype] if p not in spec]
            unknown = set(spec) - set(REQUIRED[ctype]) - OPTIONAL - {"type"}
            if missing or unknown:
                raise DQConfigError(
                    f"{where}: {ctype} check #{i + 1}: "
                    + (f"missing {missing} " if missing else "")
                    + (f"unknown parameters {sorted(unknown)}" if unknown else "")
                )
            severity = spec.get("severity", "error")
            if severity not in SEVERITIES:
                raise DQConfigError(f"{where}: {ctype} check #{i + 1}: severity must be error or warn")
            if spec.get("scope", scope) not in SCOPES:
                raise DQConfigError(f"{where}: {ctype} check #{i + 1}: unknown scope {spec['scope']!r}")
            if ctype == "range" and "min" not in spec and "max" not in spec:
                raise DQConfigError(f"{where}: range check #{i + 1} needs min and/or max")
            name = _check_name(spec)
            if name in names:
                raise DQConfigError(f"{where}: duplicate check name {name!r}; give one of them a 'name'")
            names.add(name)
            params = {k: v for k, v in spec.items() if k not in ("type", "severity", "scope", "name")}
            checks.append(Check(ctype, params, severity, spec.get("scope"), name))
        tables.append(
            TableChecks(tname, tuple(tspec.get("key", ())), scope, bool(tspec.get("optional")), tuple(checks))
        )
    return Suite(layer, batch_column, tuple(tables))
