"""Pipeline configuration: config/pipeline.yaml with environment variable overrides.

OLIST_TARGET=aws moves the lake to S3 and takes the Postgres password and API key from
Secrets Manager (see olist_pipeline.aws); the default target is the local machine.
"""
import os
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "pipeline.yaml"
ENV_PREFIX = "OLIST"
TARGET_ENV = "OLIST_TARGET"


def load_config(path: Path = DEFAULT_CONFIG, environ: Mapping[str, str] = os.environ) -> dict:
    """Read the YAML config, apply OLIST__SECTION__KEY overrides and resolve paths."""
    with open(path, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    _apply_env(cfg, environ, ENV_PREFIX)
    if environ.get(TARGET_ENV, "local") == "aws":
        from olist_pipeline.aws import apply_aws_target
        apply_aws_target(cfg)
    if _has_secret_refs(cfg):
        from olist_pipeline.aws import resolve_secrets
        resolve_secrets(cfg, cfg["aws"]["region"])
    cfg["paths"] = {k: _resolve(v) for k, v in cfg["paths"].items()}
    cfg["lake"]["root"] = resolve_uri(cfg["lake"]["root"])
    cfg["spark"]["java_home"] = str(Path(cfg["spark"]["java_home"]).expanduser())
    return cfg


def _apply_env(node: dict, environ: Mapping[str, str], prefix: str) -> None:
    for key, value in node.items():
        name = f"{prefix}__{key.upper()}"
        if isinstance(value, dict):
            _apply_env(value, environ, name)
        elif name in environ:
            node[key] = _coerce(environ[name], value)


def _has_secret_refs(node: dict) -> bool:
    return any(_has_secret_refs(v) if isinstance(v, dict) else isinstance(v, str) and v.startswith("secret://")
               for v in node.values())


def _coerce(raw: str, default):
    """Convert an env var string to the type of the YAML default it replaces."""
    if isinstance(default, bool):
        return raw.strip().lower() in ("1", "true", "yes", "on")
    if isinstance(default, int):
        return int(raw)
    if isinstance(default, float):
        return float(raw)
    if isinstance(default, date):
        return date.fromisoformat(raw)
    return raw


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else PROJECT_ROOT / path


def resolve_uri(root: str) -> str:
    """Keep URIs (s3://...) as-is; make local paths absolute. No trailing slash."""
    root = root.rstrip("/")
    return root if "://" in root else str(_resolve(root))
