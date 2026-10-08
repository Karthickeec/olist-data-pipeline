"""uvicorn entry point: `uvicorn olist_pipeline.api.server:app_from_config --factory`."""

import os

from fastapi import FastAPI

from olist_pipeline.api.activity import build_customer_index
from olist_pipeline.api.app import ServerSettings, create_app
from olist_pipeline.config import load_config


def app_from_config() -> FastAPI:
    cfg = load_config()
    key_env = cfg["api"]["key_env"]
    api_key = os.environ.get(key_env)
    if not api_key:
        raise SystemExit(f"{key_env} is not set; refusing to start without an API key")
    index = build_customer_index(cfg["paths"]["raw_dir"])
    return create_app(index, api_key, ServerSettings(**cfg["api"]["server"]))
