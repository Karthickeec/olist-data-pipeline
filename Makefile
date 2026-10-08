PY  := .venv/bin/python
UV  ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)
API_HOST ?= 127.0.0.1
API_PORT ?= 8000
# Local-only default for the mock API; override in the environment (Secrets Manager later).
OLIST_API_KEY ?= dev-local-key
export OLIST_API_KEY

.PHONY: help venv java up down psql seed replay replay-range replay-all verify verify-idempotency \
        spark-smoke bronze bronze-postgres bronze-files bronze-api api api-health silver silver-full-refresh dq daily verify-bronze verify-silver verify-silver-idempotency test test-unit reset-source reset

help:  ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

venv:  ## Create .venv (Python 3.12) and install the project
	$(UV) venv --python 3.12 .venv
	$(UV) pip install --python $(PY) -r requirements.txt -e .

java:  ## Install Temurin JDK 17 into ~/.local/share/jdk-17 (for Spark)
	./scripts/install_java.sh

up:  ## Start Postgres and wait until it is healthy
	docker compose up -d --wait

down:  ## Stop Postgres (data volume is kept)
	docker compose down

psql:  ## Open psql inside the container
	docker compose exec postgres psql -U olist -d olist

seed:  ## Create schema and load reference tables (insert-only)
	$(PY) scripts/seed_reference.py

replay:  ## Replay one day: make replay DATE=2017-03-01
	$(PY) scripts/replay.py --date $(DATE)

replay-range:  ## Replay a range: make replay-range START=2017-01-01 END=2017-01-31
	$(PY) scripts/replay.py --start $(START) --end $(END)

replay-all:  ## Replay the full history
	$(PY) scripts/replay.py --all

verify:  ## Compare every table with its CSV (after replay-all)
	$(PY) scripts/verify_replay.py full

verify-idempotency:  ## Rerun days and check nothing changes: make verify-idempotency DATES="2017-03-01 2016-10-04"
	$(PY) scripts/verify_replay.py idempotency $(foreach d,$(DATES),--date $(d))

spark-smoke:  ## Check Spark: Java, Parquet round trip, JDBC read
	$(PY) scripts/spark_smoke.py

bronze-postgres:  ## Postgres -> Bronze for one batch date: make bronze-postgres DATE=2017-03-01
	$(PY) scripts/postgres_to_bronze.py --date $(DATE)

bronze-files:  ## Landing files -> Bronze for one batch date: make bronze-files DATE=2017-03-01
	$(PY) scripts/files_to_bronze.py --date $(DATE)

bronze: bronze-postgres bronze-files  ## Both Bronze jobs: make bronze DATE=2017-03-01

api:  ## Run the mock customer-activity API (foreground; use a second terminal)
	$(PY) -m uvicorn olist_pipeline.api.server:app_from_config --factory \
		--host $(API_HOST) --port $(API_PORT) --workers 1

api-health:  ## Fail unless the mock API is up
	@curl -fsS http://$(API_HOST):$(API_PORT)/healthz >/dev/null 2>&1 || \
		{ echo "Customer-activity API is not running: start it with 'make api' in another terminal."; exit 1; }

bronze-api:  ## API -> landing -> Bronze for one batch date: make bronze-api DATE=2017-03-01
	$(PY) scripts/api_to_bronze.py --date $(DATE)

silver:  ## Bronze -> Silver for one batch date: make silver DATE=2017-03-01
	$(PY) scripts/silver.py --date $(DATE)

silver-full-refresh:  ## Rebuild every Silver table from all Bronze up to DATE
	$(PY) scripts/silver.py --date $(DATE) --full-refresh

dq:  ## Data-quality checks for a layer: make dq LAYER=silver DATE=2017-03-01 (exit 1 on errors)
	$(PY) scripts/dq.py --layer $(LAYER) --date $(DATE)

daily: api-health  ## One simulated day: replay, Bronze (Postgres, files, API), DQ, Silver, DQ
	$(MAKE) replay DATE=$(DATE)
	$(MAKE) bronze DATE=$(DATE)
	$(MAKE) bronze-api DATE=$(DATE)
	$(MAKE) dq LAYER=bronze DATE=$(DATE)
	$(MAKE) silver DATE=$(DATE)
	$(MAKE) dq LAYER=silver DATE=$(DATE)

verify-bronze:  ## Check Bronze partitions and contents against Postgres and the landing files
	$(PY) scripts/verify_bronze.py

verify-silver:  ## Check Silver vs Postgres, row accounting and quarantine vs injected dirt
	$(PY) scripts/verify_silver.py full

verify-silver-idempotency:  ## Rerun Silver for DATE and check nothing changes: make verify-silver-idempotency DATE=...
	$(PY) scripts/verify_silver.py idempotency --date $(DATE)

test:  ## All tests (integration tests skip if Postgres is down)
	$(PY) -m pytest -q

test-unit:  ## Unit tests only
	$(PY) -m pytest -q -m "not integration"

reset-source:  ## Empty transactional tables, pipeline state, landing files and lake (keeps reference data)
	$(PY) scripts/reset_source.py

reset:  ## Drop the Postgres volume, landing files and lake
	docker compose down -v
	rm -rf data/landing data/lake
