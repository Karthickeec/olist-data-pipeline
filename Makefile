PY  := .venv/bin/python
UV  ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)

.PHONY: help venv java up down psql seed replay replay-range replay-all verify verify-idempotency \
        spark-smoke bronze bronze-postgres bronze-files daily verify-bronze test test-unit reset-source reset

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

daily:  ## One simulated day: replay DATE into the source, then ingest it into Bronze
	$(MAKE) replay DATE=$(DATE)
	$(MAKE) bronze DATE=$(DATE)

verify-bronze:  ## Check Bronze partitions and contents against Postgres and the landing files
	$(PY) scripts/verify_bronze.py

test:  ## All tests (integration tests skip if Postgres is down)
	$(PY) -m pytest -q

test-unit:  ## Unit tests only
	$(PY) -m pytest -q -m "not integration"

reset-source:  ## Empty transactional tables, pipeline state, landing files and lake (keeps reference data)
	$(PY) scripts/reset_source.py

reset:  ## Drop the Postgres volume, landing files and lake
	docker compose down -v
	rm -rf data/landing data/lake
