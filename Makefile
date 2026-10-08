PY  := .venv/bin/python
UV  ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)

.PHONY: help venv up down psql seed replay replay-range replay-all verify verify-idempotency \
        test test-unit reset

help:  ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

venv:  ## Create .venv (Python 3.12) and install the project
	$(UV) venv --python 3.12 .venv
	$(UV) pip install --python $(PY) -r requirements.txt -e .

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

test:  ## All tests (integration tests skip if Postgres is down)
	$(PY) -m pytest -q

test-unit:  ## Unit tests only
	$(PY) -m pytest -q -m "not integration"

reset:  ## Drop the Postgres volume and landing files
	docker compose down -v
	rm -rf data/landing
