PY  := .venv/bin/python
UV  ?= $(shell command -v uv 2>/dev/null || echo $(HOME)/.local/bin/uv)
API_HOST ?= 127.0.0.1
API_PORT ?= 8000
# Local-only default for the mock API; override in the environment (Secrets Manager later).
OLIST_API_KEY ?= dev-local-key
export OLIST_API_KEY
# Airflow runs from its own venv; its state lives in ./airflow (gitignored except airflow/dags).
AIRFLOW := .venv-airflow/bin/airflow
export AIRFLOW_HOME := $(CURDIR)/airflow
export AIRFLOW__CORE__EXECUTOR := LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES := False
export AIRFLOW__CORE__DAGS_FOLDER := $(CURDIR)/airflow/dags
export AIRFLOW__CORE__PARALLELISM := 4
# AWS=1 runs any pipeline target against the S3 lake (ap-southeast-2) with credentials from Secrets Manager.
ifeq ($(AWS),1)
export OLIST_TARGET := aws
endif

.PHONY: help venv java up down psql seed replay replay-range replay-all verify verify-idempotency \
        spark-smoke bronze bronze-postgres bronze-files bronze-api api api-health silver silver-full-refresh gold gold-full-refresh dq daily verify-gold verify-gold-idempotency salting-demo airflow-venv airflow-setup airflow airflow-stop backfill verify-bronze verify-silver verify-silver-idempotency test test-unit reset-source reset \
        no-target aws-publish run-days glue-deploy glue-run aws-pull aws-check aws-up aws-sync aws-status aws-glue-test aws-down verify-s3-parity athena-ddl athena-create \
        athena-queries athena-check athena-register-partitions small-files-experiment

# A bare `make` (or a mistyped `make "dq LAYER=bronze"`, which make reads as a variable assignment with
# no target) must not look like a successful run: show the targets, then fail.
.DEFAULT_GOAL := no-target
no-target:
	@$(MAKE) --no-print-directory help
	@echo "error: no target given (did you quote a target and its variables together?)" >&2; exit 2

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

bronze-files:  ## Landing files -> Bronze: make bronze-files DATE=2017-03-01 (or START=.. END=..)
	$(PY) scripts/files_to_bronze.py $(if $(START),--start $(START) --end $(END),--date $(DATE))

bronze: bronze-postgres bronze-files  ## Both Bronze jobs: make bronze DATE=2017-03-01

api:  ## Run the mock customer-activity API (foreground; use a second terminal)
	$(PY) -m uvicorn olist_pipeline.api.server:app_from_config --factory \
		--host $(API_HOST) --port $(API_PORT) --workers 1

api-health:  ## Fail unless the mock API is up
	@curl -fsS http://$(API_HOST):$(API_PORT)/healthz >/dev/null 2>&1 || \
		{ echo "Customer-activity API is not running: start it with 'make api' in another terminal."; exit 1; }

bronze-api:  ## API -> landing -> Bronze: make bronze-api DATE=2017-03-01 (or START=.. END=..)
	$(PY) scripts/api_to_bronze.py $(if $(START),--start $(START) --end $(END),--date $(DATE))

silver:  ## Bronze -> Silver for one batch date: make silver DATE=2017-03-01
	$(PY) scripts/silver.py --date $(DATE)

silver-full-refresh:  ## Rebuild every Silver table from all Bronze up to DATE
	$(PY) scripts/silver.py --date $(DATE) --full-refresh

gold:  ## Silver -> Gold for one batch date: make gold DATE=2017-12-31
	$(PY) scripts/gold.py --date $(DATE)

gold-full-refresh:  ## Rebuild every Gold fact partition from Silver
	$(PY) scripts/gold.py --date $(DATE) --full-refresh

dq:  ## Data-quality checks for a layer: make dq LAYER=silver DATE=2017-03-01 (exit 1 on errors)
	$(PY) scripts/dq.py --layer $(LAYER) --date $(DATE)

daily: api-health  ## One simulated day: replay, Bronze (Postgres, files, API), Silver, Gold, DQ after each
	$(MAKE) replay DATE=$(DATE)
	$(MAKE) bronze DATE=$(DATE)
	$(MAKE) bronze-api DATE=$(DATE)
	$(MAKE) dq LAYER=bronze DATE=$(DATE)
	$(MAKE) silver DATE=$(DATE)
	$(MAKE) dq LAYER=silver DATE=$(DATE)
	$(MAKE) gold DATE=$(DATE)
	$(MAKE) dq LAYER=gold DATE=$(DATE)

verify-bronze:  ## Check Bronze partitions and contents against Postgres and the landing files
	$(PY) scripts/verify_bronze.py

verify-silver:  ## Check Silver vs Postgres, row accounting and quarantine vs injected dirt
	$(PY) scripts/verify_silver.py full

verify-silver-idempotency:  ## Rerun Silver for DATE and check nothing changes: make verify-silver-idempotency DATE=...
	$(PY) scripts/verify_silver.py idempotency --date $(DATE)

verify-gold:  ## Check Gold totals, SCD2 invariants, fact-to-version mapping and LTV: make verify-gold DATE=...
	$(PY) scripts/verify_gold.py full --date $(DATE)

verify-gold-idempotency:  ## Rerun Gold for DATE and check nothing changes
	$(PY) scripts/verify_gold.py idempotency --date $(DATE)

salting-demo:  ## Skewed aggregation by customer_state with and without salting -> docs/SALTING.md
	$(PY) scripts/salting_demo.py

airflow-venv:  ## Create .venv-airflow with Airflow 3.3.2 (official constraints file)
	$(UV) venv --python 3.12 .venv-airflow
	$(UV) pip install --python .venv-airflow/bin/python "apache-airflow==3.3.2" \
		--constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

airflow-setup:  ## Migrate the Airflow DB (SQLite) and create the 1-slot spark pool
	$(AIRFLOW) db migrate
	$(AIRFLOW) pools set spark 1 "one Spark JVM at a time (8 GB laptop)"

airflow:  ## Start Airflow standalone in the background (UI http://localhost:8080, log airflow/standalone.log)
	@PATH="$(CURDIR)/.venv-airflow/bin:$$PATH" nohup airflow standalone > airflow/standalone.log 2>&1 & echo $$! > airflow/standalone.pid
	@echo "Airflow starting (pid $$(cat airflow/standalone.pid)); admin password in airflow/simple_auth_manager_passwords.json.generated"

airflow-stop:  ## Stop Airflow standalone
	@-pkill -f "airflow standalone" ; pkill -f "airflow (scheduler|api-server|dag-processor|triggerer)" ; rm -f airflow/standalone.pid; echo "Airflow stopped"

backfill:  ## (Re)run days: make backfill START=2018-01-03 END=2018-01-03 [REPROCESS=completed] (Airflow running, DAG unpaused)
	$(AIRFLOW) backfill create --dag-id olist_daily --from-date $(START) --to-date $(END) --max-active-runs 1 \
		--reprocess-behavior $(or $(REPROCESS),none)

aws-check:  ## Read-only probes of the AWS services the project uses (ap-southeast-2)
	$(PY) scripts/aws.py check

aws-up:  ## Deploy the Step 8 stack (S3 bucket, Athena workgroup, Glue database) and the secret
	$(PY) scripts/aws.py up

aws-sync:  ## Upload the local lake and landing files to S3
	$(PY) scripts/aws.py sync

aws-status:  ## Stack, bucket usage and every resource tagged project=olist-pipeline
	$(PY) scripts/aws.py status

aws-publish:  ## Incremental upload of the local lake + landing to S3 (deletes replaced files on S3)
	$(PY) scripts/aws.py publish

run-days:  ## Local pipeline + DQ check + publish to S3 per day: make run-days START=2018-01-07 END=2018-01-09
	./scripts/run_days.sh $(START) $(END)

glue-deploy:  ## Build the wheel, upload it with the Glue entry script and DQ suites, create/update job olist-spark
	$(PY) scripts/aws.py glue-deploy

glue-run:  ## One task on Glue with local bookkeeping: make glue-run TASK=silver DATE=2018-01-10 (silver|gold|dq_silver|dq_gold)
	$(PY) scripts/glue_run.py run $(TASK) --date $(DATE)

aws-pull:  ## Copy the S3 lake back to the local lake (after Glue jobs wrote to S3)
	$(PY) scripts/aws.py pull

aws-glue-test:  ## Run the smallest Glue Spark job (Flex, 2 x G.1X, 5-min timeout; about $0.02)
	$(PY) scripts/aws.py glue-test

aws-down:  ## Teardown: pull the lake back, delete Glue test, secret, bucket and stack (DRY_RUN=1 to preview)
	$(PY) scripts/aws.py down $(if $(DRY_RUN),--dry-run)

verify-s3-parity:  ## S3 holds exactly the local lake and landing files (keys, sizes, MD5/ETags)
	$(PY) scripts/verify_s3_parity.py

small-files-experiment:  ## Daily vs monthly partitions of the fact table on Athena -> docs/SMALL_FILES.md
	$(PY) scripts/small_files_experiment.py

athena-ddl:  ## Regenerate sql/athena/<layer>/*.sql from the local lake's Parquet schemas
	$(PY) scripts/athena.py ddl

athena-create:  ## (Re)create the Athena tables in the Glue database
	$(PY) scripts/athena.py create

athena-queries:  ## Save and run the Gold KPI queries (sql/athena/kpis)
	$(PY) scripts/athena.py queries

athena-check:  ## Athena answers equal Spark answers on the S3 lake: make athena-check DATE=2018-01-09
	$(PY) scripts/athena.py check --date $(DATE)

athena-register-partitions:  ## Fallback without partition projection: register partitions, compare counts
	$(PY) scripts/athena.py register-partitions

test:  ## All tests (integration tests skip if Postgres is down)
	$(PY) -m pytest -q

test-unit:  ## Unit tests only
	$(PY) -m pytest -q -m "not integration"

reset-source:  ## Empty transactional tables, pipeline state, landing files and lake (keeps reference data)
	$(PY) scripts/reset_source.py

reset:  ## Drop the Postgres volume, landing files and lake
	docker compose down -v
	rm -rf data/landing data/lake
