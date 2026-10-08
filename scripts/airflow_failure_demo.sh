#!/usr/bin/env bash
# Failure demo for olist_daily (README, step 7): Postgres down when the run starts (replay fails,
# is retried and recovers), API down throughout (api_up waits, times out, fails with a clear log
# block), then the API comes back and api_up + downstream are cleared so the run completes.
# Usage: scripts/airflow_failure_demo.sh [DAY]   (DAY must be the day after the last completed run)
set -u
cd /Users/karthick/projects/olist-pipeline
export PATH="/Applications/Docker.app/Contents/Resources/bin:$PATH"
export AIRFLOW_HOME=$PWD/airflow AIRFLOW__CORE__EXECUTOR=LocalExecutor AIRFLOW__CORE__LOAD_EXAMPLES=False AIRFLOW__CORE__DAGS_FOLDER=$PWD/airflow/dags
A=.venv-airflow/bin/airflow
DAY="${1:-2018-01-06}"
RUN="scheduled__${DAY}T00:00:00+00:00"
PREV=$(date -j -v-1d -f %Y-%m-%d "$DAY" +%Y-%m-%d); NEXT=$(date -j -v+1d -f %Y-%m-%d "$DAY" +%Y-%m-%d)
ti() { sqlite3 airflow/airflow.db "SELECT state || ' try ' || try_number FROM task_instance WHERE run_id='$RUN' AND task_id='$1'"; }
ts() { date -u +%H:%M:%S; }

echo "$(ts) stopping the API and Postgres"
PID=$(lsof -tiTCP:8000 -sTCP:LISTEN); [ -n "$PID" ] && kill $PID
docker stop olist-postgres >/dev/null
echo "$(ts) restarting Airflow with end date $DAY, API wait 60s, retry delay 20s"
make airflow-stop >/dev/null 2>&1; sleep 3
OLIST_DAG_END_DATE=$DAY OLIST_API_WAIT_SECONDS=60 OLIST_RETRY_DELAY_SECONDS=20 make airflow >/dev/null
for i in $(seq 1 120); do s=$(ti replay); [[ "$s" == up_for_retry* ]] && break; sleep 2; done
echo "$(ts) replay: $(ti replay)  -> starting Postgres again"
docker start olist-postgres >/dev/null
for i in $(seq 1 60); do docker inspect -f '{{.State.Health.Status}}' olist-postgres | grep -q healthy && break; sleep 1; done
echo "$(ts) Postgres healthy"
for i in $(seq 1 120); do st=$(sqlite3 airflow/airflow.db "SELECT state FROM dag_run WHERE run_id='$RUN'"); [ "$st" = failed ] && break; echo "$(ts) run=$st replay=$(ti replay) api_up=$(ti api_up)"; sleep 15; done
echo "$(ts) run state: $(sqlite3 airflow/airflow.db "SELECT state FROM dag_run WHERE run_id='$RUN'")"
sqlite3 -column airflow/airflow.db "SELECT task_id, state, try_number FROM task_instance WHERE run_id='$RUN' ORDER BY task_id"

echo "$(ts) starting the API and clearing api_up + downstream"
(.venv/bin/python -m uvicorn olist_pipeline.api.server:app_from_config --factory --host 127.0.0.1 --port 8000 --workers 1 > airflow/api_demo.log 2>&1 &)
for i in $(seq 1 60); do curl -fsS http://127.0.0.1:8000/healthz >/dev/null 2>&1 && break; sleep 1; done
# The date window must bracket the logical date (an equal start and end matches nothing).
$A tasks clear olist_daily -t api_up --downstream --only-failed -s "$PREV" -e "$NEXT" --yes 2>&1 | grep -vE "warning|graphviz" | tail -2
for i in $(seq 1 80); do st=$(sqlite3 airflow/airflow.db "SELECT state FROM dag_run WHERE run_id='$RUN'"); [ "$st" = success ] || [ "$st" = failed ] && [ $i -gt 2 ] && break; sleep 10; done
echo "$(ts) run state after recovery: $(sqlite3 airflow/airflow.db "SELECT state FROM dag_run WHERE run_id='$RUN'")"
sqlite3 -column airflow/airflow.db "SELECT task_id, state, try_number FROM task_instance WHERE run_id='$RUN' ORDER BY task_id"
