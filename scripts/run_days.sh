#!/usr/bin/env bash
# Run the pipeline locally for START..END, prove DQ really ran for all three layers, publish to S3.
#   scripts/run_days.sh 2018-01-07 2018-01-09
# Each day: make daily (replay, Bronze, DQ bronze, Silver, DQ silver, Gold, DQ gold), then a check that
# pipeline.dq_results got fresh rows for every layer (so a no-op make target can't pass), then publish.
set -euo pipefail
cd "$(dirname "$0")/.."
start=${1:?START date}; end=${2:?END date}
PY=.venv/bin/python
day=$start
while [[ ! "$day" > "$end" ]]; do
  t0=$(date +%s); since=$(date -u +"%Y-%m-%d %H:%M:%S")
  make daily DATE="$day"
  t1=$(date +%s)
  $PY - "$day" "$since" <<'PYEOF'
import sys
from olist_pipeline.config import load_config
from olist_pipeline.db import connect
day, since = sys.argv[1], sys.argv[2]
with connect(load_config()["pg"]) as c:
    rows = dict(c.execute(
        "SELECT layer, count(*) FILTER (WHERE status <> 'pass' AND severity = 'error') * 1000000 + count(*) "
        "FROM pipeline.dq_results WHERE batch_date = %s AND checked_at >= %s GROUP BY layer", (day, since)).fetchall())
missing = [l for l in ("bronze", "silver", "gold") if l not in rows]
summary = ", ".join(f"{l}: {rows[l] % 1000000} checks, {rows[l] // 1000000} blocking failures" for l in sorted(rows))
print(f"DQ for {day}: {summary or 'none'}")
if missing or any(v // 1000000 for v in rows.values()):
    sys.exit(f"DQ did not run (or failed) for {day}: missing layers {missing}")
PYEOF
  $PY scripts/aws.py publish
  echo "== $day: pipeline $((t1 - t0))s, total with DQ check and publish $(( $(date +%s) - t0 ))s"
  day=$(date -j -v+1d -f "%Y-%m-%d" "$day" +%Y-%m-%d)
done
