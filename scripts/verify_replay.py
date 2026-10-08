"""Verify the replay.

  full                     every table matches its CSV exactly (run after `replay.py --all`)
  idempotency --date D...  rerunning the given days changes no table and no landing file
"""
import argparse
import sys
from datetime import date

from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.replay import replay_day
from olist_pipeline.replay_logic import build_index
from olist_pipeline.sources import TABLES, load_sources, read_address_pool
from olist_pipeline.verify import compare_table, snapshot

SAMPLE = 3


def check_full(conn, cfg) -> bool:
    ok = True
    for name in TABLES:
        diff = compare_table(conn, cfg["paths"]["raw_dir"], name)
        status = "OK" if diff.ok else "MISMATCH"
        print(f"{name:<36} csv={diff.csv_rows:>8} db={diff.db_rows:>8}  {status}")
        for label, keys in (("missing", diff.missing), ("extra", diff.extra), ("different", diff.different)):
            if keys:
                print(f"    {label}: {len(keys)} e.g. {keys[:SAMPLE]}")
        ok &= diff.ok
    return ok


def check_idempotency(conn, cfg, days: list[date]) -> bool:
    raw_dir, landing_dir = cfg["paths"]["raw_dir"], cfg["paths"]["landing_dir"]
    src = load_sources(raw_dir)
    idx = build_index(src)
    pool = read_address_pool(raw_dir)

    before = snapshot(conn, landing_dir)
    for day in days:
        print(replay_day(conn, src, idx, day, landing_dir, pool, cfg["customer_changes"]))
    after = snapshot(conn, landing_dir)

    changed = [k for k in before if before[k] != after[k]]
    for k in before:
        print(f"{k:<36} {'CHANGED' if k in changed else 'unchanged'}")
    return not changed


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="check", required=True)
    sub.add_parser("full")
    idem = sub.add_parser("idempotency")
    idem.add_argument("--date", type=date.fromisoformat, action="append", required=True)
    args = p.parse_args()

    cfg = load_config()
    with connect(cfg["pg"]) as conn:
        ok = check_full(conn, cfg) if args.check == "full" else check_idempotency(conn, cfg, args.date)
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
