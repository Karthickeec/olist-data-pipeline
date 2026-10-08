"""Empty the simulated source and everything derived from it, keeping reference data.

  * TRUNCATE the 5 transactional tables (products/sellers and other reference
    tables are never truncated: order_items references them)
  * DROP the pipeline bookkeeping schema (watermarks, runs, snapshots)
  * delete the landing files and the local lake

Afterwards, replay and ingest day by day (`make daily DATE=...`).
"""

import shutil
from pathlib import Path

from psycopg import sql

from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.sources import TRANSACTIONAL_TABLES


def main() -> None:
    cfg = load_config()
    with connect(cfg["pg"]) as conn, conn.transaction():
        conn.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(map(sql.Identifier, TRANSACTIONAL_TABLES))))
        conn.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(cfg["pg"]["pipeline_schema"])))
    print(f"truncated {', '.join(TRANSACTIONAL_TABLES)}; dropped schema {cfg['pg']['pipeline_schema']}")

    for path in (str(cfg["paths"]["landing_dir"]), cfg["lake"]["root"]):
        if "://" in path:
            print(f"skipped {path}: not a local path, delete it yourself")
        elif Path(path).exists():
            shutil.rmtree(path)
            print(f"deleted {path}")


if __name__ == "__main__":
    main()
