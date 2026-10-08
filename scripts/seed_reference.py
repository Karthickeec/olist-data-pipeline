"""Create the source schema and load the reference tables (insert-only, safe to rerun)."""

from olist_pipeline.config import load_config
from olist_pipeline.db import apply_schema, connect
from olist_pipeline.seed import seed_reference


def main() -> None:
    cfg = load_config()
    with connect(cfg["pg"]) as conn:
        apply_schema(conn, cfg["pg"]["schema"])
        for name, (read, inserted) in seed_reference(conn, cfg["paths"]["raw_dir"]).items():
            print(f"{name:<36} read={read:>8} inserted={inserted:>8}")


if __name__ == "__main__":
    main()
