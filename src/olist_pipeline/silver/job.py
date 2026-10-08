"""silver: Bronze -> Silver for one batch date.

Each table processes the Bronze partitions with ingest_date in (last processed, D]
(pipeline.layer_runs), cleans and types them, quarantines what cannot be fixed,
and merges the rest into Silver. Tables run in dependency order: reference data,
customers, orders, the order children, the CRM and API sources, then order_lines.
"""
import argparse
from collections.abc import Callable
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from olist_pipeline.bronze import utc_now
from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.lake import (PARTITION_COLUMN, delete_path, list_dirs, overwrite_partition, path_exists,
                                 table_path)
from olist_pipeline.silver.clean import (build_geolocation, build_products, clean_customer_activity,
                                         clean_customer_changes)
from olist_pipeline.silver.common import (BRONZE_INGEST_DATE, FIRST_BATCH, FIRST_SEEN, REASON, add_silver_metadata,
                                          latest_snapshot, merge_into, pending_dir, quarantine_dir,
                                          read_bronze, silver_dir, split_quarantine, write_quarantine)
from olist_pipeline.silver.order_lines import build_order_lines
from olist_pipeline.spark import build_spark
from olist_pipeline.watermarks import Bookkeeping, IngestRange, LayerRun, layer_range, record_layer_run

LAYER = "silver"
LATE_ARRIVING = ("orphan_order", "orphan_customer")
PENDING_MAX_DAYS = 7
PG = "olist_postgres"
BRONZE_METADATA = ("_ingested_at", "_batch_date", "_source")


def pg_order() -> list[Column]:
    """Postgres rows: newest source version first, then newest Bronze partition and Silver write."""
    return [F.col("updated_at").desc(), F.col(BRONZE_INGEST_DATE).desc(), F.col("_processed_at").desc()]


def file_order() -> list[Column]:
    """File/API rows have no updated_at: newest Bronze partition, then newest Silver write."""
    return [F.col(BRONZE_INGEST_DATE).desc(), F.col("_processed_at").desc()]


Transform = Callable[[DataFrame], tuple[DataFrame, DataFrame, int]]


class SilverJob:
    def __init__(self, spark: SparkSession, cfg: dict, book: Bookkeeping, batch_date: date,
                 full_refresh: bool = False):
        self.spark, self.cfg, self.book = spark, cfg, book
        self.batch_date, self.full_refresh = batch_date, full_refresh
        self.root = cfg["lake"]["root"]
        self.processed_at = utc_now()
        self.touched_purchase_dates: set = set()
        self.reference_changed = False
        self.results: dict[str, LayerRun] = {}

    # --- helpers ---------------------------------------------------------------------------
    def bronze_path(self, source: str, table: str) -> str:
        return table_path(self.root, "bronze", source, table)

    def silver(self, table: str) -> DataFrame:
        return self.spark.read.parquet(silver_dir(self.root, table))

    def as_of(self, table: str) -> DataFrame:
        """Silver rows whose key existed by this batch: a rerun of D sees what D saw the first time,
        not parents that only arrived in later batches."""
        return self.silver(table).filter(F.col(FIRST_BATCH) <= F.lit(self.batch_date.isoformat()).cast("date"))

    def range_for(self, table: str) -> IngestRange:
        return layer_range(self.book, LAYER, table, self.batch_date, self.full_refresh)

    def finish(self, table: str, rng: IngestRange, run: LayerRun) -> None:
        record_layer_run(self.book, LAYER, table, self.batch_date, rng, run)
        self.results[table] = run
        print(f"{table:<20} in={run.rows_in:>6} valid={run.rows_valid:>6} quarantined={run.rows_quarantined:>4} "
              f"pending={run.rows_pending:>3} duplicate={run.rows_duplicate:>5} written={run.rows_written:>7}"
              f"  range {rng}"
              + (f"  {run.detail}" if run.detail else ""), flush=True)

    def previous_pending(self, table: str) -> DataFrame | None:
        """Rows left pending by the previous processed batch of this table (per layer_runs).

        A batch that left nothing pending writes no partition, so looking up the previous
        batch explicitly (instead of the newest pending partition) avoids retrying stale rows.
        A rerun of D finds the same previous batch, so it reads the same input.
        """
        if self.full_refresh:
            return None
        row = self.book.conn.execute(
            f"SELECT max(batch_date) FROM {self.book.schema}.layer_runs "
            "WHERE layer = %s AND table_name = %s AND batch_date < %s",
            (LAYER, table, self.batch_date)).fetchone()
        if row[0] is None:
            return None
        path = f"{pending_dir(self.root, table)}/batch_date={row[0].isoformat()}"
        return self.spark.read.parquet(path) if path_exists(self.spark, path) else None

    def write_outputs(self, table: str, quarantined: DataFrame, pending: DataFrame) -> tuple[int, int]:
        n_quarantined = write_quarantine(self.spark, quarantined, self.root, table, self.batch_date)
        out = pending.withColumn("batch_date", F.lit(self.batch_date.isoformat()).cast("date"))
        n_pending = overwrite_partition(self.spark, out, pending_dir(self.root, table), "batch_date",
                                        self.batch_date)
        return n_quarantined, n_pending

    def process(self, table: str, source: str, transform: Transform, key: list[str], order: list[Column],
                partition_col: str | None) -> list:
        """Read the Bronze range (+ rows still pending), clean, quarantine, merge.

        Returns the Silver partitions rewritten.
        """
        rng = self.range_for(table)
        if self.full_refresh:
            for path in (quarantine_dir(self.root, table), pending_dir(self.root, table)):
                delete_path(self.spark, path)
        bronze = read_bronze(self.spark, self.bronze_path(source, table), rng.low, rng.high)
        pending = self.previous_pending(table)
        inputs = []
        if bronze is not None:
            inputs.append(bronze.drop(*BRONZE_METADATA)
                          .withColumn(FIRST_SEEN, F.lit(self.batch_date.isoformat()).cast("date")))
        if pending is not None:
            inputs.append(pending)
        if not inputs:
            empty = self.spark.createDataFrame([], "x string")
            self.write_outputs(table, empty, empty)
            self.finish(table, rng, LayerRun(detail="no Bronze data yet"))
            return []
        batch = inputs[0] if len(inputs) == 1 else inputs[0].unionByName(inputs[1])
        batch = batch.cache()
        rows_in = batch.count()
        retried = pending.count() if pending is not None else 0
        if rows_in == 0:
            batch.unpersist()
            empty = self.spark.createDataFrame([], "x string")
            self.write_outputs(table, empty, empty)
            self.finish(table, rng, LayerRun(detail="no new Bronze rows"))
            return []

        valid, rejected, duplicates = transform(batch)
        valid = valid.drop(FIRST_SEEN).cache()
        rows_valid = valid.count()
        unique = valid.select(*key).distinct().count()
        # Orphans are usually late-arriving parents (e.g. an order whose newer version landed in a
        # later Bronze partition): keep them pending and retry for a week before quarantining.
        late = (F.col(REASON).isin(*LATE_ARRIVING)
                & (F.datediff(F.lit(self.batch_date.isoformat()).cast("date"), F.col(FIRST_SEEN))
                   < PENDING_MAX_DAYS))
        n_quarantined, n_pending = self.write_outputs(
            table, rejected.filter(~late).drop(FIRST_SEEN), rejected.filter(late).drop(REASON))
        reasons = ({r[0]: r[1] for r in rejected.filter(~late).groupBy(REASON).count().collect()}
                   if n_quarantined else {})

        merged = merge_into(self.spark, add_silver_metadata(valid, self.batch_date, self.processed_at),
                            self.root, table, key, order, partition_col, self.full_refresh)
        detail = [f"{k}={v}" for k, v in sorted(reasons.items())]
        if retried or n_pending:
            detail.append(f"pending: {retried} retried, {n_pending} still waiting")
        run = LayerRun(rows_in=rows_in, rows_valid=unique, rows_quarantined=n_quarantined,
                       rows_duplicate=duplicates + rows_valid - unique, rows_written=merged.rows_written,
                       rows_pending=n_pending, detail=", ".join(detail))
        assert run.rows_in == (run.rows_valid + run.rows_quarantined + run.rows_duplicate
                               + run.rows_pending), (table, run)
        self.extra_detail(table, valid, run)
        valid.unpersist()
        batch.unpersist()
        self.finish(table, rng, run)
        return merged.partitions

    def extra_detail(self, table: str, valid: DataFrame, run: LayerRun) -> None:
        """Count the fixes applied to dirty CRM/API rows (they were repaired, not quarantined)."""
        if table == "customer_changes":
            fixes = valid.agg(F.sum(F.col("_state_fixed").cast("int")).alias("states_fixed"),
                              F.sum(F.col("_city_filled").cast("int")).alias("cities_filled")).first()
        elif table == "customer_activity":
            fixes = valid.agg(F.sum(F.col("_timestamp_reformatted").cast("int")).alias("timestamps_reparsed"),
                              F.sum(F.col("page_views").isNull().cast("int")).alias("missing_page_views"),
                              F.sum(F.col("device").isNull().cast("int")).alias("missing_device"),
                              F.sum(F.col("last_seen_at").isNull().cast("int")).alias("missing_last_seen")).first()
        else:
            return
        text = ", ".join(f"{k}={v or 0}" for k, v in fixes.asDict().items())
        run.detail = f"{run.detail}; {text}" if run.detail else text

    # --- reference tables (Bronze snapshots, rebuilt only when a new snapshot arrived) --------
    def new_snapshot(self, table: str, rng: IngestRange) -> bool:
        parts = [d.split("=", 1)[1] for d in list_dirs(self.spark, self.bronze_path(PG, table))]
        return any((rng.low is None or p > rng.low.isoformat()) and p <= rng.high.isoformat() for p in parts)

    def reference(self, table: str, sources: tuple[str, ...], build: Callable[..., DataFrame], key: list[str]) -> None:
        rng = self.range_for(table)
        exists = path_exists(self.spark, silver_dir(self.root, table))
        if exists and not any(self.new_snapshot(s, rng) for s in sources):
            self.finish(table, rng, LayerRun(detail="unchanged, skipped"))
            return
        snapshots = [latest_snapshot(self.spark, self.bronze_path(PG, s), self.batch_date) for s in sources]
        if any(s is None for s in snapshots):
            self.finish(table, rng, LayerRun(detail="no Bronze snapshot yet"))
            return
        rows_in = snapshots[0].count()
        df = add_silver_metadata(build(*[s.drop(*BRONZE_METADATA, PARTITION_COLUMN) for s in snapshots]),
                                 self.batch_date, self.processed_at)
        merged = merge_into(self.spark, df, self.root, table, key, [F.col("_processed_at").desc()],
                            full_refresh=True)
        self.reference_changed |= exists
        self.finish(table, rng, LayerRun(rows_in=rows_in, rows_valid=merged.rows_written,
                                         rows_duplicate=rows_in - merged.rows_written,
                                         rows_written=merged.rows_written,
                                         detail="rebuilt from latest snapshot"))

    # --- tables ------------------------------------------------------------------------------
    def run(self) -> dict[str, LayerRun]:
        self.reference("geolocation", ("geolocation",), build_geolocation, ["zip_code_prefix"])
        self.reference("products", ("products", "product_category_name_translation"), build_products,
                       ["product_id"])
        self.reference("sellers", ("sellers",), lambda s: s, ["seller_id"])

        self.process("customers", PG, self.customers, ["customer_id"], pg_order(), None)
        parts = self.process("orders", PG, self.orders, ["order_id"], pg_order(), "order_purchase_date")
        self.touched_purchase_dates.update(parts)
        for table in ("order_items", "order_payments"):
            key = ["order_id", "order_item_id" if table == "order_items" else "payment_sequential"]
            parts = self.process(table, PG, self.order_child, key, pg_order(), "order_purchase_date")
            self.touched_purchase_dates.update(parts)
        self.process("order_reviews", PG, self.reviews, ["review_id", "order_id"], pg_order(),
                     "review_date")
        self.process("customer_changes", "crm", self.customer_changes, ["change_id"], file_order(),
                     "requested_date")
        self.process("customer_activity", "api", self.customer_activity,
                     ["customer_unique_id", "activity_date"], file_order(), "activity_date")
        self.order_lines()
        return self.results

    def customers(self, b: DataFrame):
        valid, quarantined = split_quarantine(b, [
            ("missing_required_field", F.col("customer_id").isNull() | F.col("customer_unique_id").isNull()),
        ])
        return valid, quarantined, 0

    def orders(self, b: DataFrame):
        known = self.as_of("customers").select("customer_id").withColumn("_known", F.lit(True))
        # Silver customers is ≈99k narrow rows: broadcast it rather than shuffle the batch.
        x = b.join(F.broadcast(known), "customer_id", "left")
        valid, quarantined = split_quarantine(x, [
            ("orphan_customer", F.col("_known").isNull()),
        ])
        valid = valid.drop("_known").withColumn("order_purchase_date", F.to_date("order_purchase_timestamp"))
        return valid, quarantined.select(*b.columns, REASON), 0

    def order_child(self, b: DataFrame):
        """Items and payments take their partition (the order's purchase date) from Silver orders."""
        # Silver orders is narrow here (2 columns, ≈99k rows at full scale): broadcast lookup.
        dates = self.as_of("orders").select("order_id", "order_purchase_date")
        x = b.join(F.broadcast(dates), "order_id", "left")
        valid, quarantined = split_quarantine(x, [
            ("orphan_order", F.col("order_purchase_date").isNull()),
        ])
        return valid, quarantined.select(*b.columns, REASON), 0

    def reviews(self, b: DataFrame):
        known = self.as_of("orders").select("order_id").withColumn("_known", F.lit(True))
        x = b.join(F.broadcast(known), "order_id", "left")
        valid, quarantined = split_quarantine(x, [
            ("orphan_order", F.col("_known").isNull()),
            ("missing_required_field", F.col("review_creation_date").isNull()),
        ])
        # Partition column is a separate date: review_creation_date itself stays the source timestamp.
        valid = valid.drop("_known").withColumn("review_date", F.to_date("review_creation_date"))
        return valid, quarantined.select(*b.columns, REASON), 0

    def customer_changes(self, b: DataFrame):
        return clean_customer_changes(b, self.as_of("customers"),
                                      self.silver("geolocation").select("zip_code_prefix", "city"))

    def customer_activity(self, b: DataFrame):
        return clean_customer_activity(b, self.as_of("customers"))

    def order_lines(self) -> None:
        table = "order_lines"
        rng = self.range_for(table)
        target_exists = path_exists(self.spark, silver_dir(self.root, table))
        parts = set(self.touched_purchase_dates)
        if self.reference_changed and target_exists or self.full_refresh:
            # A product/seller change (or a full refresh) can affect every partition.
            parts = {d.split("=", 1)[1] for d in list_dirs(self.spark, silver_dir(self.root, "orders"))}
        if not parts:
            self.finish(table, rng, LayerRun(detail="no orders touched"))
            return
        wanted = F.col("order_purchase_date").isin([F.lit(str(p)).cast("date") for p in sorted(map(str, parts))])
        orders = self.silver("orders").filter(wanted)
        lines = build_order_lines(orders, self.silver("order_items").filter(wanted),
                                  self.silver("order_payments").filter(wanted), self.silver("customers"),
                                  self.silver("products"), self.silver("sellers"))
        merged = merge_into(self.spark, add_silver_metadata(lines, self.batch_date, self.processed_at),
                            self.root, table, ["order_id", "order_item_id"], [F.col("_processed_at").desc()],
                            "order_purchase_date", self.full_refresh, union_existing=False)
        no_items = orders.join(self.silver("order_items").filter(wanted), "order_id", "left_anti").count()
        self.finish(table, rng, LayerRun(rows_in=merged.rows_written, rows_valid=merged.rows_written,
                                         rows_written=merged.rows_written,
                                         detail=f"{len(merged.partitions)} partitions rebuilt; "
                                                f"{no_items} orders without items"))


def run(spark: SparkSession, cfg: dict, batch_date: date, full_refresh: bool = False) -> dict[str, LayerRun]:
    with connect(cfg["pg"]) as conn:
        book = Bookkeeping(conn, cfg["pg"]["pipeline_schema"])
        book.ensure_schema()
        return SilverJob(spark, cfg, book, batch_date, full_refresh).run()


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Build Silver from Bronze for one batch date.")
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.add_argument("--full-refresh", action="store_true", help="rebuild every Silver table from all of Bronze")
    args = p.parse_args(argv)
    cfg = load_config()
    spark = build_spark(cfg, "silver")
    try:
        run(spark, cfg, args.date, args.full_refresh)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
