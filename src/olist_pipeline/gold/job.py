"""gold: Silver -> Gold for one batch date.

Dimensions are rebuilt in full from Silver every batch (they are small, and full rebuilds
are deterministic). The fact and the daily aggregate rebuild only the purchase-date
partitions that Silver batch D rewrote in order_lines; customer_metrics writes the
as_of_date=D snapshot. A rerun of D therefore reproduces the same Gold.
"""
import argparse
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from olist_pipeline.bronze import utc_now
from olist_pipeline.config import load_config
from olist_pipeline.db import connect
from olist_pipeline.gold import model
from olist_pipeline.lake import list_dirs, overwrite_partition
from olist_pipeline.silver.common import layer_dir, merge_into, silver_dir
from olist_pipeline.spark import build_spark
from olist_pipeline.watermarks import Bookkeeping, IngestRange, LayerRun, record_layer_run

LAYER = "gold"
DATE_RANGE = (date(2016, 1, 1), date(2018, 12, 31))


class GoldJob:
    def __init__(self, spark: SparkSession, cfg: dict, book: Bookkeeping, batch_date: date,
                 full_refresh: bool = False):
        self.spark, self.cfg, self.book = spark, cfg, book
        self.batch_date, self.full_refresh = batch_date, full_refresh
        self.root = cfg["lake"]["root"]
        self.processed_at = utc_now()

    def silver(self, table: str) -> DataFrame:
        return self.spark.read.parquet(silver_dir(self.root, table))

    def gold(self, table: str) -> DataFrame:
        return self.spark.read.parquet(layer_dir(self.root, LAYER, table))

    def stamp(self, df: DataFrame) -> DataFrame:
        return (df.withColumn("_batch_date", F.lit(self.batch_date.isoformat()).cast("date"))
                  .withColumn("_processed_at", F.lit(self.processed_at.isoformat(sep=" ")).cast("timestamp")))

    def finish(self, table: str, rows: int, detail: str = "") -> None:
        record_layer_run(self.book, LAYER, table, self.batch_date, IngestRange(None, self.batch_date),
                         LayerRun(rows_in=rows, rows_valid=rows, rows_written=rows, detail=detail))
        print(f"{table:<26} rows={rows:>8}  {detail}", flush=True)

    def replace_table(self, table: str, df: DataFrame, key: list[str]) -> int:
        # Dimensions carry only _processed_at: they are rebuilt whole and belong to no single batch.
        df = df.withColumn("_processed_at", F.lit(self.processed_at.isoformat(sep=" ")).cast("timestamp"))
        return merge_into(self.spark, df, self.root, table, key, [F.col(key[0])], full_refresh=True,
                          layer=LAYER).rows_written

    def run(self) -> None:
        self.finish("dim_date", self.replace_table("dim_date", model.build_dim_date(self.spark, *DATE_RANGE),
                                                   ["date_key"]))
        self.finish("dim_product", self.replace_table("dim_product", model.build_dim_product(self.silver("products")),
                                                      ["product_sk"]))
        self.finish("dim_seller", self.replace_table(
            "dim_seller", model.build_dim_seller(self.silver("sellers"), self.silver("geolocation")), ["seller_sk"]))
        dim_customer = model.build_dim_customer(self.silver("orders"), self.silver("customers"),
                                                self.silver("customer_changes"))
        rows = self.replace_table("dim_customer", dim_customer, ["customer_sk"])
        versions = self.gold("dim_customer")
        multi = versions.groupBy("customer_unique_id").count().filter("count > 1").count()
        self.finish("dim_customer", rows, f"{versions.filter('is_current').count()} customers, "
                                          f"{multi} with more than one version")

        lines = self.silver("order_lines")
        if self.full_refresh:
            parts = [d.split("=", 1)[1] for d in list_dirs(self.spark, silver_dir(self.root, "order_lines"))]
        else:
            parts = [str(r[0]) for r in lines.filter(F.col("_batch_date") == F.lit(self.batch_date.isoformat())
                                                     .cast("date")).select("order_purchase_date").distinct().collect()]
        if parts:
            wanted = F.col("order_purchase_date").isin([F.lit(p).cast("date") for p in sorted(parts)])
            fact = model.build_fact_order_lines(lines.filter(wanted), self.gold("dim_customer"))
            res = merge_into(self.spark, self.stamp(fact), self.root, "fact_order_lines",
                             ["order_id", "order_item_id"], [F.col("_processed_at").desc()], "order_purchase_date",
                             self.full_refresh, union_existing=False, layer=LAYER)
            self.finish("fact_order_lines", res.rows_written, f"{len(res.partitions)} partitions rebuilt")
            sales = model.build_daily_category_sales(self.gold("fact_order_lines").filter(wanted),
                                                     self.gold("dim_product"))
            res = merge_into(self.spark, self.stamp(sales), self.root, "agg_daily_category_sales",
                             ["order_purchase_date", "category"], [F.col("_processed_at").desc()],
                             "order_purchase_date", self.full_refresh, union_existing=False, layer=LAYER)
            self.finish("agg_daily_category_sales", res.rows_written, f"{len(res.partitions)} partitions rebuilt")
        else:
            self.finish("fact_order_lines", 0, "no order_lines partitions rebuilt by this Silver batch")

        metrics = self.stamp(model.build_customer_metrics(self.gold("fact_order_lines"), self.batch_date))
        rows = overwrite_partition(self.spark, metrics.coalesce(1), layer_dir(self.root, LAYER, "customer_metrics"),
                                   "as_of_date", self.batch_date)
        segments = {r[0]: r[1] for r in self.gold("customer_metrics")
                    .filter(F.col("as_of_date") == F.lit(self.batch_date.isoformat()).cast("date"))
                    .groupBy("rfm_segment").count().collect()}
        self.finish("customer_metrics", rows, ", ".join(f"{k}={segments.get(k, 0)}" for k in model.RFM_SEGMENTS))


def run(spark: SparkSession, cfg: dict, batch_date: date, full_refresh: bool = False) -> None:
    with connect(cfg["pg"]) as conn:
        book = Bookkeeping(conn, cfg["pg"]["pipeline_schema"])
        book.ensure_schema()
        GoldJob(spark, cfg, book, batch_date, full_refresh).run()


def main(argv=None) -> None:
    p = argparse.ArgumentParser(description="Build Gold from Silver for one batch date.")
    p.add_argument("--date", type=date.fromisoformat, required=True)
    p.add_argument("--full-refresh", action="store_true", help="rebuild every fact partition")
    args = p.parse_args(argv)
    cfg = load_config()
    spark = build_spark(cfg, "gold")
    try:
        run(spark, cfg, args.date, args.full_refresh)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
