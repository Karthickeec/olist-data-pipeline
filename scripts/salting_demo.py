"""Salting demo on gold.fact_order_lines, skewed by customer_state (SP is the largest state by far).

  (a) sum + count per state: Spark pre-aggregates per input partition before the shuffle
      (partial aggregation), so each state sends one row per partition and skew barely matters.
  (b) a sort-merge join on customer_state (broadcast disabled): every SP row is shuffled to one
      task. Salted: the big side gets a salt 0..N-1, the small side is replicated N times, and the
      join runs on (state, salt). Also run with AQE's skew-join splitting.

For each: wall time (median of 3 runs) and, for the stage after the shuffle, rows read and run
time per task (max vs median, from Spark's REST API). Writes docs/SALTING.md.
"""

import json
import statistics
import time
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from olist_pipeline.config import PROJECT_ROOT, load_config
from olist_pipeline.silver.common import layer_dir
from olist_pipeline.spark import build_spark

SCALE = 100  # replicate the fact to make the skew measurable (5M rows), keeping the state mix
SHUFFLE_PARTITIONS = 8
SALT = 8
RUNS = 3
OUT = PROJECT_ROOT / "docs" / "SALTING.md"


def with_salt(df: DataFrame) -> DataFrame:
    # Deterministic salt from the row's own key, so runs are reproducible.
    return df.withColumn("salt", F.pmod(F.xxhash64("order_id", "order_item_id", "rep"), F.lit(SALT)))


def sum_plain(df: DataFrame, _states: DataFrame) -> DataFrame:
    return df.groupBy("customer_state").agg(F.sum("price").alias("revenue"), F.count(F.lit(1)).alias("lines"))


def sum_salted(df: DataFrame, _states: DataFrame) -> DataFrame:
    partial = (
        with_salt(df).groupBy("customer_state", "salt").agg(F.sum("price").alias("r"), F.count(F.lit(1)).alias("n"))
    )
    return partial.groupBy("customer_state").agg(F.sum("r").alias("revenue"), F.sum("n").alias("lines"))


def join_plain(df: DataFrame, states: DataFrame) -> DataFrame:
    return df.join(states, "customer_state").select(
        "customer_state",
        "order_id",
        "order_item_id",
        "rep",
        (F.col("price") / F.col("state_avg_price")).alias("price_index"),
    )


def join_salted(df: DataFrame, states: DataFrame) -> DataFrame:
    replicated = states.crossJoin(F.broadcast(df.sparkSession.range(SALT).withColumnRenamed("id", "salt")))
    return (
        with_salt(df)
        .join(replicated, ["customer_state", "salt"])
        .select(
            "customer_state",
            "order_id",
            "order_item_id",
            "rep",
            (F.col("price") / F.col("state_avg_price")).alias("price_index"),
        )
    )


QUERIES = {
    "(a) sum+count by state": (sum_plain, sum_salted),
    "(b) sort-merge join on state": (join_plain, join_salted),
}
AQE_SKEW_JOIN = {  # thresholds lowered so AQE treats this laptop-sized partition as skewed
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "2",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "1MB",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "1MB",
}


def stage_task_times(spark: SparkSession, group: str) -> dict:
    """For the jobs in `group`: the post-shuffle stage with the slowest task (rows read and run time
    per task), via Spark's REST API."""
    sc = spark.sparkContext
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}"
    busiest = None
    for job_id in sc.statusTracker().getJobIdsForGroup(group):
        for stage_id in sc.statusTracker().getJobInfo(job_id).stageIds:
            try:
                tasks = json.load(urllib.request.urlopen(f"{base}/stages/{stage_id}/0/taskList?length=10000"))
            except Exception:  # noqa: BLE001 - skipped stages have no task list
                continue
            metrics = [t["taskMetrics"] for t in tasks if t.get("taskMetrics")]
            reads = [m["shuffleReadMetrics"]["recordsRead"] for m in metrics]
            if not metrics or sum(reads) == 0:
                continue  # not a post-shuffle stage
            times = [m["executorRunTime"] for m in metrics]
            if busiest is None or max(times) > busiest["max_ms"]:
                busiest = {
                    "stage": stage_id,
                    "tasks": len(times),
                    "max_ms": max(times),
                    "median_ms": statistics.median(times),
                    "max_rows": max(reads),
                    "median_rows": statistics.median(reads),
                }
    return busiest or {}


def measure(spark: SparkSession, df: DataFrame, label: str) -> dict:
    walls = []
    for i in range(RUNS):
        group = f"{label}-{i}"
        spark.sparkContext.setJobGroup(group, group)
        t0 = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()  # full computation, nothing written
        walls.append(time.perf_counter() - t0)
    return {"wall_s": statistics.median(walls), "walls": walls, **stage_task_times(spark, group)}


def summary(df: DataFrame) -> list[tuple]:
    """Per-state row count and total of the last column, to compare plain and salted results."""
    measure_col = df.columns[-1]
    return sorted(
        map(tuple, df.groupBy("customer_state").agg(F.count(F.lit(1)), F.round(F.sum(measure_col), 4)).collect())
    )


def partition_sizes(df: DataFrame, *cols: str) -> list[int]:
    """Rows in each of the shuffle partitions (empty partitions included as 0)."""
    sizes = {
        r["p"]: r["count"]
        for r in df.repartition(SHUFFLE_PARTITIONS, *cols).groupBy(F.spark_partition_id().alias("p")).count().collect()
    }
    return [sizes.get(i, 0) for i in range(SHUFFLE_PARTITIONS)]


def explain(df: DataFrame) -> str:
    text = df._sc._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    return text.split("\n\n(1)")[0].strip()  # the operator tree, without the per-node details


def main() -> None:
    cfg = load_config()
    spark = build_spark(
        cfg,
        "salting-demo",
        {"spark.ui.enabled": "true", "spark.ui.port": "4050", "spark.ui.showConsoleProgress": "false"},
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    # Force sort-merge joins: with broadcasting the small side, there would be no skewed shuffle.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
    fact = spark.read.parquet(layer_dir(cfg["lake"]["root"], "gold", "fact_order_lines")).select(
        "customer_state", "order_id", "order_item_id", "price"
    )
    big = fact.crossJoin(F.broadcast(spark.range(SCALE).withColumnRenamed("id", "rep"))).cache()
    rows = big.count()
    # A small per-state table. Built as a local DataFrame on purpose: if it were cached straight from a
    # groupBy on customer_state it would already be hash-partitioned on the key, Spark would not shuffle
    # it, and AQE's skew-join rule (which needs a shuffle on both sides) would silently not apply.
    states = spark.createDataFrame(big.groupBy("customer_state").agg(F.avg("price").alias("state_avg_price")).collect())
    shares = {r[0]: r[1] / rows for r in big.groupBy("customer_state").count().collect()}
    top_states = sorted(shares.items(), key=lambda kv: -kv[1])[:4]
    sizes_plain = partition_sizes(big, "customer_state")
    sizes_salted = partition_sizes(with_salt(big), "customer_state", "salt")

    results, plans, same = {}, {}, {}
    for name, (plain, salty) in QUERIES.items():
        a, b = plain(big, states), salty(big, states)
        same[name] = summary(a) == summary(b)
        plans[name] = (explain(a), explain(b))
        for label, conf in (
            ("off", {"spark.sql.adaptive.enabled": "false"}),
            ("on (skew join)", {"spark.sql.adaptive.enabled": "true", **AQE_SKEW_JOIN}),
        ):
            for k, v in conf.items():
                spark.conf.set(k, v)
            for variant, df in (("plain", a), ("salted", b)):
                results[(name, label, variant)] = measure(spark, df, f"{name}-{label}-{variant}")

                print(name, f"AQE {label}", variant, results[(name, label, variant)], flush=True)
        spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.stop()
    write_report(rows, top_states, sizes_plain, sizes_salted, results, plans, same)


def write_report(rows, top_states, sizes_plain, sizes_salted, results, plans, same) -> None:
    aqe_labels = ("off", "on (skew join)")

    def row(name, aqe, variant):
        r = results[(name, aqe, variant)]
        return (
            f"| {name} | {aqe} | {variant} | {r['wall_s']:.2f} | {r['tasks']} | "
            f"{r['max_rows']:,} / {int(r['median_rows']):,} | {r['max_ms']} / {int(r['median_ms'])} |"
        )

    def change(name, aqe):
        p, s = results[(name, aqe, "plain")], results[(name, aqe, "salted")]
        return (
            f"wall {p['wall_s']:.2f}s → {s['wall_s']:.2f}s, busiest task reads {p['max_rows']:,} → "
            f"{s['max_rows']:,} rows and runs {p['max_ms']} → {s['max_ms']} ms"
        )

    a, b = list(QUERIES)
    jp_off, js_off = results[(b, "off", "plain")], results[(b, "off", "salted")]
    jp_on = results[(b, "on (skew join)", "plain")]
    lines = [
        "# Salting demo: skew on customer_state",
        "",
        "Generated by `make salting-demo` (`scripts/salting_demo.py`). Setup: Spark 3.5.9 on this laptop, "
        f"`local[2]`, 2 GB driver, {SHUFFLE_PARTITIONS} shuffle partitions, salt factor {SALT}. "
        f"The input is `gold.fact_order_lines` (orders through 2017-12-31) replicated ×{SCALE}, giving {rows:,} rows. "
        "Replicating keeps the state mix: " + ", ".join(f"{s} {v:.1%}" for s, v in top_states) + ".",
        "",
        "## Where the rows go",
        "",
        f"Rows per shuffle partition when hashing on `customer_state` (the plain queries): `{sizes_plain}`. "
        f"The largest partition holds {max(sizes_plain) / rows:.0%} of all rows. Hashing on `(customer_state, salt)` "
        f"gives `{sizes_salted}`, and the largest holds {max(sizes_salted) / rows:.0%}.",
        "",
        "## Results",
        "",
        "Wall time is the median of 3 runs. Rows and run time are per task, for the stage after the shuffle "
        "(busiest task / median task).",
        "",
        "| Query | AQE | Variant | Wall (s) | Tasks | Rows read: max / median | Task ms: max / median |",
        "|---|---|---|---|---|---|---|",
        *[row(n, aqe, v) for n in QUERIES for aqe in aqe_labels for v in ("plain", "salted")],
        "",
        "The salted results are identical to the plain ones (per-state counts and totals): "
        f"{a}: {same[a]}, {b}: {same[b]}.",
        "",
        "## What the numbers say",
        "",
        f"- **{a}**, AQE off: {change(a, 'off')}.",
        "  - Spark pre-aggregates each input partition before the shuffle, so every state sends only one row "
        "per partition.",
        "  - The skew never reaches the shuffle, and salting just adds a second aggregation.",
        "  - **Don't salt combinable aggregations** (sum, count, min, max, avg).",
        "  - The same goes for `countDistinct`, which Spark shuffles by (key, value), and for `row_number() <= N`",
        "    windows, where Spark 3.5 applies a per-partition limit before the shuffle (WindowGroupLimit).",
        f"- **{b}**, AQE off: {change(b, 'off')}.",
        f"  - In a sort-merge join every row of a key goes to one task, so {top_states[0][0]}'s "
        f"{top_states[0][1]:.0%} of the rows lands on a single task.",
        "  - Salting the big side and replicating the small side ×N spreads SP over N tasks.",
        f"  - The cost is {SALT}× more rows on the small side, plus an extra column.",
        f"- **AQE with skew-join handling** (thresholds lowered to suit laptop data): plain join "
        f"wall {jp_on['wall_s']:.2f}s, busiest task {jp_on['max_rows']:,} rows / {jp_on['max_ms']} ms "
        f"(vs {jp_off['max_rows']:,} rows / {jp_off['max_ms']} ms with AQE off).",
        f"  - AQE split the skewed partition: the join stage ran {jp_on['tasks']} tasks instead of "
        f"{SHUFFLE_PARTITIONS}, with no code change.",
        "  - AQE splits the skewed partition at runtime and replicates the matching partition of the other side.",
        "  - Gotcha found while building this demo: AQE's skew-join rule needs a shuffle on *both* sides.",
        "    The first version cached the small side straight from a `groupBy(customer_state)`, so it was already",
        "    hash-partitioned on the key and Spark skipped its shuffle. AQE then left the skewed join alone",
        "    (no `AQEShuffleRead` in the plan), even with `forceOptimizeSkewedJoin`.",
        "  - That is the same idea as salting, done automatically. On Spark 3.x, AQE skew joins are the first tool;",
        "    manual salting is for when AQE can't apply (the skew is under its thresholds, a non-join operator, "
        "or older Spark).",
        "- **Why the timings move less than the row counts**:",
        "  - `local[2]` has two cores, so balancing can speed things up at most about 2×.",
        f"  - At {rows:,} rows the stages take milliseconds, and fixed costs dominate.",
        f"  - The row counts show the effect directly: the busiest join task reads {jp_off['max_rows']:,} rows "
        f"plain and {js_off['max_rows']:,} salted.",
        "  - On a cluster with dozens of executors and a skewed key of hundreds of GB, that busiest task is the "
        "job's runtime.",
        "",
        "## Plans (AQE off)",
        "",
        "Compare the `Exchange hashpartitioning` keys: `customer_state` alone vs `customer_state, salt`.",
        "",
    ]
    for name, (p, s) in plans.items():
        lines += [f"### {name}: plain", "", "```", p, "```", "", f"### {name}: salted", "", "```", s, "```", ""]
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
