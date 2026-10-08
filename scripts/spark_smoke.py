"""Smoke test for local Spark: Java, Parquet round trip, and a JDBC read from Postgres."""

import sys
import tempfile

from olist_pipeline.config import load_config
from olist_pipeline.spark import build_spark, read_jdbc


def main() -> None:
    cfg = load_config()
    spark = build_spark(cfg, "spark-smoke")
    ok = True
    try:
        jvm = spark.sparkContext._jvm
        print(
            f"Spark {spark.version}, Java {jvm.System.getProperty('java.version')}, "
            f"master {spark.sparkContext.master}, "
            f"time zone {spark.conf.get('spark.sql.session.timeZone')}"
        )

        with tempfile.TemporaryDirectory() as tmp:
            spark.range(5).write.parquet(f"{tmp}/smoke")
            n = spark.read.parquet(f"{tmp}/smoke").count()
        print(f"Parquet round trip: {n} rows {'OK' if n == 5 else 'FAILED'}")
        ok &= n == 5

        try:
            schema = cfg["pg"]["schema"]
            row = read_jdbc(spark, cfg, query=f'SELECT count(*) AS n FROM "{schema}"."products"').first()
            print(f"JDBC read: {schema}.products has {row['n']} rows OK")
        except Exception as e:  # noqa: BLE001 - report any connection problem plainly
            print(f"JDBC read FAILED (is Postgres up and seeded? `make up seed`): {e}".splitlines()[0])
            ok = False
    finally:
        spark.stop()
    print("PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
