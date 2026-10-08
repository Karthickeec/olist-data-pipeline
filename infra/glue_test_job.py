"""Smallest Glue Spark job: read gold/dim_date from the S3 lake, write a one-row JSON result.

Proves that Glue ETL can run Spark against the lake (Step 9 depends on it). Arguments: --INPUT, --OUTPUT.
"""
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["INPUT", "OUTPUT"])
spark = SparkSession.builder.getOrCreate()
dim = spark.read.parquet(args["INPUT"])
result = dim.agg(F.count("*").alias("dim_date_rows"),
                 F.min("date").cast("string").alias("first_date"),
                 F.max("date").cast("string").alias("last_date"))
result = result.withColumn("spark_version", F.lit(spark.version))
result.coalesce(1).write.mode("overwrite").json(args["OUTPUT"])
print(result.first().asDict())
