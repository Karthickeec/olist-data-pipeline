CREATE EXTERNAL TABLE `olist_lake`.`silver_geolocation` (
  `zip_code_prefix` string,
  `lat` double,
  `lng` double,
  `city` string,
  `state` string,
  `source_rows` bigint,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/geolocation/'
