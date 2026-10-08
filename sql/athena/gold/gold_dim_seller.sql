CREATE EXTERNAL TABLE `olist_lake`.`gold_dim_seller` (
  `seller_sk` bigint,
  `seller_id` string,
  `seller_zip_code_prefix` string,
  `seller_city` string,
  `seller_state` string,
  `lat` double,
  `lng` double,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/dim_seller/'
