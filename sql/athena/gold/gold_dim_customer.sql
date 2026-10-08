CREATE EXTERNAL TABLE `olist_lake`.`gold_dim_customer` (
  `customer_sk` bigint,
  `customer_unique_id` string,
  `zip_code_prefix` string,
  `city` string,
  `state` string,
  `valid_from` timestamp,
  `valid_to` timestamp,
  `is_current` boolean,
  `version` int,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/dim_customer/'
