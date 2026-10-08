CREATE EXTERNAL TABLE `olist_lake`.`silver_sellers` (
  `seller_id` string,
  `seller_zip_code_prefix` string,
  `seller_city` string,
  `seller_state` string,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/sellers/'
