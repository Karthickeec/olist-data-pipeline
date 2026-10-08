CREATE EXTERNAL TABLE `olist_lake`.`silver_customers` (
  `customer_id` string,
  `customer_unique_id` string,
  `customer_zip_code_prefix` string,
  `customer_city` string,
  `customer_state` string,
  `updated_at` timestamp,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/customers/'
