CREATE EXTERNAL TABLE `olist_lake`.`silver_customer_changes` (
  `change_id` string,
  `customer_unique_id` string,
  `new_zip_code_prefix` string,
  `new_city` string,
  `new_state` string,
  `requested_at` timestamp,
  `source` string,
  `_state_fixed` boolean,
  `_city_filled` boolean,
  `_source_file` string,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`requested_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/customer_changes/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.requested_date.type'='date',
  'projection.requested_date.format'='yyyy-MM-dd',
  'projection.requested_date.range'='2016-09-01,2018-12-31',
  'projection.requested_date.interval'='1',
  'projection.requested_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/customer_changes/requested_date=${requested_date}/'
)
