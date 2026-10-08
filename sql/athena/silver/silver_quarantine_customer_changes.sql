CREATE EXTERNAL TABLE `olist_lake`.`silver_quarantine_customer_changes` (
  `change_id` string,
  `customer_unique_id` string,
  `new_zip_code_prefix` string,
  `new_city` string,
  `new_state` string,
  `requested_at` string,
  `source` string,
  `_corrupt_record` string,
  `_source_file` string,
  `_bronze_ingest_date` date,
  `_reason` string
)
PARTITIONED BY (`batch_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/_quarantine/customer_changes/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.batch_date.type'='date',
  'projection.batch_date.format'='yyyy-MM-dd',
  'projection.batch_date.range'='2016-09-01,2018-12-31',
  'projection.batch_date.interval'='1',
  'projection.batch_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/_quarantine/customer_changes/batch_date=${batch_date}/'
)
