CREATE EXTERNAL TABLE `olist_lake`.`bronze_customer_changes` (
  `change_id` string,
  `customer_unique_id` string,
  `new_zip_code_prefix` string,
  `new_city` string,
  `new_state` string,
  `requested_at` string,
  `source` string,
  `_corrupt_record` string,
  `_source_file` string,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/crm/customer_changes/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/crm/customer_changes/ingest_date=${ingest_date}/'
)
