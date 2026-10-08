CREATE EXTERNAL TABLE `olist_lake`.`silver_quarantine_customer_activity` (
  `customer_unique_id` string,
  `activity_date` string,
  `sessions` string,
  `page_views` string,
  `cart_adds` string,
  `support_tickets` string,
  `last_seen_at` string,
  `device` string,
  `_page` bigint,
  `_source_file` string,
  `_corrupt_record` string,
  `_bronze_ingest_date` date,
  `_reason` string
)
PARTITIONED BY (`batch_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/_quarantine/customer_activity/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.batch_date.type'='date',
  'projection.batch_date.format'='yyyy-MM-dd',
  'projection.batch_date.range'='2016-09-01,2018-12-31',
  'projection.batch_date.interval'='1',
  'projection.batch_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/_quarantine/customer_activity/batch_date=${batch_date}/'
)
