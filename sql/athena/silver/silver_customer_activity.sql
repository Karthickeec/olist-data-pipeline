CREATE EXTERNAL TABLE `olist_lake`.`silver_customer_activity` (
  `customer_unique_id` string,
  `sessions` int,
  `page_views` int,
  `cart_adds` int,
  `support_tickets` int,
  `last_seen_at` timestamp,
  `device` string,
  `_timestamp_reformatted` boolean,
  `_page` bigint,
  `_source_file` string,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`activity_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/customer_activity/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.activity_date.type'='date',
  'projection.activity_date.format'='yyyy-MM-dd',
  'projection.activity_date.range'='2016-09-01,2018-12-31',
  'projection.activity_date.interval'='1',
  'projection.activity_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/customer_activity/activity_date=${activity_date}/'
)
