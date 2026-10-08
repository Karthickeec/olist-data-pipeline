CREATE EXTERNAL TABLE `olist_lake`.`bronze_customers` (
  `customer_id` string,
  `customer_unique_id` string,
  `customer_zip_code_prefix` string,
  `customer_city` string,
  `customer_state` string,
  `updated_at` timestamp,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/olist_postgres/customers/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/olist_postgres/customers/ingest_date=${ingest_date}/'
)
