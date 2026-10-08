CREATE EXTERNAL TABLE `olist_lake`.`bronze_geolocation` (
  `geolocation_row_id` bigint,
  `geolocation_zip_code_prefix` string,
  `geolocation_lat` double,
  `geolocation_lng` double,
  `geolocation_city` string,
  `geolocation_state` string,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/olist_postgres/geolocation/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/olist_postgres/geolocation/ingest_date=${ingest_date}/'
)
