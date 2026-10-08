CREATE EXTERNAL TABLE `olist_lake`.`bronze_order_reviews` (
  `review_id` string,
  `order_id` string,
  `review_score` smallint,
  `review_comment_title` string,
  `review_comment_message` string,
  `review_creation_date` timestamp,
  `review_answer_timestamp` timestamp,
  `updated_at` timestamp,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/olist_postgres/order_reviews/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/olist_postgres/order_reviews/ingest_date=${ingest_date}/'
)
