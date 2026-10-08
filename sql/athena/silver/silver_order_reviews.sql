CREATE EXTERNAL TABLE `olist_lake`.`silver_order_reviews` (
  `order_id` string,
  `review_id` string,
  `review_score` smallint,
  `review_comment_title` string,
  `review_comment_message` string,
  `review_creation_date` timestamp,
  `review_answer_timestamp` timestamp,
  `updated_at` timestamp,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`review_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/order_reviews/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.review_date.type'='date',
  'projection.review_date.format'='yyyy-MM-dd',
  'projection.review_date.range'='2016-09-01,2018-12-31',
  'projection.review_date.interval'='1',
  'projection.review_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/order_reviews/review_date=${review_date}/'
)
