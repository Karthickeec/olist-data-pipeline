CREATE EXTERNAL TABLE `olist_lake`.`bronze_order_items` (
  `order_id` string,
  `order_item_id` int,
  `product_id` string,
  `seller_id` string,
  `shipping_limit_date` timestamp,
  `price` decimal(10,2),
  `freight_value` decimal(10,2),
  `updated_at` timestamp,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/olist_postgres/order_items/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/olist_postgres/order_items/ingest_date=${ingest_date}/'
)
