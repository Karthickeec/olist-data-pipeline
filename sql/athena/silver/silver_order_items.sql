CREATE EXTERNAL TABLE `olist_lake`.`silver_order_items` (
  `order_id` string,
  `order_item_id` int,
  `product_id` string,
  `seller_id` string,
  `shipping_limit_date` timestamp,
  `price` decimal(10,2),
  `freight_value` decimal(10,2),
  `updated_at` timestamp,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/order_items/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/order_items/order_purchase_date=${order_purchase_date}/'
)
