CREATE EXTERNAL TABLE `olist_lake`.`gold_agg_daily_category_sales` (
  `category` string,
  `orders` bigint,
  `items` bigint,
  `revenue` decimal(20,2),
  `freight` decimal(20,2),
  `gmv` decimal(22,2),
  `avg_item_price` decimal(12,2),
  `_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/agg_daily_category_sales/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/gold/agg_daily_category_sales/order_purchase_date=${order_purchase_date}/'
)
