CREATE EXTERNAL TABLE `olist_lake`.`silver_orders` (
  `customer_id` string,
  `order_id` string,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `order_approved_at` timestamp,
  `order_delivered_carrier_date` timestamp,
  `order_delivered_customer_date` timestamp,
  `order_estimated_delivery_date` timestamp,
  `updated_at` timestamp,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/orders/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/orders/order_purchase_date=${order_purchase_date}/'
)
