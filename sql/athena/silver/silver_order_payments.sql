CREATE EXTERNAL TABLE `olist_lake`.`silver_order_payments` (
  `order_id` string,
  `payment_sequential` int,
  `payment_type` string,
  `payment_installments` int,
  `payment_value` decimal(10,2),
  `updated_at` timestamp,
  `_bronze_ingest_date` date,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/order_payments/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/order_payments/order_purchase_date=${order_purchase_date}/'
)
