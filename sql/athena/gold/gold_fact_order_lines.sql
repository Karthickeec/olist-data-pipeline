CREATE EXTERNAL TABLE `olist_lake`.`gold_fact_order_lines` (
  `order_id` string,
  `order_item_id` int,
  `date_key` int,
  `customer_sk` bigint,
  `customer_unique_id` string,
  `product_sk` bigint,
  `seller_sk` bigint,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `customer_state` string,
  `payment_type_main` string,
  `payment_installments_max` int,
  `price` decimal(10,2),
  `freight_value` decimal(10,2),
  `line_total` decimal(12,2),
  `allocated_payment` decimal(12,2),
  `_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/fact_order_lines/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/gold/fact_order_lines/order_purchase_date=${order_purchase_date}/'
)
