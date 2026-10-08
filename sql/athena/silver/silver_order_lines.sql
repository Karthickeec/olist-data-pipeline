CREATE EXTERNAL TABLE `olist_lake`.`silver_order_lines` (
  `order_id` string,
  `order_item_id` int,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `order_approved_at` timestamp,
  `order_delivered_carrier_date` timestamp,
  `order_delivered_customer_date` timestamp,
  `order_estimated_delivery_date` timestamp,
  `customer_id` string,
  `customer_unique_id` string,
  `customer_zip_code_prefix` string,
  `customer_city` string,
  `customer_state` string,
  `product_id` string,
  `product_category_name` string,
  `product_category_name_english` string,
  `seller_id` string,
  `seller_zip_code_prefix` string,
  `seller_city` string,
  `seller_state` string,
  `shipping_limit_date` timestamp,
  `price` decimal(10,2),
  `freight_value` decimal(10,2),
  `line_total` decimal(12,2),
  `payment_total` decimal(20,2),
  `payment_count` bigint,
  `payment_type_main` string,
  `payment_installments_max` int,
  `allocated_payment` decimal(12,2),
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`order_purchase_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/order_lines/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_purchase_date.type'='date',
  'projection.order_purchase_date.format'='yyyy-MM-dd',
  'projection.order_purchase_date.range'='2016-09-01,2018-12-31',
  'projection.order_purchase_date.interval'='1',
  'projection.order_purchase_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/silver/order_lines/order_purchase_date=${order_purchase_date}/'
)
