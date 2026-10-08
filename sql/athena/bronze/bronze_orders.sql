CREATE EXTERNAL TABLE `olist_lake`.`bronze_orders` (
  `order_id` string,
  `customer_id` string,
  `order_status` string,
  `order_purchase_timestamp` timestamp,
  `order_approved_at` timestamp,
  `order_delivered_carrier_date` timestamp,
  `order_delivered_customer_date` timestamp,
  `order_estimated_delivery_date` timestamp,
  `updated_at` timestamp,
  `_ingested_at` timestamp,
  `_batch_date` date,
  `_source` string
)
PARTITIONED BY (`ingest_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/bronze/olist_postgres/orders/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.ingest_date.type'='date',
  'projection.ingest_date.format'='yyyy-MM-dd',
  'projection.ingest_date.range'='2016-09-01,2018-12-31',
  'projection.ingest_date.interval'='1',
  'projection.ingest_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/bronze/olist_postgres/orders/ingest_date=${ingest_date}/'
)
