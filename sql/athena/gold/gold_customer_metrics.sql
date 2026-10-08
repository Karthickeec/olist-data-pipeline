CREATE EXTERNAL TABLE `olist_lake`.`gold_customer_metrics` (
  `customer_unique_id` string,
  `lifetime_value` decimal(14,2),
  `order_count` bigint,
  `first_order_date` date,
  `last_order_date` date,
  `avg_order_value` decimal(12,2),
  `days_since_last_order` int,
  `r_score` int,
  `f_score` int,
  `m_score` int,
  `rfm_segment` string,
  `_batch_date` date,
  `_processed_at` timestamp
)
PARTITIONED BY (`as_of_date` date)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/customer_metrics/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.as_of_date.type'='date',
  'projection.as_of_date.format'='yyyy-MM-dd',
  'projection.as_of_date.range'='2016-09-01,2018-12-31',
  'projection.as_of_date.interval'='1',
  'projection.as_of_date.interval.unit'='DAYS',
  'storage.location.template'='{{LAKE}}/gold/customer_metrics/as_of_date=${as_of_date}/'
)
