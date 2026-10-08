CREATE EXTERNAL TABLE `olist_lake`.`gold_dim_date` (
  `date_key` int,
  `date` date,
  `year` int,
  `quarter` int,
  `month` int,
  `month_name` string,
  `day` int,
  `week_of_year` int,
  `day_name` string,
  `is_weekend` boolean,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/dim_date/'
