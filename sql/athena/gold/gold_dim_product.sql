CREATE EXTERNAL TABLE `olist_lake`.`gold_dim_product` (
  `product_sk` bigint,
  `product_id` string,
  `product_category_name` string,
  `product_category_name_english` string,
  `product_photos_qty` int,
  `product_weight_g` int,
  `product_length_cm` int,
  `product_height_cm` int,
  `product_width_cm` int,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/gold/dim_product/'
