CREATE EXTERNAL TABLE `olist_lake`.`silver_products` (
  `product_id` string,
  `product_category_name` string,
  `product_category_name_english` string,
  `product_name_length` int,
  `product_description_length` int,
  `product_photos_qty` int,
  `product_weight_g` int,
  `product_length_cm` int,
  `product_height_cm` int,
  `product_width_cm` int,
  `_batch_date` date,
  `_first_batch_date` date,
  `_processed_at` timestamp
)
STORED AS PARQUET
LOCATION '{{LAKE}}/silver/products/'
