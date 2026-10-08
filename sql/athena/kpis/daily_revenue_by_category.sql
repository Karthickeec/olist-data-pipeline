-- Revenue, orders and items per category over the last 7 days of data (Gold aggregate, partition-pruned)
SELECT order_purchase_date, category, orders, items, revenue, avg_item_price
FROM gold_agg_daily_category_sales
WHERE order_purchase_date > (SELECT max(order_purchase_date) FROM gold_agg_daily_category_sales) - INTERVAL '7' DAY
ORDER BY order_purchase_date DESC, revenue DESC
