-- Top 10 sellers by revenue (excluding canceled/unavailable orders), with their state and order count
SELECT s.seller_id, s.seller_state, count(DISTINCT f.order_id) AS orders,
       sum(f.price) AS revenue, round(avg(f.price), 2) AS avg_item_price
FROM gold_fact_order_lines f JOIN gold_dim_seller s ON f.seller_sk = s.seller_sk
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY s.seller_id, s.seller_state
ORDER BY revenue DESC
LIMIT 10
