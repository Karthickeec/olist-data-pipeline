-- RFM segment mix on the latest as-of date: customers, share, lifetime value and average order value
WITH latest AS (SELECT max(as_of_date) AS d FROM gold_customer_metrics)
SELECT m.as_of_date, m.rfm_segment, count(*) AS customers,
       round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_customers,
       sum(m.lifetime_value) AS lifetime_value, round(avg(m.avg_order_value), 2) AS avg_order_value
FROM gold_customer_metrics m JOIN latest ON m.as_of_date = latest.d
GROUP BY m.as_of_date, m.rfm_segment
ORDER BY lifetime_value DESC
