-- SCD2: customers whose address history crosses states, by from-state -> to-state
WITH v AS (
  SELECT customer_unique_id, state, version,
         lead(state) OVER (PARTITION BY customer_unique_id ORDER BY version) AS next_state
  FROM gold_dim_customer
)
SELECT state AS from_state, next_state AS to_state, count(*) AS moves
FROM v
WHERE next_state IS NOT NULL AND next_state <> state
GROUP BY 1, 2
ORDER BY moves DESC
LIMIT 10
