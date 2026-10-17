-- =====================================================================================
-- mart_top_customers  (business question 5) - masked identity only (decisions Q3 / Q4)
-- =====================================================================================
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    rank() OVER (ORDER BY sum(f.net_revenue) DESC)       AS spend_rank,
    c.customer_id, c.customer_name, c.loyalty_tier, c.email_masked, c.phone_masked,
    count(*) FILTER (WHERE f.revenue_eligible)           AS delivered_orders,
    round(sum(f.net_revenue), 2)                         AS total_spend
FROM mart_order_facts f
JOIN stg_customers c USING (customer_id)
GROUP BY c.customer_id, c.customer_name, c.loyalty_tier, c.email_masked, c.phone_masked
QUALIFY spend_rank <= 10
ORDER BY spend_rank;
