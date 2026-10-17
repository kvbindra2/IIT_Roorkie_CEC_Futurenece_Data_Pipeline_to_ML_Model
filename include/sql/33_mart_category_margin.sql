-- =====================================================================================
-- mart_category_margin  (business question 2)   margin = revenue - quantity x cost_price
-- =====================================================================================
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    count(*) FILTER (WHERE f.revenue_eligible)                              AS delivered_orders,
    round(sum(f.net_revenue), 2)                                            AS net_revenue,
    round(sum(f.cost_amount), 2)                                            AS cost,
    round(sum(f.gross_margin), 2)                                           AS gross_margin,
    round(100 * sum(f.gross_margin)
          / nullif(sum(f.net_revenue) FILTER (WHERE f.gross_margin IS NOT NULL), 0), 1) AS margin_pct,
    count(*) FILTER (WHERE f.revenue_eligible AND f.cost_amount IS NULL)    AS orders_without_cost
FROM mart_order_facts f
JOIN stg_products p USING (product_id)
GROUP BY ALL
ORDER BY net_revenue DESC;
