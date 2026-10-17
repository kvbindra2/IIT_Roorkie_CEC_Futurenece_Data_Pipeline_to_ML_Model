-- =====================================================================================
-- mart_delivery_sla  (business question 3: the "10-minute promise")
-- =====================================================================================
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.zone, s.store_id, s.store_name,
    count(*) FILTER (WHERE f.order_status = 'DELIVERED')                    AS delivered_orders,
    count(*) FILTER (WHERE f.sla_eligible)                                  AS orders_with_valid_time,
    count(*) FILTER (WHERE f.order_status = 'DELIVERED' AND NOT f.sla_eligible) AS excluded_bad_or_missing_time,
    round(avg(f.delivery_minutes) FILTER (WHERE f.sla_eligible), 1)         AS avg_delivery_minutes,
    round(median(f.delivery_minutes) FILTER (WHERE f.sla_eligible), 1)      AS median_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE f.within_15_min)
          / nullif(count(*) FILTER (WHERE f.sla_eligible), 0), 1)           AS pct_within_15_min
FROM mart_order_facts f
JOIN stg_stores s USING (store_id)
GROUP BY ALL
ORDER BY pct_within_15_min DESC;
