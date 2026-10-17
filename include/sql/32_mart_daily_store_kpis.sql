-- =====================================================================================
-- mart_daily_store_kpis  (business questions 1 and 3; bonus: store capacity check)
-- =====================================================================================
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    f.order_date, s.zone, s.store_id, s.store_name,
    count(*)                                                             AS orders_placed,
    count(*) FILTER (WHERE f.revenue_eligible)                           AS delivered_orders,
    round(sum(f.net_revenue), 2)                                         AS net_revenue,
    round(sum(f.order_amount) FILTER (WHERE f.order_status = 'DELIVERED' AND NOT f.revenue_eligible), 2)
                                                                         AS held_unpaid_revenue,
    round(avg(f.delivery_minutes) FILTER (WHERE f.sla_eligible), 1)      AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE f.within_15_min)
          / nullif(count(*) FILTER (WHERE f.sla_eligible), 0), 1)        AS pct_within_15_min,
    s.capacity_orders_per_day,
    round(100.0 * count(*) / nullif(s.capacity_orders_per_day, 0), 1)    AS capacity_utilisation_pct
FROM mart_order_facts f
JOIN stg_stores s USING (store_id)
GROUP BY f.order_date, s.zone, s.store_id, s.store_name, s.capacity_orders_per_day
ORDER BY f.order_date, s.zone, s.store_id;
