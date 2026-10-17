-- =====================================================================================
-- mart_order_facts: order-grain fact used by every other mart (the ELT twin of etl_fact_orders)
--   only orders that passed every Critical check; measures follow decisions D2 / D3
-- =====================================================================================
CREATE OR REPLACE TABLE mart_order_facts AS
SELECT
    o.order_id, o.order_ts, o.order_date, o.customer_id, o.store_id, o.product_id,
    o.quantity, o.unit_price, o.discount_pct, o.order_amount, o.payment_mode, o.order_status, o.channel,
    o.delivery_minutes, o.dq_warnings,
    coalesce(r.recon_status, 'NOT_APPLICABLE')                                       AS recon_status,
    -- D2: delivered but unpaid / payment unknown -> not revenue yet
    o.order_status = 'DELIVERED'
      AND coalesce(r.recon_status, '') NOT IN ('DELIVERED_PAYMENT_FAILED', 'DELIVERED_NO_PAYMENT',
                                               'AWAITING_PAYMENT_FEED')               AS revenue_eligible,
    CASE WHEN revenue_eligible THEN round(o.order_amount, 2) ELSE 0 END              AS net_revenue,
    CASE WHEN revenue_eligible THEN round(o.quantity * p.cost_price, 2) ELSE 0 END   AS cost_amount,   -- NULL if no trusted cost
    round(net_revenue - cost_amount, 2)                                              AS gross_margin,
    -- D3: only plausible, recorded delivery times count towards the SLA
    o.order_status = 'DELIVERED' AND o.delivery_minutes IS NOT NULL AND o.delivery_minutes <= 120 AS sla_eligible,
    CASE WHEN sla_eligible THEN o.delivery_minutes <= 15 END                         AS within_15_min,
    o._source_file, o._run_id, o._loaded_at
FROM stg_orders o
LEFT JOIN (SELECT DISTINCT ON (order_id) order_id, recon_status FROM mart_payment_reconciliation
           WHERE recon_status NOT IN ('ORPHAN_PAYMENT', 'DUPLICATE_PAYMENT')) r USING (order_id)
LEFT JOIN stg_products p USING (product_id)
WHERE o.dq_status <> 'FAIL';
