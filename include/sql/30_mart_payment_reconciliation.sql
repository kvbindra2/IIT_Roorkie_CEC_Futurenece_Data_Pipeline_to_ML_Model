-- =====================================================================================
-- mart_payment_reconciliation: every order (and every unmatched payment) with a recon_status.
-- Same classification as include/dq_rules.py::reconcile (decisions D1, D2):
--   variance = cash collected - cash we should have collected  (negative = money not received)
-- =====================================================================================
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH o AS (
    SELECT order_id, order_ts, order_status, payment_mode, order_amount AS expected_amount, formula_amount,
           CASE WHEN dq_status = 'FAIL' THEN failure_reason END AS dq_failure_reason
    FROM stg_orders
),
pay AS (
    SELECT payment_id, order_id, num(amount_paid) AS amount, ts_strict(payment_ts) AS pts,
           upper(trim(payment_status)) AS pst, _row_num,
           row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) > 1 AS is_dup
    FROM raw_payments WHERE _run_id = getvariable('run_id')
),
best AS (               -- one payment per order: SUCCESS, then REFUNDED, then FAILED
    SELECT * FROM pay WHERE NOT is_dup
    QUALIFY row_number() OVER (PARTITION BY order_id
            ORDER BY CASE pst WHEN 'SUCCESS' THEN 0 WHEN 'REFUNDED' THEN 1 WHEN 'FAILED' THEN 2 ELSE 3 END, _row_num) = 1
),
feed AS (               -- the payment export is complete up to its latest valid payment_ts
    SELECT max(pts) AS cutoff FROM pay WHERE pts <= biz_date('payments.csv') + INTERVAL 1 DAY
),
per_order AS (
    SELECT o.order_id, b.payment_id, o.order_status, b.pst AS payment_status, o.payment_mode,
           o.expected_amount, b.amount AS received_amount, o.formula_amount, o.dq_failure_reason,
           CASE
             WHEN contains(coalesce(o.dq_failure_reason, ''), 'orders_quantity_positive') THEN 'NEGATIVE_ORDER'
             WHEN contains(coalesce(o.dq_failure_reason, ''), 'orders_amount_formula')    THEN 'PRICING_ERROR_AT_CHECKOUT'
             WHEN o.dq_failure_reason IS NOT NULL
                  THEN CASE WHEN b.payment_id IS NOT NULL THEN 'PAYMENT_FOR_QUARANTINED_ORDER' END
             WHEN o.order_status = 'CANCELLED'
                  THEN CASE WHEN b.payment_id IS NOT NULL THEN 'CANCELLED_WITH_PAYMENT' END
             WHEN o.order_status = 'RETURNED'
                  THEN CASE WHEN b.pst = 'REFUNDED' THEN 'REFUND_MATCHED' ELSE 'RETURNED_NO_REFUND' END
             WHEN b.payment_id IS NULL
                  THEN CASE WHEN o.order_ts > (SELECT cutoff FROM feed) THEN 'AWAITING_PAYMENT_FEED'
                            ELSE 'DELIVERED_NO_PAYMENT' END
             WHEN b.pst = 'FAILED' THEN 'DELIVERED_PAYMENT_FAILED'
             WHEN abs(round(o.expected_amount - b.amount, 2)) <= 0.01 THEN 'MATCHED'
             WHEN round(o.expected_amount - b.amount, 2) IN (10, 25, 50, 100) THEN 'PROMO_SHORTFALL'
             ELSE 'AMOUNT_MISMATCH'
           END AS recon_status
    FROM o LEFT JOIN best b USING (order_id)
),
unmatched AS (
    SELECT order_id, payment_id, NULL AS order_status, pst AS payment_status, NULL AS payment_mode,
           NULL::DOUBLE AS expected_amount, amount AS received_amount, NULL::DOUBLE AS formula_amount,
           NULL AS dq_failure_reason, 'ORPHAN_PAYMENT' AS recon_status
    FROM pay WHERE order_id NOT IN (SELECT order_id FROM o)
    UNION ALL
    SELECT order_id, payment_id, NULL, pst, NULL, NULL, amount, NULL, NULL, 'DUPLICATE_PAYMENT'
    FROM pay WHERE is_dup
),
all_rows AS (
    SELECT * FROM per_order WHERE recon_status IS NOT NULL
    UNION ALL BY NAME SELECT * FROM unmatched
)
SELECT a.*,
       round(CASE WHEN payment_status = 'SUCCESS' THEN coalesce(received_amount, 0) ELSE 0 END
             - CASE WHEN recon_status = 'PRICING_ERROR_AT_CHECKOUT' THEN formula_amount
                    WHEN order_status = 'DELIVERED' THEN coalesce(expected_amount, 0) ELSE 0 END, 2) AS variance,
       r.owner,
       getvariable('run_id') AS _run_id
FROM all_rows a LEFT JOIN ref_recon_owner r USING (recon_status);
