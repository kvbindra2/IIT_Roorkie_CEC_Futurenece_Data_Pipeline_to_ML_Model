-- =====================================================================================
-- stg_payments: typed, standardised, de-duplicated, with dq_status + failure_reason
-- FK / mode checks look at the orders received in this run (base file + increments)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS stg_dq_row_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT,
    row_key VARCHAR, check_name VARCHAR, severity VARCHAR);

CREATE OR REPLACE TEMP TABLE chk AS
WITH batch AS (
    SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY _row_num) AS rn
    FROM raw_payments WHERE _run_id = getvariable('run_id')
),
run_orders AS (SELECT DISTINCT order_id FROM raw_orders WHERE _run_id = getvariable('run_id')),
order_mode AS (            -- payment_mode of the first occurrence of each order in the base file
    SELECT order_id, std_mode(payment_mode) AS mode
    FROM raw_orders
    WHERE _run_id = getvariable('run_id') AND _source_file = 'orders.csv'
    QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY _row_num) = 1
)
SELECT b.*,
    checks([
        CASE WHEN rn > 1                                                     THEN 'payments_payment_id_unique' END,
        CASE WHEN b.order_id NOT IN (SELECT order_id FROM run_orders)        THEN 'payments_order_fk' END,
        CASE WHEN num(amount_paid) <= 0                                      THEN 'payments_amount_positive' END,
        CASE WHEN ts_strict(payment_ts) > biz_date(_source_file) + INTERVAL 1 DAY THEN 'payments_ts_not_future' END
    ]) AS crit,
    checks([
        CASE WHEN coalesce(b.payment_mode, '') NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                                                                             THEN 'payments_mode_casing' END,
        CASE WHEN om.mode IS NOT NULL AND coalesce(std_mode(b.payment_mode) <> om.mode, true)
                                                                             THEN 'payments_mode_matches_order' END,
        CASE WHEN is_missing(gateway_ref) AND coalesce(std_mode(b.payment_mode) <> 'Cash on Delivery', true)
                                                                             THEN 'payments_gateway_ref_present' END
    ]) AS warn
FROM batch b LEFT JOIN order_mode om USING (order_id);

DELETE FROM stg_dq_row_results WHERE run_id = getvariable('run_id') AND file = 'payments';
INSERT INTO stg_dq_row_results
SELECT getvariable('run_id'), getvariable('dag_id'), 'payments', _source_file, _row_num, payment_id,
       c.check_name, c.severity
FROM (SELECT *, unnest(list_transform(crit, x -> {'check_name': x, 'severity': 'Critical'})
                       || list_transform(warn, x -> {'check_name': x, 'severity': 'Warning'}), recursive := true)
      FROM chk) c;

-- One row per payment_id (repeat rows dropped here, counted in dq_results / elt_quarantine)
CREATE OR REPLACE TABLE stg_payments AS
SELECT
    payment_id, order_id,
    ts_strict(payment_ts)                                             AS payment_ts,
    std_mode(payment_mode)                                            AS payment_mode,
    num(amount_paid)                                                  AS amount_paid,
    upper(trim(payment_status))                                       AS payment_status,
    CASE WHEN is_missing(gateway_ref) THEN NULL ELSE gateway_ref END  AS gateway_ref,
    CASE WHEN len(crit) > 0 THEN 'FAIL' WHEN len(warn) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(crit, '; '), '')                           AS failure_reason,
    nullif(array_to_string(warn, '; '), '')                           AS dq_warnings,
    _row_num, _source_file, _run_id, _loaded_at
FROM chk
WHERE rn = 1;
