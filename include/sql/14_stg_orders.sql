-- =====================================================================================
-- stg_orders: persistent table, MERGEd on order_id every run (incremental load).
--   * Batch = every order row received in this run: orders.csv + any incoming increment files
--   * Uniqueness is checked within each source file; across files the increment (newer) wins
--   * MERGE: new order_ids inserted, existing ones updated (e.g. DELIVERED -> RETURNED)
--   * An older base-file row never overwrites a correction that came from an increment file
-- =====================================================================================
CREATE TABLE IF NOT EXISTS stg_dq_row_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT,
    row_key VARCHAR, check_name VARCHAR, severity VARCHAR);

CREATE OR REPLACE TEMP TABLE chk AS
WITH batch AS (
    SELECT *,
           row_number() OVER (PARTITION BY order_id, _source_file ORDER BY _row_num) AS rn_in_file,
           contains(_source_file, 'increment')                                         AS is_increment
    FROM raw_orders WHERE _run_id = getvariable('run_id')
),
cust AS (SELECT DISTINCT customer_id FROM raw_customers WHERE _run_id = getvariable('run_id')),
prod AS (SELECT DISTINCT product_id  FROM raw_products  WHERE _run_id = getvariable('run_id')),
stor AS (SELECT DISTINCT store_id    FROM raw_stores    WHERE _run_id = getvariable('run_id'))
SELECT b.*,
    checks([
        CASE WHEN rn_in_file > 1                                              THEN 'orders_order_id_unique' END,
        CASE WHEN is_missing(customer_id)                                     THEN 'orders_customer_id_not_null' END,
        CASE WHEN ts_strict(order_ts) IS NULL                                 THEN 'orders_order_ts_valid' END,
        CASE WHEN ts_strict(order_ts) > biz_date(_source_file) + INTERVAL 1 DAY THEN 'orders_order_ts_not_future' END,
        CASE WHEN std_status(order_status) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                                                                              THEN 'orders_status_allowed' END,
        CASE WHEN std_mode(payment_mode) IS NULL                              THEN 'orders_payment_mode_allowed' END,
        CASE WHEN num(quantity) <= 0                                          THEN 'orders_quantity_positive' END,
        CASE WHEN NOT coalesce(num(discount_pct) BETWEEN 0 AND 100, false)    THEN 'orders_discount_range' END,
        CASE WHEN abs(formula_amount(quantity, unit_price, discount_pct) - num(order_amount)) > 0.01
                  AND num(discount_pct) BETWEEN 0 AND 100 AND num(quantity) > 0
                                                                              THEN 'orders_amount_formula' END,
        CASE WHEN NOT is_missing(customer_id) AND customer_id NOT IN (SELECT customer_id FROM cust)
                                                                              THEN 'orders_customer_fk' END,
        CASE WHEN product_id NOT IN (SELECT product_id FROM prod)             THEN 'orders_product_fk' END,
        CASE WHEN store_id NOT IN (SELECT store_id FROM stor)                 THEN 'orders_store_fk' END
    ]) AS crit,
    checks([
        CASE WHEN order_status <> std_status(order_status)
                  AND std_status(order_status) IN ('DELIVERED', 'CANCELLED', 'RETURNED')
                                                                              THEN 'orders_status_casing' END,
        CASE WHEN coalesce(payment_mode, '') NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')
                  AND std_mode(payment_mode) IS NOT NULL                      THEN 'orders_payment_mode_casing' END,
        CASE WHEN std_status(order_status) = 'DELIVERED' AND num(delivery_minutes) IS NULL
                                                                              THEN 'orders_delivery_minutes_present' END,
        CASE WHEN num(delivery_minutes) > 120                                 THEN 'orders_delivery_minutes_plausible' END
    ]) AS warn
FROM batch b;

DELETE FROM stg_dq_row_results WHERE run_id = getvariable('run_id') AND file = 'orders';
INSERT INTO stg_dq_row_results
SELECT getvariable('run_id'), getvariable('dag_id'), 'orders', _source_file, _row_num, order_id,
       c.check_name, c.severity
FROM (SELECT *, unnest(list_transform(crit, x -> {'check_name': x, 'severity': 'Critical'})
                       || list_transform(warn, x -> {'check_name': x, 'severity': 'Warning'}), recursive := true)
      FROM chk) c;

-- One version per order_id for this run: increment beats base file; repeat rows never selected
CREATE OR REPLACE TEMP TABLE src AS
SELECT
    order_id,
    ts_strict(order_ts)                                               AS order_ts,
    ts_strict(order_ts)::DATE                                         AS order_date,
    CASE WHEN is_missing(customer_id) THEN NULL ELSE customer_id END  AS customer_id,
    store_id, product_id,
    TRY_CAST(num(quantity) AS INTEGER)                                AS quantity,
    num(unit_price)                                                   AS unit_price,
    num(discount_pct)                                                 AS discount_pct,
    num(order_amount)                                                 AS order_amount,
    formula_amount(quantity, unit_price, discount_pct)                AS formula_amount,
    std_mode(payment_mode)                                            AS payment_mode,
    std_status(order_status)                                          AS order_status,
    trim(channel)                                                     AS channel,
    num(delivery_minutes)                                             AS delivery_minutes,
    CASE WHEN len(crit) > 0 THEN 'FAIL' WHEN len(warn) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    nullif(array_to_string(crit, '; '), '')                           AS failure_reason,
    nullif(array_to_string(warn, '; '), '')                           AS dq_warnings,
    _source_file, _run_id, _loaded_at
FROM chk
WHERE rn_in_file = 1
QUALIFY row_number() OVER (PARTITION BY order_id
                           ORDER BY is_increment DESC, _source_file DESC, _row_num DESC) = 1;

CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM src LIMIT 0;

-- Merge statistics (observability for the incremental-load experiment)
CREATE TABLE IF NOT EXISTS elt_merge_log (
    run_id VARCHAR, merged_at TIMESTAMP, batch_rows BIGINT, inserted BIGINT, updated BIGINT,
    protected BIGINT, status_changes BIGINT);
DELETE FROM elt_merge_log WHERE run_id = getvariable('run_id');
INSERT INTO elt_merge_log
SELECT getvariable('run_id'), getvariable('as_of_now'),
       count(*),
       count(*) FILTER (WHERE t.order_id IS NULL),
       count(*) FILTER (WHERE t.order_id IS NOT NULL AND NOT (contains(t._source_file, 'increment')
                                                              AND NOT contains(s._source_file, 'increment'))),
       count(*) FILTER (WHERE contains(t._source_file, 'increment') AND NOT contains(s._source_file, 'increment')),
       count(*) FILTER (WHERE t.order_status IS DISTINCT FROM s.order_status AND t.order_id IS NOT NULL
                          AND NOT (contains(t._source_file, 'increment') AND NOT contains(s._source_file, 'increment')))
FROM src s LEFT JOIN stg_orders t USING (order_id);

MERGE INTO stg_orders t
USING src s
ON t.order_id = s.order_id
WHEN MATCHED AND NOT (contains(t._source_file, 'increment') AND NOT contains(s._source_file, 'increment'))
    THEN UPDATE SET *
WHEN NOT MATCHED
    THEN INSERT *;
