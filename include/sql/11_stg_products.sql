-- =====================================================================================
-- stg_products: typed, de-duplicated ('(old)' rows rejected), with dq_status
-- Decision Q1: blank category -> 'Unknown', bad mrp / cost -> NULL (excluded from margin), row kept
-- =====================================================================================
CREATE TABLE IF NOT EXISTS stg_dq_row_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT,
    row_key VARCHAR, check_name VARCHAR, severity VARCHAR);

CREATE OR REPLACE TEMP TABLE chk AS
WITH batch AS (
    SELECT *, count(*) OVER (PARTITION BY product_id) AS n_same_id
    FROM raw_products WHERE _run_id = getvariable('run_id')
)
SELECT *,
    checks([
        CASE WHEN n_same_id > 1 AND contains(product_name, '(old)')   THEN 'products_product_id_unique' END,
        CASE WHEN is_missing(category)                                THEN 'products_category_present' END,
        CASE WHEN num(mrp) <= 0                                       THEN 'products_mrp_positive' END,
        CASE WHEN num(mrp) > 0 AND num(cost_price) > num(mrp)         THEN 'products_cost_le_mrp' END
    ]) AS crit,
    []::VARCHAR[] AS warn
FROM batch;

DELETE FROM stg_dq_row_results WHERE run_id = getvariable('run_id') AND file = 'products';
INSERT INTO stg_dq_row_results
SELECT getvariable('run_id'), getvariable('dag_id'), 'products', _source_file, _row_num, product_id,
       c.check_name, c.severity
FROM (SELECT *, unnest(list_transform(crit, x -> {'check_name': x, 'severity': 'Critical'}), recursive := true)
      FROM chk) c;

CREATE OR REPLACE TABLE stg_products AS
SELECT
    product_id, product_name,
    CASE WHEN is_missing(category) THEN 'Unknown' ELSE category END    AS category,
    sub_category, brand,
    CASE WHEN num(mrp) > 0 THEN num(mrp) END                           AS mrp,
    CASE WHEN list_contains(crit, 'products_cost_le_mrp') THEN NULL ELSE num(cost_price) END AS cost_price,
    unit,
    lower(is_perishable) = 'true'                                      AS is_perishable,
    ts_strict(launch_date)::DATE                                       AS launch_date,
    CASE WHEN len(crit) > 0 THEN 'QUARANTINED' ELSE 'OK' END           AS dq_status,
    nullif(array_to_string(crit, '; '), '')                            AS failure_reason,
    _source_file, _run_id, _loaded_at
FROM chk
WHERE NOT list_contains(crit, 'products_product_id_unique');
