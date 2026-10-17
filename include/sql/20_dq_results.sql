-- =====================================================================================
-- dq_results: one row per check per source file per run (shared scorecard with the ETL DAG)
-- Check names, dimensions and severities come from ref_dq_checks (seeded from include/dq_rules.py)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, check_name VARCHAR, dimension VARCHAR, severity VARCHAR,
    rows_checked BIGINT, rows_failed BIGINT, status VARCHAR, checked_at TIMESTAMP);

DELETE FROM dq_results WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id');

INSERT INTO dq_results
WITH files AS (
    SELECT 'orders'    AS file, _source_file AS source_file, count(*) AS n FROM raw_orders    WHERE _run_id = getvariable('run_id') GROUP BY ALL
    UNION ALL SELECT 'payments',  _source_file, count(*) FROM raw_payments  WHERE _run_id = getvariable('run_id') GROUP BY ALL
    UNION ALL SELECT 'customers', _source_file, count(*) FROM raw_customers WHERE _run_id = getvariable('run_id') GROUP BY ALL
    UNION ALL SELECT 'products',  _source_file, count(*) FROM raw_products  WHERE _run_id = getvariable('run_id') GROUP BY ALL
    UNION ALL SELECT 'stores',    _source_file, count(*) FROM raw_stores    WHERE _run_id = getvariable('run_id') GROUP BY ALL
),
fails AS (
    SELECT file, source_file, check_name, count(*) AS n_failed
    FROM stg_dq_row_results WHERE run_id = getvariable('run_id') GROUP BY ALL
)
SELECT getvariable('run_id'), getvariable('dag_id'), f.source_file, r.check_name, r.dimension, r.severity,
       f.n, coalesce(x.n_failed, 0),
       CASE WHEN coalesce(x.n_failed, 0) = 0 THEN 'PASS' WHEN r.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END,
       getvariable('as_of_now')
FROM ref_dq_checks r
JOIN files f ON f.file = r.file
LEFT JOIN fails x ON x.file = r.file AND x.source_file = f.source_file AND x.check_name = r.check_name
ORDER BY r.ordinal, f.source_file;

-- Dataset-level checks (Timeliness: stale feed, Completeness: volume drop > 90% vs previous run)
INSERT INTO dq_results
WITH base AS (SELECT * FROM raw_orders WHERE _run_id = getvariable('run_id') AND _source_file = 'orders.csv'),
latest AS (SELECT max(ts_strict(order_ts)) FILTER (WHERE ts_strict(order_ts) <= biz_date('orders.csv') + INTERVAL 1 DAY) AS t,
                  count(*) AS n FROM base),
prev AS (SELECT rows_checked FROM dq_results
         WHERE dag_id = getvariable('dag_id') AND check_name = 'orders_order_id_unique'
           AND file = 'orders.csv' AND run_id <> getvariable('run_id')
         ORDER BY checked_at DESC LIMIT 1)
SELECT getvariable('run_id'), getvariable('dag_id'), 'orders.csv', 'orders_freshness', 'Timeliness', 'Warning', n,
       (t IS NULL OR t < getvariable('as_of') - INTERVAL 1 DAY)::INT,
       CASE WHEN t IS NULL OR t < getvariable('as_of') - INTERVAL 1 DAY THEN 'WARN' ELSE 'PASS' END,
       getvariable('as_of_now')
FROM latest
UNION ALL
SELECT getvariable('run_id'), getvariable('dag_id'), 'orders.csv', 'orders_volume_drop', 'Completeness', 'Warning', l.n,
       (1 - l.n / p.rows_checked > 0.9)::INT,
       CASE WHEN 1 - l.n / p.rows_checked > 0.9 THEN 'WARN' ELSE 'PASS' END,
       getvariable('as_of_now')
FROM latest l, prev p;
