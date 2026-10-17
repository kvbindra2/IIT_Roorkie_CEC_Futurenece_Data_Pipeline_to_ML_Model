-- =====================================================================================
-- stg_stores: typed, with dq_status. Decision Q1: missing zone -> 'Unknown', row kept (S07)
-- =====================================================================================
CREATE TABLE IF NOT EXISTS stg_dq_row_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT,
    row_key VARCHAR, check_name VARCHAR, severity VARCHAR);

CREATE OR REPLACE TEMP TABLE chk AS
SELECT *,
    checks([CASE WHEN coalesce(zone, '') NOT IN ('North', 'South', 'East', 'West') THEN 'stores_zone_valid' END]) AS crit,
    checks([CASE WHEN num(capacity_orders_per_day) <= 0 THEN 'stores_capacity_positive' END])                AS warn
FROM raw_stores WHERE _run_id = getvariable('run_id');

DELETE FROM stg_dq_row_results WHERE run_id = getvariable('run_id') AND file = 'stores';
INSERT INTO stg_dq_row_results
SELECT getvariable('run_id'), getvariable('dag_id'), 'stores', _source_file, _row_num, store_id,
       c.check_name, c.severity
FROM (SELECT *, unnest(list_transform(crit, x -> {'check_name': x, 'severity': 'Critical'})
                       || list_transform(warn, x -> {'check_name': x, 'severity': 'Warning'}), recursive := true)
      FROM chk) c;

CREATE OR REPLACE TABLE stg_stores AS
SELECT
    store_id, store_name,
    CASE WHEN len(crit) > 0 THEN 'Unknown' ELSE zone END              AS zone,
    area, pincode,
    ts_strict(open_date)::DATE                                        AS open_date,
    manager_name,
    CASE WHEN num(capacity_orders_per_day) > 0 THEN num(capacity_orders_per_day)::INTEGER END AS capacity_orders_per_day,
    CASE WHEN len(crit) > 0 THEN 'QUARANTINED' WHEN len(warn) > 0 THEN 'WARNING' ELSE 'OK' END AS dq_status,
    nullif(array_to_string(crit, '; '), '')                           AS failure_reason,
    nullif(array_to_string(warn, '; '), '')                           AS dq_warnings,
    _source_file, _run_id, _loaded_at
FROM chk;
