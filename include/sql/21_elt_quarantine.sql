-- =====================================================================================
-- elt_quarantine: every row that failed a Critical check this run, with failure_reason.
-- Rows are NOT removed from raw - (source_file, row_num, run_id) points back to the exact raw row.
-- =====================================================================================
CREATE TABLE IF NOT EXISTS elt_quarantine (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT, row_key VARCHAR,
    failure_reason VARCHAR, quarantined_at TIMESTAMP);

DELETE FROM elt_quarantine WHERE run_id = getvariable('run_id');

INSERT INTO elt_quarantine
SELECT d.run_id, d.dag_id, d.file, d.source_file, d.row_num, d.row_key,
       string_agg(d.check_name, '; ' ORDER BY r.ordinal), getvariable('as_of_now')
FROM stg_dq_row_results d JOIN ref_dq_checks r USING (check_name)
WHERE d.run_id = getvariable('run_id') AND d.severity = 'Critical'
GROUP BY ALL;
