-- =====================================================================================
-- stg_customers: typed, standardised, de-duplicated, PII masked, with dq_status
-- Source: raw_customers rows loaded in this run (the CRM sends a full snapshot daily)
-- Decision Q1: a row failing only a fixable check (future signup) stays as a flagged member
-- =====================================================================================
CREATE TABLE IF NOT EXISTS stg_dq_row_results (
    run_id VARCHAR, dag_id VARCHAR, file VARCHAR, source_file VARCHAR, row_num BIGINT,
    row_key VARCHAR, check_name VARCHAR, severity VARCHAR);

CREATE OR REPLACE TEMP TABLE chk AS
WITH batch AS (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _row_num) AS rn
    FROM raw_customers WHERE _run_id = getvariable('run_id')
)
SELECT *,
    checks([
        CASE WHEN rn > 1                                            THEN 'customers_customer_id_unique' END,
        CASE WHEN ts_strict(signup_date) > biz_date(_source_file)     THEN 'customers_signup_not_future' END
    ]) AS crit,
    checks([
        CASE WHEN NOT valid_email(email)                            THEN 'customers_email_valid' END,
        CASE WHEN NOT regexp_full_match(coalesce(phone, ''), '[6-9][0-9]{9}')   THEN 'customers_phone_valid' END,
        CASE WHEN NOT regexp_full_match(coalesce(pincode, ''), '[0-9]{6}')      THEN 'customers_pincode_valid' END,
        CASE WHEN is_missing(city)                                  THEN 'customers_city_present' END,
        CASE WHEN coalesce(loyalty_tier, '') NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')
                                                                    THEN 'customers_tier_casing' END
    ]) AS warn
FROM batch;

DELETE FROM stg_dq_row_results WHERE run_id = getvariable('run_id') AND file = 'customers';
INSERT INTO stg_dq_row_results
SELECT getvariable('run_id'), getvariable('dag_id'), 'customers', _source_file, _row_num, customer_id,
       c.check_name, c.severity
FROM (SELECT *, unnest(list_transform(crit, x -> {'check_name': x, 'severity': 'Critical'})
                       || list_transform(warn, x -> {'check_name': x, 'severity': 'Warning'}), recursive := true)
      FROM chk) c;

CREATE OR REPLACE TABLE stg_customers AS
SELECT
    customer_id,
    mask_name(full_name)                                              AS customer_name,
    mask_email(email)                                                 AS email_masked,
    mask_phone(phone)                                                 AS phone_masked,
    CASE WHEN is_missing(city) THEN NULL ELSE trim(city) END          AS city,
    area,
    CASE WHEN regexp_full_match(coalesce(pincode, ''), '[0-9]{6}') THEN pincode END AS pincode,
    CASE WHEN list_contains(crit, 'customers_signup_not_future') THEN NULL
         ELSE ts_strict(signup_date)::DATE END                        AS signup_date,
    title_word(loyalty_tier)                                       AS loyalty_tier,
    upper(trim(is_active)) = 'Y'                                      AS is_active,
    CASE WHEN len(crit) > 0 THEN 'QUARANTINED' WHEN len(warn) > 0 THEN 'WARNING' ELSE 'OK' END AS dq_status,
    nullif(array_to_string(crit, '; '), '')                           AS failure_reason,
    nullif(array_to_string(warn, '; '), '')                           AS dq_warnings,
    _source_file, _run_id, _loaded_at
FROM chk
WHERE NOT list_contains(crit, 'customers_customer_id_unique');        -- repeat rows are rejected outright
