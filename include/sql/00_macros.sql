-- =====================================================================================
-- Shared SQL helpers for the NammaMart ELT pipeline (karamvir).
-- Prepended to every include/sql/*.sql file at run time, so each file is self-contained.
-- Each macro mirrors a helper in include/dq_rules.py - same rule, same result.
-- Session variables set by the runner: run_id, dag_id, as_of (IST timestamp)
-- =====================================================================================

-- NULL, '' and textual nulls ('N/A', 'NULL', ...) all count as missing
CREATE OR REPLACE TEMP MACRO is_missing(x) AS
    coalesce(upper(trim(x)), '') IN ('', 'N/A', 'NA', 'NULL', 'NONE');

CREATE OR REPLACE TEMP MACRO num(x) AS TRY_CAST(x AS DOUBLE);

-- Strict timestamp: only 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' (31/09/2026, month 13, N/A -> NULL)
CREATE OR REPLACE TEMP MACRO ts_strict(x) AS
    coalesce(try_strptime(x, '%Y-%m-%d %H:%M:%S'), try_strptime(x, '%Y-%m-%d'));

CREATE OR REPLACE TEMP MACRO std_status(x) AS upper(trim(coalesce(x, '')));

-- Canonical payment mode, NULL if unrecognisable (upi, 'UPI ', cod, CARD, Cashh -> canonical)
CREATE OR REPLACE TEMP MACRO std_mode(x) AS
    CASE upper(trim(coalesce(x, '')))
        WHEN 'UPI' THEN 'UPI'
        WHEN 'CARD' THEN 'Card'
        WHEN 'WALLET' THEN 'Wallet'
        WHEN 'CASH ON DELIVERY' THEN 'Cash on Delivery'
        WHEN 'COD' THEN 'Cash on Delivery'
        WHEN 'CASHH' THEN 'Cash on Delivery'
    END;

-- order_amount = quantity x unit_price x (1 - discount_pct / 100), rounded to 2 dp (half-even, as pandas)
CREATE OR REPLACE TEMP MACRO formula_amount(q, p, d) AS
    round_even(num(q) * num(p) * (1 - num(d) / 100), 2);

-- PII masking (decisions Q3 / Q4)
CREATE OR REPLACE TEMP MACRO valid_email(e) AS
    regexp_full_match(trim(coalesce(e, '')), '[^@\s]+@[^@\s]+\.[A-Za-z]{2,}');
CREATE OR REPLACE TEMP MACRO mask_email(e) AS
    CASE WHEN valid_email(e)
         THEN left(split_part(trim(e), '@', 1), 2) || '***@' || split_part(trim(e), '@', 2) END;
CREATE OR REPLACE TEMP MACRO mask_phone(p) AS
    CASE WHEN length(regexp_replace(coalesce(p, ''), '\D', '', 'g')) = 10
         THEN left(regexp_replace(p, '\D', '', 'g'), 2) || '******' || right(regexp_replace(p, '\D', '', 'g'), 2) END;
CREATE OR REPLACE TEMP MACRO mask_name(n) AS
    CASE WHEN is_missing(n) THEN NULL
         WHEN len(string_split(trim(n), ' ')) = 1 THEN trim(n)
         ELSE string_split(trim(n), ' ')[1] || ' ' || left(string_split(trim(n), ' ')[-1], 1) || '.' END;

-- Collect the names of failed checks into a list: checks([CASE WHEN <fails> THEN '<name>' END, ...])
CREATE OR REPLACE TEMP MACRO checks(l) AS list_filter(l, x -> x IS NOT NULL);

-- 'BRONZE' / 'bronze' -> 'Bronze' (single-word values such as loyalty_tier)
CREATE OR REPLACE TEMP MACRO title_word(x) AS upper(left(trim(x), 1)) || lower(substr(trim(x), 2));

-- Business date of a source file = the last day it is supposed to cover (same rule as dq_rules.business_date):
-- a date in the file name (orders_2026-10-01_increment.csv) > agreed period end (ref_source_periods) > yesterday.
-- Timeliness "not in the future" checks compare against this, not the wall clock, so verdicts don't drift with time.
CREATE OR REPLACE TEMP MACRO biz_date(f) AS coalesce(
    try_strptime(regexp_extract(f, '(\d{4}-\d{2}-\d{2})', 1), '%Y-%m-%d'),
    (SELECT max(period_end) FROM ref_source_periods p WHERE p.source_file = f),
    date_trunc('day', getvariable('as_of')) - INTERVAL 1 DAY);
