"""
NammaMart ELT pipeline runner (Part B) - called by dags/nammamart_elt_karamvir.py

Extract -> Load raw (exact text copy, append-only) -> Transform in DuckDB with SQL (include/sql/*.sql)

Python here only moves files into raw tables and executes SQL files; every cleaning,
quality rule, reconciliation and business metric is SQL.

Run locally without Airflow:   python -m include.elt_karamvir
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

try:
    from include import dq_rules as dq
    from include.etl_karamvir import connect, DATA_DIR, INCOMING_DIR, INCREMENT_GLOB, SOURCE_FILES, WAREHOUSE
except ImportError:                       # pragma: no cover
    import dq_rules as dq
    from etl_karamvir import connect, DATA_DIR, INCOMING_DIR, INCREMENT_GLOB, SOURCE_FILES, WAREHOUSE

DAG_ID = "nammamart_elt_karamvir"
ETL_DAG_ID = "nammamart_etl_karamvir"
SQL_DIR = Path(__file__).resolve().parent / "sql"
IST = ZoneInfo("Asia/Kolkata")

STAGING_SQL = ["10_stg_customers.sql", "11_stg_products.sql", "12_stg_stores.sql",
               "13_stg_payments.sql", "14_stg_orders.sql"]
QUALITY_SQL = ["20_dq_results.sql", "21_elt_quarantine.sql"]
MART_SQL = ["30_mart_payment_reconciliation.sql", "31_mart_order_facts.sql", "32_mart_daily_store_kpis.sql",
            "33_mart_category_margin.sql", "34_mart_delivery_sla.sql", "35_mart_top_customers.sql"]


def _now() -> datetime:
    return datetime.now(IST).replace(tzinfo=None)


# --------------------------------------------------------------------------------------
# EXTRACT + LOAD RAW
# --------------------------------------------------------------------------------------
def _insert_raw(con, table: str, path: Path, run_id: str) -> int:
    """Append one CSV to its raw table: all columns as text, plus lineage columns. Never cleaned."""
    header = [r[0] for r in con.execute(
        "DESCRIBE SELECT * FROM read_csv(?, all_varchar = true, header = true)", [str(path)]).fetchall()]
    dq.check_schema(table.removeprefix("raw_"), pd.DataFrame(columns=header), path.name)   # fail loudly on drift
    select = f"""SELECT *, row_number() OVER () AS _row_num,
                        ?::TIMESTAMP AS _loaded_at, ? AS _source_file, ? AS _run_id
                 FROM read_csv(?, all_varchar = true, header = true)"""
    params = [_now(), path.name, run_id, str(path)]
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS {select} LIMIT 0", params)
    # Re-trying the same run replaces only that run's rows for this file; new runs always append
    con.execute(f"DELETE FROM {table} WHERE _run_id = ? AND _source_file = ?", [run_id, path.name])
    con.execute(f"INSERT INTO {table} BY NAME {select}", params)
    return con.execute(f"SELECT count(*) FROM {table} WHERE _run_id = ? AND _source_file = ?",
                       [run_id, path.name]).fetchone()[0]


def load_raw(name: str, run_id: str) -> dict:
    path = DATA_DIR / SOURCE_FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    con = connect()
    try:
        n = _insert_raw(con, f"raw_{name}", path, run_id)
        total = con.execute(f"SELECT count(*) FROM raw_{name}").fetchone()[0]
    finally:
        con.close()
    print(f"[Load raw] {path.name}: {n} rows appended to raw_{name} (table now {total} rows)")
    return {"name": name, "file": path.name, "rows": n}


def load_raw_increments(run_id: str) -> dict:
    files = sorted(INCOMING_DIR.glob(INCREMENT_GLOB))
    if not files:
        print(f"[Load raw] no increment files in {INCOMING_DIR}")
        return {"name": "orders_increment", "file": None, "rows": 0}
    con = connect()
    try:
        n = sum(_insert_raw(con, "raw_orders", f, run_id) for f in files)
    finally:
        con.close()
    print(f"[Load raw] {', '.join(f.name for f in files)}: {n} rows appended to raw_orders")
    return {"name": "orders_increment", "file": ", ".join(f.name for f in files), "rows": n}


# --------------------------------------------------------------------------------------
# REFERENCE DATA (rule catalogue shared with the ETL - one definition of every check)
# --------------------------------------------------------------------------------------
def seed_reference_data() -> dict:
    checks = pd.DataFrame([{"ordinal": i, "check_name": c.name, "file": c.file, "dimension": c.dimension,
                            "severity": c.severity, "problem": c.problem, "rule": c.rule}
                           for i, c in enumerate(dq.CHECKS)])
    owners = pd.DataFrame(list(dq.RECON_OWNER.items()), columns=["recon_status", "owner"])
    periods = pd.DataFrame([(f, pd.Timestamp(d)) for f, d in dq.SOURCE_PERIOD_END.items()],
                           columns=["source_file", "period_end"])
    con = connect()
    try:
        for name, df in [("ref_dq_checks", checks), ("ref_recon_owner", owners), ("ref_source_periods", periods)]:
            con.register("_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
            con.unregister("_df")
    finally:
        con.close()
    print(f"[Reference] {len(checks)} checks, {len(owners)} reconciliation statuses")
    return {"checks": len(checks)}


# --------------------------------------------------------------------------------------
# TRANSFORM: run SQL files
# --------------------------------------------------------------------------------------
def run_sql(filename: str, run_id: str, as_of: pd.Timestamp, show: str | None = None) -> dict:
    """Execute include/sql/<filename> with the shared macros and run variables."""
    sql = (SQL_DIR / "00_macros.sql").read_text() + "\n" + (SQL_DIR / filename).read_text()
    con = connect()
    try:
        con.execute("SET VARIABLE run_id = ?", [run_id])
        con.execute("SET VARIABLE dag_id = ?", [DAG_ID])
        con.execute("SET VARIABLE as_of = ?::TIMESTAMP", [pd.Timestamp(as_of).to_pydatetime()])
        con.execute("SET VARIABLE as_of_now = ?::TIMESTAMP", [_now()])
        con.execute(sql)
        out = {"file": filename}
        if show:
            df = con.execute(show).df()
            print(f"[SQL] {filename}\n{df.to_string(index=False)}")
            out["preview_rows"] = len(df)
        else:
            print(f"[SQL] {filename} done")
    finally:
        con.close()
    return out


# Small "what happened" queries printed in each task log
SHOW = {
    "10_stg_customers.sql": "SELECT dq_status, count(*) AS n FROM stg_customers GROUP BY ALL ORDER BY 1",
    "11_stg_products.sql": "SELECT dq_status, count(*) AS n FROM stg_products GROUP BY ALL ORDER BY 1",
    "12_stg_stores.sql": "SELECT dq_status, count(*) AS n FROM stg_stores GROUP BY ALL ORDER BY 1",
    "13_stg_payments.sql": "SELECT dq_status, count(*) AS n FROM stg_payments GROUP BY ALL ORDER BY 1",
    "14_stg_orders.sql": """SELECT m.batch_rows, m.inserted, m.updated, m.protected, m.status_changes,
                                   (SELECT count(*) FROM stg_orders) AS stg_orders_total,
                                   (SELECT count(*) FROM stg_orders WHERE dq_status = 'FAIL') AS failed
                            FROM elt_merge_log m WHERE run_id = getvariable('run_id')""",
    "20_dq_results.sql": """SELECT file, check_name, severity, rows_failed, status FROM dq_results
                            WHERE run_id = getvariable('run_id') AND status <> 'PASS'""",
    "21_elt_quarantine.sql": """SELECT file, count(*) AS rows_quarantined FROM elt_quarantine
                                WHERE run_id = getvariable('run_id') GROUP BY ALL ORDER BY 1""",
    "30_mart_payment_reconciliation.sql": """SELECT recon_status, count(*) AS n, round(sum(variance), 2) AS variance
                                             FROM mart_payment_reconciliation GROUP BY ALL ORDER BY n DESC""",
    "31_mart_order_facts.sql": """SELECT count(*) AS orders, round(sum(net_revenue), 2) AS net_revenue
                                  FROM mart_order_facts""",
    "32_mart_daily_store_kpis.sql": """SELECT zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
                                       FROM mart_daily_store_kpis GROUP BY ALL ORDER BY net_revenue DESC""",
    "33_mart_category_margin.sql": "SELECT * FROM mart_category_margin",
    "34_mart_delivery_sla.sql": "SELECT * FROM mart_delivery_sla",
    "35_mart_top_customers.sql": "SELECT * FROM mart_top_customers",
}


# --------------------------------------------------------------------------------------
# PARITY: ELT marts must match ETL totals
# --------------------------------------------------------------------------------------
PARITY_SQL = """
CREATE OR REPLACE TABLE etl_elt_parity AS
WITH e AS (SELECT 'ETL' AS pipeline, count(*) AS fact_rows, round(sum(net_revenue), 2) AS net_revenue,
                  round(sum(gross_margin), 2) AS gross_margin, count(*) FILTER (WHERE revenue_eligible) AS revenue_orders
           FROM etl_fact_orders WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30'),
     l AS (SELECT 'ELT', count(*), round(sum(net_revenue), 2), round(sum(gross_margin), 2),
                  count(*) FILTER (WHERE revenue_eligible)
           FROM mart_order_facts WHERE order_date BETWEEN DATE '2026-09-01' AND DATE '2026-09-30')
SELECT *, ? AS checked_run_id, ?::TIMESTAMP AS checked_at FROM (SELECT * FROM e UNION ALL SELECT * FROM l)
"""


def parity_check(run_id: str) -> dict:
    con = connect()
    try:
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        if "etl_fact_orders" not in tables:
            print("[Parity] ETL tables not found - run nammamart_etl_karamvir first. Skipping.")
            return {"match": None}
        con.execute(PARITY_SQL, [run_id, _now()])
        df = con.execute("SELECT * FROM etl_elt_parity").df()
        diff = con.execute("""
            SELECT coalesce(e.order_id, l.order_id) AS order_id, e.net_revenue AS etl, l.net_revenue AS elt,
                   e.recon_status AS etl_recon, l.recon_status AS elt_recon
            FROM (SELECT * FROM etl_fact_orders WHERE order_date < DATE '2026-10-01') e
            FULL JOIN (SELECT * FROM mart_order_facts WHERE order_date < DATE '2026-10-01') l USING (order_id)
            WHERE e.net_revenue IS DISTINCT FROM l.net_revenue LIMIT 20""").df()
    finally:
        con.close()
    print("[Parity] September 2026\n" + df.drop(columns=["checked_run_id", "checked_at"]).to_string(index=False))
    match = len(diff) == 0
    print("[Parity] MATCH - every September order has the same net revenue in ETL and ELT" if match
          else "[Parity] DIFFERENCES (first 20 orders):\n" + diff.to_string(index=False))
    return {"match": match, "differences": len(diff)}


# --------------------------------------------------------------------------------------
# RUN AUDIT (same pipeline_runs table as the ETL)
# --------------------------------------------------------------------------------------
def write_run_audit(run_id: str, start_time, status: str) -> None:
    con = connect()
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMP,
                       end_time TIMESTAMP, rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT,
                       status VARCHAR)""")
        tables = {r[0] for r in con.execute("SHOW TABLES").fetchall()}
        q = lambda sql: con.execute(sql, [run_id]).fetchone()[0] if all(  # noqa: E731
            t in tables for t in ("raw_orders", "elt_quarantine", "elt_merge_log")) else 0
        extracted = q("""SELECT (SELECT count(*) FROM raw_orders WHERE _run_id = $1)
                              + (SELECT count(*) FROM raw_payments WHERE _run_id = $1)
                              + (SELECT count(*) FROM raw_customers WHERE _run_id = $1)
                              + (SELECT count(*) FROM raw_products WHERE _run_id = $1)
                              + (SELECT count(*) FROM raw_stores WHERE _run_id = $1)""")
        loaded = q("""SELECT count(*) FROM stg_orders WHERE _run_id = $1 AND dq_status <> 'FAIL'""")
        quarantined = q("SELECT count(*) FROM elt_quarantine WHERE run_id = $1")
        con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
        con.execute("INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [run_id, DAG_ID, start_time, _now(), extracted, loaded, quarantined, status])
    finally:
        con.close()
    print(f"[Audit] {run_id}: extracted={extracted} loaded={loaded} quarantined={quarantined} status={status}")


# --------------------------------------------------------------------------------------
# Local run without Airflow
# --------------------------------------------------------------------------------------
def run_local(run_id: str | None = None, as_of: pd.Timestamp | None = None) -> None:
    run_id = run_id or f"local__{datetime.now():%Y%m%dT%H%M%S}"
    as_of = as_of or pd.Timestamp(_now())
    start = _now()
    seed_reference_data()
    for n in SOURCE_FILES:
        load_raw(n, run_id)
    load_raw_increments(run_id)
    for f in STAGING_SQL + QUALITY_SQL + MART_SQL:
        run_sql(f, run_id, as_of, SHOW.get(f) if f in ("14_stg_orders.sql", "31_mart_order_facts.sql") else None)
    parity_check(run_id)
    write_run_audit(run_id, start, "SUCCESS")


if __name__ == "__main__":
    run_local()
