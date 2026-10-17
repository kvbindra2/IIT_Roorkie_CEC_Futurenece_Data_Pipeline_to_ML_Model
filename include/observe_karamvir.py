"""
Part F observation queries - read-only look into include/warehouse_karamvir.duckdb.

Usage (project root; needs `pip install duckdb pandas`, or run inside `astro dev bash`):
    python -m include.observe_karamvir runs          # pipeline_runs: last runs of both DAGs
    python -m include.observe_karamvir dq            # dq_results of the latest run per DAG (non-PASS only)
    python -m include.observe_karamvir quarantine    # rows rejected in the latest run, by file and reason (Q6)
    python -m include.observe_karamvir counts        # fact / staging / raw row counts (idempotency, growth)
    python -m include.observe_karamvir merge         # ELT MERGE log: inserted / updated / protected
    python -m include.observe_karamvir recon         # reconciliation summary, both pipelines
    python -m include.observe_karamvir store S05     # one store's revenue & price in both pipelines (drift)
    python -m include.observe_karamvir order ORD900003   # where is this order? raw / quarantine / fact
    python -m include.observe_karamvir all

Run it between DAG runs: DuckDB lets one process write at a time, so a query while a DAG is
writing may wait or fail with a lock error - just re-run it.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

WAREHOUSE = Path(__file__).resolve().parent / "warehouse_karamvir.duckdb"
ETL, ELT = "nammamart_etl_karamvir", "nammamart_elt_karamvir"
pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 30)
pd.set_option("display.max_rows", 200)


def _con():
    if not WAREHOUSE.exists():
        sys.exit(f"{WAREHOUSE} not found - trigger a DAG first")
    return duckdb.connect(str(WAREHOUSE), read_only=True)


def _tables(con):
    return {r[0] for r in con.execute("SHOW TABLES").fetchall()}


def show(title, con, sql, params=None):
    print(f"\n=== {title}")
    try:
        df = con.execute(sql, params or []).df()
        print(df.to_string(index=False) if len(df) else "(no rows)")
    except duckdb.Error as e:
        print(f"(not available yet: {str(e).splitlines()[0]})")


def runs(con):
    show("pipeline_runs (latest 10)", con, """
        SELECT dag_id, run_id, start_time, end_time,
               round(date_diff('millisecond', start_time, end_time) / 1000.0, 1) AS seconds,
               rows_extracted, rows_loaded, rows_quarantined, status
        FROM pipeline_runs ORDER BY start_time DESC LIMIT 10""")


LATEST = """(SELECT run_id FROM pipeline_runs WHERE dag_id = ? ORDER BY start_time DESC LIMIT 1)"""


def dq(con):
    for dag in (ETL, ELT):
        show(f"dq_results - latest {dag} run (WARN/FAIL only)", con, f"""
            SELECT file, check_name, dimension, severity, rows_checked, rows_failed, status
            FROM dq_results WHERE dag_id = ? AND run_id = {LATEST} AND status <> 'PASS'
            ORDER BY severity, file, check_name""", [dag, dag])


def quarantine(con):
    show("ETL - rows rejected in latest run, by file and reason", con, f"""
        SELECT source_file, failure_reason, count(*) AS rows
        FROM etl_quarantine_log WHERE run_id = {LATEST}
        GROUP BY ALL ORDER BY source_file, rows DESC""", [ETL])
    show("ELT - rows rejected in latest run, by file and reason", con, f"""
        SELECT source_file, failure_reason, count(*) AS rows
        FROM elt_quarantine WHERE run_id = {LATEST}
        GROUP BY ALL ORDER BY source_file, rows DESC""", [ELT])


def counts(con):
    t = _tables(con)
    parts = []
    for name, sql in [("etl_fact_orders", "SELECT count(*) FROM etl_fact_orders"),
                      ("stg_orders", "SELECT count(*) FROM stg_orders"),
                      ("mart_order_facts", "SELECT count(*) FROM mart_order_facts"),
                      ("raw_orders", "SELECT count(*) FROM raw_orders"),
                      ("raw_orders runs", "SELECT count(DISTINCT _run_id) FROM raw_orders"),
                      ("raw_customers", "SELECT count(*) FROM raw_customers")]:
        if name.split()[0] in t:
            parts.append(f"SELECT '{name}' AS table_name, ({sql}) AS row_count")
    if parts:
        show("Row counts", con, " UNION ALL ".join(parts))
    if {"etl_fact_orders", "mart_order_facts"} <= t:
        show("Net revenue, September (ETL vs ELT)", con, """
            SELECT (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders  WHERE order_date < DATE '2026-10-01') AS etl,
                   (SELECT round(sum(net_revenue), 2) FROM mart_order_facts WHERE order_date < DATE '2026-10-01') AS elt""")


def merge(con):
    show("ELT MERGE log (stg_orders)", con, "SELECT * FROM elt_merge_log ORDER BY merged_at DESC LIMIT 10")


def recon(con):
    show("Reconciliation - ETL (mismatches only)", con, """
        SELECT recon_status, count(*) AS rows, round(sum(variance), 2) AS variance
        FROM etl_payment_reconciliation GROUP BY ALL ORDER BY rows DESC""")
    show("Reconciliation - ELT (all orders)", con, """
        SELECT recon_status, count(*) AS rows, round(sum(variance), 2) AS variance
        FROM mart_payment_reconciliation GROUP BY ALL ORDER BY rows DESC""")


def store(con, store_id):
    show(f"{store_id} - ETL", con, """
        SELECT count(*) AS orders_in_fact, round(sum(net_revenue), 2) AS net_revenue,
               round(avg(unit_price), 2) AS avg_unit_price, round(max(order_amount), 2) AS max_order
        FROM etl_fact_orders WHERE store_id = ?""", [store_id])
    show(f"{store_id} - ELT", con, """
        SELECT count(*) AS orders_in_mart, round(sum(net_revenue), 2) AS net_revenue,
               round(avg(unit_price), 2) AS avg_unit_price, round(max(order_amount), 2) AS max_order
        FROM mart_order_facts WHERE store_id = ?""", [store_id])
    show(f"{store_id} - reconciliation (ELT)", con, """
        SELECT r.recon_status, count(*) AS rows, round(sum(r.variance), 2) AS variance
        FROM mart_payment_reconciliation r JOIN stg_orders o USING (order_id)
        WHERE o.store_id = ? GROUP BY ALL ORDER BY rows DESC""", [store_id])


def order(con, order_id):
    show("raw_orders (ELT, every version received)", con,
         "SELECT * EXCLUDE (_loaded_at) FROM raw_orders WHERE order_id = ? ORDER BY _run_id", [order_id])
    show("ETL quarantine log", con, "SELECT * FROM etl_quarantine_log WHERE row_key = ? ORDER BY quarantined_at", [order_id])
    show("ELT quarantine", con, "SELECT * FROM elt_quarantine WHERE row_key = ? ORDER BY quarantined_at", [order_id])
    show("In ETL fact?", con, "SELECT order_id, order_status, net_revenue, recon_status FROM etl_fact_orders WHERE order_id = ?", [order_id])
    show("In ELT mart?", con, "SELECT order_id, order_status, net_revenue, recon_status FROM mart_order_facts WHERE order_id = ?", [order_id])


def main(argv):
    cmd = argv[1] if len(argv) > 1 else "all"
    con = _con()
    try:
        if cmd == "store":
            store(con, argv[2] if len(argv) > 2 else "S05")
        elif cmd == "order":
            order(con, argv[2])
        elif cmd == "all":
            for f in (runs, counts, merge, dq, quarantine, recon):
                f(con)
        elif cmd in {"runs", "dq", "quarantine", "counts", "merge", "recon"}:
            globals()[cmd](con)
        else:
            print(__doc__)
    finally:
        con.close()


if __name__ == "__main__":
    main(sys.argv)
