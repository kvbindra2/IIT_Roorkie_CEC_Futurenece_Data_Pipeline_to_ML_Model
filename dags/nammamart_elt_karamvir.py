"""
dags/nammamart_elt_karamvir.py
------------------------------
NammaMart ELT pipeline (Part B): Extract -> Load raw -> Transform in the warehouse with SQL.

  seed_reference_data ─┐
  load_raw_orders ─────┤          ┌► stg_customers ─┐
  load_raw_payments ───┤          ├► stg_products ──┤        ┌► dq_results ────┐
  load_raw_customers ──┼──────────┼► stg_stores ────┼────────┤                 ├► mart_payment_reconciliation ► mart_order_facts ─┐
  load_raw_products ───┤          ├► stg_payments ──┤        └► elt_quarantine ┘                                                 │
  load_raw_stores ─────┤          └► stg_orders ────┘                       ┌───────────────────────────────────────────────────┘
  load_raw_order_increments ┘          (MERGE)                              ├► mart_daily_store_kpis ─┐
                                                                            ├► mart_category_margin ──┤
                                                                            ├► mart_delivery_sla ─────┼► parity_check ► run_audit (all_done)
                                                                            └► mart_top_customers ────┘
Layers in include/warehouse_karamvir.duckdb
  raw_*   exact text copy of every file received + _loaded_at, _source_file, _run_id (append-only, never cleaned)
  stg_*   SQL only: typed, standardised, de-duplicated, PII masked, dq_status + failure_reason
  mart_*  SQL only: business-ready tables for the six business questions
SQL lives in include/sql/ (one file per table); 00_macros.sql holds the shared rule helpers.

Schedule (Part C): cron "0 9 * * 1-6", start_date in Asia/Kolkata -> 09:00 IST Mon-Sat (03:30 UTC).
catchup=False: every run re-reads the full current extract; replaying missed days adds nothing.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:                                                              # Airflow 3 (Astro Runtime 3.x)
    from airflow.sdk import dag, task
except ImportError:                                               # Airflow 2.x fallback
    from airflow.decorators import dag, task

IST = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "karamvir",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def _as_of(context):
    import pandas as pd
    when = context.get("logical_date") or context.get("data_interval_end") or pendulum.now(IST)
    return pd.Timestamp(pendulum.instance(when).in_timezone(IST).naive())


@dag(
    dag_id="nammamart_elt_karamvir",
    description="NammaMart ELT: raw -> staging -> marts in DuckDB SQL for the 9:30 AM ops review",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, 9, 0, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "elt", "karamvir"],
    doc_md=__doc__,
)
def nammamart_elt_karamvir():

    # ── REFERENCE DATA: the rule catalogue shared with the ETL DAG ────────────────────
    @task
    def seed_reference_data():
        from include import elt_karamvir as elt
        return elt.seed_reference_data()

    # ── EXTRACT + LOAD RAW: one task per file, in parallel, no cleaning ───────────────
    @task
    def load_raw(name, **context):
        from include import elt_karamvir as elt
        return elt.load_raw(name, context["run_id"])

    @task
    def load_raw_order_increments(**context):
        from include import elt_karamvir as elt
        return elt.load_raw_increments(context["run_id"])

    # ── TRANSFORM IN THE WAREHOUSE: one task per SQL file ─────────────────────────────
    @task
    def run_sql(filename, **context):
        from include import elt_karamvir as elt
        return elt.run_sql(filename, context["run_id"], _as_of(context), elt.SHOW.get(filename))

    @task
    def parity_check(**context):
        """Part B.4: September mart totals must match the ETL totals, order by order."""
        from include import elt_karamvir as elt
        return elt.parity_check(context["run_id"])

    @task(trigger_rule="all_done", retries=0)
    def run_audit(**context):
        from include import elt_karamvir as elt
        ok = context["ti"].xcom_pull(task_ids="parity_check") is not None
        dag_run = context.get("dag_run")
        start = getattr(dag_run, "start_date", None) or pendulum.now(IST)
        elt.write_run_audit(context["run_id"], pendulum.instance(start).in_timezone(IST).naive(),
                            "SUCCESS" if ok else "FAILED")
        if not ok:
            raise RuntimeError("Upstream task failed - see pipeline_runs and the failed task's log")

    sql = lambda task_id, f: run_sql.override(task_id=task_id)(f)   # noqa: E731

    ref = seed_reference_data()
    raw = [load_raw.override(task_id=f"load_raw_{n}")(n) for n in
           ["orders", "payments", "customers", "products", "stores"]] + [load_raw_order_increments()]

    staging = [sql("stg_customers", "10_stg_customers.sql"), sql("stg_products", "11_stg_products.sql"),
               sql("stg_stores", "12_stg_stores.sql"), sql("stg_payments", "13_stg_payments.sql"),
               sql("stg_orders", "14_stg_orders.sql")]
    quality = [sql("dq_results", "20_dq_results.sql"), sql("elt_quarantine", "21_elt_quarantine.sql")]
    recon = sql("mart_payment_reconciliation", "30_mart_payment_reconciliation.sql")
    facts = sql("mart_order_facts", "31_mart_order_facts.sql")
    marts = [sql("mart_daily_store_kpis", "32_mart_daily_store_kpis.sql"),
             sql("mart_category_margin", "33_mart_category_margin.sql"),
             sql("mart_delivery_sla", "34_mart_delivery_sla.sql"),
             sql("mart_top_customers", "35_mart_top_customers.sql")]
    parity = parity_check()
    audit = run_audit()

    for s in staging:
        [ref, *raw] >> s
    for q in quality:
        staging >> q
    quality >> recon >> facts >> marts
    marts >> parity
    [*raw, *staging, *quality, recon, facts, *marts, parity] >> audit


nammamart_elt_karamvir()
