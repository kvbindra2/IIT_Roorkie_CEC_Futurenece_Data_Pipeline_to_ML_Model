"""
dags/nammamart_etl_karamvir.py
------------------------------
NammaMart ETL pipeline (Part A): Extract -> Validate -> Transform -> Load, report ready by 9:00 AM IST.

  extract_orders ─────────────┐
  extract_payments ───────────┤
  extract_customers ──────────┤                ┌──► quarantine ─────────────┐
  extract_products ───────────┼──► validate ───┤                            ├──► load_star_schema ──► reconcile ──► report ──► run_audit
  extract_stores ─────────────┤                └──► transform ──────────────┘                                                  (all_done)
  extract_order_increments ───┘

Only rows that pass every Critical check reach etl_fact_orders. Business logic lives in
include/etl_karamvir.py and include/dq_rules.py (testable without Airflow: `python -m include.etl_karamvir`).

Schedule (Part C)
  cron "0 9 * * 1-6" with a timezone-aware start_date in Asia/Kolkata -> 09:00 IST Mon-Sat = 03:30 UTC.
  catchup=False: each run re-reads the full current extract, so back-filling missed days would only repeat
  the same load; the 9 AM report needs today's data, not a replay of history.
"""
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))     # project root -> `import include.*`

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
    """Run date in IST as a naive timestamp (source timestamps are naive IST)."""
    import pandas as pd
    when = context.get("logical_date") or context.get("data_interval_end") or pendulum.now(IST)
    return pd.Timestamp(pendulum.instance(when).in_timezone(IST).naive())


@dag(
    dag_id="nammamart_etl_karamvir",
    description="NammaMart ETL: validated, reconciled star schema for the 9:30 AM ops review",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, 9, 0, tz=IST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nammamart", "etl", "karamvir"],
    doc_md=__doc__,
)
def nammamart_etl_karamvir():

    # ── EXTRACT: one task per source file, all in parallel ────────────────────────────
    @task
    def extract_orders(**context):
        from include import etl_karamvir as etl
        return etl.extract("orders", context["run_id"])

    @task
    def extract_payments(**context):
        from include import etl_karamvir as etl
        return etl.extract("payments", context["run_id"])

    @task
    def extract_customers(**context):
        from include import etl_karamvir as etl
        return etl.extract("customers", context["run_id"])

    @task
    def extract_products(**context):
        from include import etl_karamvir as etl
        return etl.extract("products", context["run_id"])

    @task
    def extract_stores(**context):
        from include import etl_karamvir as etl
        return etl.extract("stores", context["run_id"])

    @task
    def extract_order_increments(**context):
        """Next-day files dropped into include/data/nammamart/incoming/ (none is normal)."""
        from include import etl_karamvir as etl
        return etl.extract_increments(context["run_id"])

    # ── VALIDATE: every check, every file -> passed / failed + dq_results ─────────────
    @task
    def validate(o, p, c, pr, s, inc, **context):
        from include import etl_karamvir as etl
        return etl.validate_all([o, p, c, pr, s, inc], context["run_id"], _as_of(context))

    # ── QUARANTINE: failed rows with failure_reason, never silently dropped ──────────
    @task
    def quarantine(validated, **context):
        from include import etl_karamvir as etl
        return etl.quarantine(validated, context["run_id"])

    # ── TRANSFORM: standardise, dedupe, mask PII, join, derive measures ───────────────
    @task
    def transform(validated, **context):
        from include import etl_karamvir as etl
        return etl.transform(validated, context["run_id"])

    # ── LOAD: star schema in DuckDB, idempotent upsert ───────────────────────────────
    @task
    def load_star_schema(transformed, quarantined, **context):
        from include import etl_karamvir as etl
        return etl.load(transformed, context["run_id"])

    # ── RECONCILE: clean orders vs payments.csv -> etl_payment_reconciliation ────────
    @task
    def reconcile(transformed, loaded, **context):
        from include import etl_karamvir as etl
        return etl.write_reconciliation(transformed, context["run_id"])

    # ── REPORT: etl_daily_zone_revenue + etl_category_margin, printed to the log ─────
    @task
    def report(reconciled, **context):
        from include import etl_karamvir as etl
        return etl.report(context["run_id"])

    # ── RUN AUDIT: always runs, one row per run in pipeline_runs ─────────────────────
    @task(trigger_rule="all_done", retries=0)
    def run_audit(**context):
        from include import etl_karamvir as etl
        ti = context["ti"]
        pull = lambda tid: ti.xcom_pull(task_ids=tid) or {}                    # noqa: E731  None if task failed
        extract_ids = ["extract_orders", "extract_payments", "extract_customers", "extract_products",
                       "extract_stores", "extract_order_increments"]
        dag_run = context.get("dag_run")
        start = getattr(dag_run, "start_date", None) or pendulum.now(IST)
        status = "SUCCESS" if pull("report") else "FAILED"
        etl.write_run_audit(
            run_id=context["run_id"],
            start_time=pendulum.instance(start).in_timezone(IST).naive(),
            rows_extracted=sum(pull(t).get("rows", 0) for t in extract_ids),
            rows_loaded=pull("load_star_schema").get("rows_loaded", 0),
            rows_quarantined=pull("quarantine").get("rows_quarantined", 0),
            status=status,
        )
        if status == "FAILED":
            raise RuntimeError("Upstream task failed - see pipeline_runs and the failed task's log")

    extracts = [extract_orders(), extract_payments(), extract_customers(), extract_products(), extract_stores(),
                extract_order_increments()]
    validated = validate(*extracts)
    quarantined = quarantine(validated)
    transformed = transform(validated)
    loaded = load_star_schema(transformed, quarantined)
    reconciled = reconcile(transformed, loaded)
    reported = report(reconciled)
    audit = run_audit()
    [*extracts, quarantined, loaded, reconciled, reported] >> audit


nammamart_etl_karamvir()
