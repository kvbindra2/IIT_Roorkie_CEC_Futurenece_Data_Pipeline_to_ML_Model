"""
NammaMart ETL pipeline logic (Part A) - called by dags/nammamart_etl_karamvir.py

Kept out of the DAG file so it can be unit-tested and run without Airflow:
    python -m include.etl_karamvir          # one full local run against include/warehouse_karamvir.duckdb

Flow: extract (6 files) -> validate -> quarantine | transform -> load (star schema, idempotent upsert)
      -> reconcile -> report -> run audit

Decisions applied (see include/dq_rules.py header):
  D1 pricing errors quarantined + reconciled   D2 unpaid deliveries held out of net revenue
  D3 bad delivery time -> revenue yes, SLA no  Q1 bad master rows kept as flagged dimension members
  Q2 increments picked up from incoming/       Q3 PII masked in quarantine   Q4 names shortened in reports
"""
from __future__ import annotations

import pickle
import re
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

try:                                    # works both as `include.etl_karamvir` and flat import
    from include import dq_rules as dq
except ImportError:                     # pragma: no cover
    import dq_rules as dq

# --------------------------------------------------------------------------------------
# Locations (resolved from this file, so the same code works in Docker and on a laptop)
# --------------------------------------------------------------------------------------
INCLUDE = Path(__file__).resolve().parent
DATA_DIR = INCLUDE / "data" / "nammamart"
INCOMING_DIR = DATA_DIR / "incoming"
QUARANTINE_DIR = INCLUDE / "quarantine" / "karamvir"
STAGING_DIR = INCLUDE / "staging" / "karamvir"
WAREHOUSE = INCLUDE / "warehouse_karamvir.duckdb"

DAG_ID = "nammamart_etl_karamvir"
SOURCE_FILES = {"orders": "orders.csv", "payments": "payments.csv", "customers": "customers.csv",
                "products": "products.csv", "stores": "stores.csv"}
INCREMENT_GLOB = "orders_*_increment.csv"


# --------------------------------------------------------------------------------------
# Plumbing
# --------------------------------------------------------------------------------------
def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are illegal in Windows file names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _stage(run_id: str, name: str, obj) -> str:
    d = STAGING_DIR / safe_run_id(run_id)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump(obj, f)
    return str(path)


def _unstage(path: str):
    with open(path, "rb") as f:
        return pickle.load(f)


def connect(retries: int = 30, wait_s: float = 2.0) -> duckdb.DuckDBPyConnection:
    """DuckDB allows one writer process; the ELT DAG may hold the lock at 9:00 too, so wait for it."""
    for attempt in range(retries):
        try:
            return duckdb.connect(str(WAREHOUSE))
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or attempt == retries - 1:
                raise
            print(f"[DuckDB] warehouse locked by another process, retrying in {wait_s}s ({attempt + 1}/{retries})")
            time.sleep(wait_s)


def _replace_table(con, name: str, df: pd.DataFrame) -> None:
    con.register("_df", df)
    con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
    con.unregister("_df")


def _replace_run_rows(con, name: str, df: pd.DataFrame, run_id: str) -> None:
    """Append-only audit tables: delete this run's rows first, so a retry never duplicates them."""
    con.register("_df", df)
    con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS SELECT * FROM _df LIMIT 0")
    con.execute(f"DELETE FROM {name} WHERE run_id = ? AND dag_id = ?", [run_id, DAG_ID])
    con.execute(f"INSERT INTO {name} BY NAME SELECT * FROM _df")
    con.unregister("_df")


IST = ZoneInfo("Asia/Kolkata")


def _now() -> datetime:
    """All audit timestamps are naive IST (the Airflow container clock is UTC)."""
    return datetime.now(IST).replace(tzinfo=None)


# --------------------------------------------------------------------------------------
# 1. EXTRACT
# --------------------------------------------------------------------------------------
def extract(name: str, run_id: str) -> dict:
    """Read one source file exactly as text. Missing file / renamed column -> fail loudly."""
    path = DATA_DIR / SOURCE_FILES[name]
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    dq.check_schema(name, df, path.name)
    df["_source_file"] = path.name
    print(f"[Extract] {path.name}: {len(df)} rows")
    return {"name": name, "file": path.name, "rows": len(df), "path": _stage(run_id, f"raw_{name}", df)}


def extract_increments(run_id: str) -> dict:
    """Pick up every next-day order file dropped into incoming/ (decision Q2). None is normal."""
    files = sorted(INCOMING_DIR.glob(INCREMENT_GLOB))
    frames = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        dq.check_schema("orders", df, f.name)
        frames.append(df.assign(_source_file=f.name))
        print(f"[Extract] {f.name}: {len(df)} rows")
    if not frames:
        print(f"[Extract] no increment files in {INCOMING_DIR}")
    df = (pd.concat(frames, ignore_index=True) if frames
          else pd.DataFrame(columns=dq.EXPECTED_COLUMNS["orders"] + ["_source_file"]))
    return {"name": "orders_increment", "file": ", ".join(f.name for f in files) or None,
            "rows": len(df), "path": _stage(run_id, "raw_orders_increment", df)}


# --------------------------------------------------------------------------------------
# 2. VALIDATE
# --------------------------------------------------------------------------------------
def validate_all(extracts: list[dict], run_id: str, as_of: pd.Timestamp) -> dict:
    raw = {e["name"]: _unstage(e["path"]) for e in extracts}
    inc = raw.pop("orders_increment")
    ctx = dq.build_context(raw, as_of)
    ctx["order_ids"] |= set(inc.order_id)                 # payments may belong to next-day orders

    passed, failed, results, issues = {}, {}, [], []
    for name, df in raw.items():
        p, f, r, i = dq.validate(name, df, ctx, SOURCE_FILES[name])
        passed[name], failed[name] = p, f
        results.append(r.assign(file=SOURCE_FILES[name]))
        issues.append(i)
    if len(inc):
        p, f, r, i = dq.validate("orders", inc, ctx, "orders_increment")
        passed["orders_increment"], failed["orders_increment"] = p, f
        results.append(r.assign(file="orders_increment")); issues.append(i)

    # Dataset-level checks: freshness + volume vs the previous run
    prev = None
    if WAREHOUSE.exists():
        con = connect()
        try:
            prev = con.execute("""SELECT rows_checked FROM dq_results
                                  WHERE dag_id = ? AND check_name = 'orders_order_id_unique' AND run_id <> ?
                                  ORDER BY checked_at DESC LIMIT 1""", [DAG_ID, run_id]).fetchone()
            prev = prev[0] if prev else None
        except duckdb.CatalogException:
            prev = None
        finally:
            con.close()
    ds = pd.DataFrame(dq.dataset_checks(raw["orders"], as_of, prev)).assign(file="orders.csv")
    for _, r in ds.iterrows():
        print(f"[DQ] {r.check_name}: {r.status} ({r.detail})")
    results.append(ds.drop(columns="detail"))

    dq_results = pd.concat(results, ignore_index=True).assign(run_id=run_id, dag_id=DAG_ID, checked_at=_now())
    dq_results = dq_results[["run_id", "dag_id", "file", "check_name", "dimension", "severity",
                             "rows_checked", "rows_failed", "status", "checked_at"]]
    con = connect()
    try:
        _replace_run_rows(con, "dq_results", dq_results, run_id)
    finally:
        con.close()

    for name in passed:
        print(f"[Validate] {name}: passed={len(passed[name])} failed={len(failed[name])}")
    fails = dq_results[dq_results.status != "PASS"]
    print(fails[["file", "check_name", "severity", "rows_failed", "status"]].to_string(index=False))
    return {
        "passed": {k: _stage(run_id, f"passed_{k}", v) for k, v in passed.items()},
        "failed": {k: _stage(run_id, f"failed_{k}", v) for k, v in failed.items()},
        "raw_orders": extracts[[e["name"] for e in extracts].index("orders")]["path"],
        "raw_increment": extracts[[e["name"] for e in extracts].index("orders_increment")]["path"],
        "raw_payments": extracts[[e["name"] for e in extracts].index("payments")]["path"],
        "as_of": pd.Timestamp(as_of).isoformat(),        # XCom must be JSON-serialisable
        "counts": {k: {"passed": len(passed[k]), "failed": len(failed[k])} for k in passed},
    }


# --------------------------------------------------------------------------------------
# 3. QUARANTINE
# --------------------------------------------------------------------------------------
def quarantine(validated: dict, run_id: str) -> dict:
    """Failed rows -> include/quarantine/karamvir/<file>_failed_<run_id>.csv (+ etl_quarantine_log table)."""
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    log, written = [], {}
    for name, path in validated["failed"].items():
        df = _unstage(path)
        if df.empty:
            continue
        if name == "customers":                                         # decision Q3: no raw PII leaves the source
            df = df.assign(email=df.email.map(dq.mask_email), phone=df.phone.map(dq.mask_phone))
        out = QUARANTINE_DIR / f"{name}_failed_{safe_run_id(run_id)}.csv"
        df.to_csv(out, index=False)
        written[name] = len(df)
        key = dq.KEYS["orders" if name == "orders_increment" else name]
        log.append(pd.DataFrame({"run_id": run_id, "dag_id": DAG_ID, "source_file": df["_source_file"],
                                 "row_key": df[key], "failure_reason": df.failure_reason,
                                 "quarantined_at": _now()}))
        print(f"[Quarantine] {len(df):>4} rows -> {out.name}")
    if log:
        con = connect()
        try:
            _replace_run_rows(con, "etl_quarantine_log", pd.concat(log, ignore_index=True), run_id)
        finally:
            con.close()
    return {"rows_quarantined": sum(written.values()), "by_file": written}


# --------------------------------------------------------------------------------------
# 4. TRANSFORM
# --------------------------------------------------------------------------------------
def _flagged_members(failed: pd.DataFrame) -> pd.DataFrame:
    """Decision Q1: master rows whose only Critical failures are fixable become flagged members."""
    if failed.empty:
        return failed
    reasons = failed.failure_reason.str.split("; ")
    fixable = reasons.map(lambda rs: all(r in dq.FLAGGED_MEMBER_FIXES for r in rs))
    out = failed[fixable].copy()
    for i, rs in reasons[fixable].items():
        for r in rs:
            for col, val in dq.FLAGGED_MEMBER_FIXES[r].items():
                out.at[i, col] = val
    return out.assign(dq_status="QUARANTINED", dq_notes=out.failure_reason).drop(columns="failure_reason")


def _master(validated: dict, name: str) -> pd.DataFrame:
    p = _unstage(validated["passed"][name])
    p = p.assign(dq_status=p.dq_warnings.map(lambda w: "WARNING" if w else "OK"),
                 dq_notes=p.dq_warnings.replace("", None)).drop(columns="dq_warnings")
    return pd.concat([p, _flagged_members(_unstage(validated["failed"][name]))], ignore_index=True)


def transform(validated: dict, run_id: str) -> dict:
    loaded_at = _now()

    # ---- dimensions
    c = _master(validated, "customers")
    dim_customer = pd.DataFrame({
        "customer_id": c.customer_id,
        "customer_name": c.full_name.map(dq.mask_name),                 # Q4
        "email_masked": c.email.map(dq.mask_email),
        "phone_masked": c.phone.map(dq.mask_phone),
        "city": c.city.where(~dq.is_null(c.city), None),
        "area": c.area,
        "pincode": c.pincode.where(c.pincode.str.match(r"^\d{6}$"), None),
        "signup_date": pd.to_datetime(c.signup_date, errors="coerce").dt.date,
        "loyalty_tier": c.loyalty_tier.str.strip().str.title(),
        "is_active": c.is_active.str.upper().eq("Y"),
        "dq_status": c.dq_status, "dq_notes": c.dq_notes,
    })

    p = _master(validated, "products")
    dim_product = pd.DataFrame({
        "product_id": p.product_id, "product_name": p.product_name, "category": p.category,
        "sub_category": p.sub_category, "brand": p.brand,
        "mrp": dq.num(p.mrp), "cost_price": dq.num(p.cost_price), "unit": p.unit,
        "is_perishable": p.is_perishable.str.lower().eq("true"),
        "launch_date": pd.to_datetime(p.launch_date, errors="coerce").dt.date,
        "dq_status": p.dq_status, "dq_notes": p.dq_notes,
    })

    s = _master(validated, "stores")
    dim_store = pd.DataFrame({
        "store_id": s.store_id, "store_name": s.store_name, "zone": s.zone, "area": s.area,
        "pincode": s.pincode, "open_date": pd.to_datetime(s.open_date, errors="coerce").dt.date,
        "manager_name": s.manager_name,
        "capacity_orders_per_day": dq.num(s.capacity_orders_per_day).where(lambda x: x > 0).astype("Int64"),
        "dq_status": s.dq_status, "dq_notes": s.dq_notes,
    })

    # ---- orders: base file then increments; the latest version of an order_id wins
    o = pd.concat([_unstage(validated["passed"]["orders"]),
                   _unstage(validated["passed"]["orders_increment"]) if "orders_increment" in validated["passed"]
                   else pd.DataFrame()], ignore_index=True)
    o = o.drop_duplicates("order_id", keep="last").reset_index(drop=True)
    passed_ids = set(o.order_id)
    failed_orders = pd.concat([_unstage(v) for k, v in validated["failed"].items() if k.startswith("orders")],
                              ignore_index=True)
    stale_ids = sorted(set(failed_orders.order_id) - passed_ids)        # remove from fact if loaded earlier

    # ---- reconciliation (needed before the fact: D2 decides revenue eligibility)
    raw_orders = pd.concat([_unstage(validated["raw_orders"]), _unstage(validated["raw_increment"])],
                           ignore_index=True)
    recon = dq.reconcile(raw_orders, failed_orders[~failed_orders.order_id.isin(passed_ids)],
                         _unstage(validated["raw_payments"]), pd.Timestamp(validated["as_of"]))

    status = dq.std_status(o.order_status)
    qty = dq.num(o.quantity).astype("Int64")
    amount = dq.num(o.order_amount)
    dmin = dq.num(o.delivery_minutes)
    fact = pd.DataFrame({
        "order_id": o.order_id,
        "order_ts": pd.to_datetime(o.order_ts, format="%Y-%m-%d %H:%M:%S"),
        "customer_id": o.customer_id, "store_id": o.store_id, "product_id": o.product_id,
        "quantity": qty, "unit_price": dq.num(o.unit_price), "discount_pct": dq.num(o.discount_pct),
        "order_amount": amount,
        "payment_mode": dq.std_payment_mode(o.payment_mode),
        "order_status": status, "channel": o.channel.str.strip(),
        "delivery_minutes": dmin,
        "dq_warnings": o.dq_warnings.replace("", None),
        "_source_file": o._source_file, "_run_id": run_id, "_loaded_at": loaded_at,
    })
    fact.insert(2, "order_date", fact.order_ts.dt.date)
    fact = fact.merge(recon[["order_id", "recon_status"]].drop_duplicates("order_id"), on="order_id", how="left")
    fact["recon_status"] = fact.recon_status.fillna("NOT_APPLICABLE")       # cancelled, no payment expected

    # All derived columns are computed from fact's own columns (never from `o`) so rows cannot misalign
    assert len(fact) == len(o), "recon merge must not change the fact grain"
    cost = fact.product_id.map(dim_product.set_index("product_id").cost_price)
    delivered = fact.order_status == "DELIVERED"
    fact["revenue_eligible"] = delivered & ~fact.recon_status.isin(dq.REVENUE_BLOCKING_RECON)      # D2
    fact["net_revenue"] = fact.order_amount.where(fact.revenue_eligible, 0.0).round(2)
    fact["cost_amount"] = (fact.quantity.astype(float) * cost).where(fact.revenue_eligible, 0.0).round(2)
    fact["gross_margin"] = (fact.net_revenue - fact.cost_amount).round(2)                       # NaN if no cost
    fact["sla_eligible"] = delivered & fact.delivery_minutes.notna() & (fact.delivery_minutes <= dq.MAX_DELIVERY_MIN)  # D3
    fact["within_15_min"] = (fact.delivery_minutes <= 15).where(fact.sla_eligible, None)
    bad = fact.revenue_eligible & (fact.net_revenue != fact.order_amount)
    assert not bad.any(), f"net_revenue misaligned for {fact.order_id[bad].tolist()[:5]}"

    print(f"[Transform] dims: customers={len(dim_customer)} products={len(dim_product)} stores={len(dim_store)}"
          f" | fact rows={len(fact)} | stale ids to remove={len(stale_ids)}")
    print(f"[Transform] flagged members: customers={int((dim_customer.dq_status == 'QUARANTINED').sum())}"
          f" products={int((dim_product.dq_status == 'QUARANTINED').sum())}"
          f" stores={int((dim_store.dq_status == 'QUARANTINED').sum())}")
    return {
        "dim_customer": _stage(run_id, "dim_customer", dim_customer),
        "dim_product": _stage(run_id, "dim_product", dim_product),
        "dim_store": _stage(run_id, "dim_store", dim_store),
        "fact_orders": _stage(run_id, "fact_orders", fact),
        "recon": _stage(run_id, "recon", recon),
        "stale_ids": stale_ids,
    }


# --------------------------------------------------------------------------------------
# 5. LOAD - star schema, idempotent
# --------------------------------------------------------------------------------------
def load(transformed: dict, run_id: str) -> dict:
    """
    Dimensions: full replace (small masters, always the current truth).
    Fact: upsert on order_id (delete keys in this batch + stale keys, then insert) inside one transaction,
          so re-running never double-counts and late status changes overwrite the old row.
    """
    fact = _unstage(transformed["fact_orders"])
    con = connect()
    try:
        con.execute("BEGIN TRANSACTION")
        for t in ["dim_customer", "dim_product", "dim_store"]:
            _replace_table(con, f"etl_{t}", _unstage(transformed[t]))

        con.register("_fact", fact)
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM _fact LIMIT 0")
        existing = set(r[0] for r in con.execute("SELECT order_id FROM etl_fact_orders").fetchall())
        # Never let the older base file overwrite a correction that arrived in an increment file
        protected = set(r[0] for r in con.execute(f"""
            SELECT t.order_id FROM etl_fact_orders t JOIN _fact f USING (order_id)
            WHERE t._source_file LIKE '%increment%' AND f._source_file = '{SOURCE_FILES["orders"]}'""").fetchall())
        if protected:
            print(f"[Load] {len(protected)} orders keep their newer increment version (base file is older)")
            fact = fact[~fact.order_id.isin(protected)]
            con.unregister("_fact")
            con.register("_fact", fact)
        batch = set(fact.order_id)
        stale = set(transformed["stale_ids"]) & existing
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM _fact)")
        if stale:
            con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT UNNEST(?))", [list(stale)])
        con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM _fact")
        con.unregister("_fact")
        total = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    stats = {"rows_loaded": len(batch), "inserted": len(batch - existing), "updated": len(batch & existing),
             "removed_stale": len(stale), "fact_total": total}
    print(f"[Load] etl_fact_orders: {stats}")
    return stats


# --------------------------------------------------------------------------------------
# 6. RECONCILE
# --------------------------------------------------------------------------------------
def write_reconciliation(transformed: dict, run_id: str) -> dict:
    """etl_payment_reconciliation: one row per order/payment mismatch, with reason, variance and owner."""
    recon = _unstage(transformed["recon"])
    mism = recon[~recon.recon_status.isin(["MATCHED", "REFUND_MATCHED"])].assign(run_id=run_id, reconciled_at=_now())
    con = connect()
    try:
        _replace_table(con, "etl_payment_reconciliation", mism)
    finally:
        con.close()
    summary = (recon.groupby("recon_status").agg(rows=("recon_status", "size"), variance=("variance", "sum"))
                    .round(2).sort_values("rows", ascending=False))
    print("[Reconcile]\n" + summary.to_string())
    return {"mismatches": len(mism), "net_variance": float(mism.variance.sum().round(2))}


# --------------------------------------------------------------------------------------
# 7. REPORT
# --------------------------------------------------------------------------------------
REPORT_SQL = {
    "etl_daily_zone_revenue": """
        SELECT f.order_date, s.zone, s.store_id, s.store_name,
               COUNT(*) FILTER (WHERE f.revenue_eligible)                         AS delivered_orders,
               ROUND(SUM(f.net_revenue), 2)                                       AS net_revenue,
               ROUND(SUM(f.order_amount) FILTER (WHERE f.order_status = 'DELIVERED'
                                                AND NOT f.revenue_eligible), 2)  AS held_unpaid_revenue
        FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
        GROUP BY ALL ORDER BY f.order_date, s.zone, s.store_id""",
    "etl_category_margin": """
        SELECT p.category,
               COUNT(*) FILTER (WHERE f.revenue_eligible)                         AS delivered_orders,
               ROUND(SUM(f.net_revenue), 2)                                       AS net_revenue,
               ROUND(SUM(f.cost_amount), 2)                                       AS cost,
               ROUND(SUM(f.gross_margin), 2)                                      AS gross_margin,
               ROUND(100 * SUM(f.gross_margin) / NULLIF(SUM(f.net_revenue)
                     FILTER (WHERE f.gross_margin IS NOT NULL), 0), 1)            AS margin_pct,
               COUNT(*) FILTER (WHERE f.revenue_eligible AND f.cost_amount IS NULL) AS orders_without_cost
        FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
        GROUP BY ALL ORDER BY net_revenue DESC""",
}


def report(run_id: str) -> dict:
    con = connect()
    try:
        for name, sql in REPORT_SQL.items():
            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
        zone = con.execute("""SELECT zone, SUM(delivered_orders) AS orders, ROUND(SUM(net_revenue), 2) AS net_revenue,
                                     ROUND(SUM(held_unpaid_revenue), 2) AS held_unpaid
                              FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC""").df()
        cat = con.execute("SELECT * FROM etl_category_margin").df()
        total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
    finally:
        con.close()
    print("[Report] Net revenue by zone\n" + zone.to_string(index=False))
    print("[Report] Category margin\n" + cat.to_string(index=False))
    print(f"[Report] Total net revenue: {total:,.2f}")
    return {"net_revenue": float(total)}


# --------------------------------------------------------------------------------------
# 8. RUN AUDIT
# --------------------------------------------------------------------------------------
def write_run_audit(run_id: str, start_time, rows_extracted, rows_loaded, rows_quarantined, status: str) -> None:
    row = pd.DataFrame([{"run_id": run_id, "dag_id": DAG_ID, "start_time": start_time, "end_time": _now(),
                         "rows_extracted": rows_extracted, "rows_loaded": rows_loaded,
                         "rows_quarantined": rows_quarantined, "status": status}])
    con = connect()
    try:
        _replace_run_rows(con, "pipeline_runs", row, run_id)
    finally:
        con.close()
    print(f"[Audit] {row.to_dict('records')[0]}")


# --------------------------------------------------------------------------------------
# Local run without Airflow (same steps, same order)
# --------------------------------------------------------------------------------------
def run_local(run_id: str | None = None, as_of: pd.Timestamp | None = None) -> None:
    run_id = run_id or f"local__{datetime.now():%Y%m%dT%H%M%S}"
    as_of = as_of or pd.Timestamp(_now())
    start = _now()
    ex = [extract(n, run_id) for n in SOURCE_FILES] + [extract_increments(run_id)]
    v = validate_all(ex, run_id, as_of)
    q = quarantine(v, run_id)
    t = transform(v, run_id)
    ld = load(t, run_id)
    write_reconciliation(t, run_id)
    report(run_id)
    write_run_audit(run_id, start, sum(e["rows"] for e in ex), ld["rows_loaded"], q["rows_quarantined"], "SUCCESS")


if __name__ == "__main__":
    run_local()
