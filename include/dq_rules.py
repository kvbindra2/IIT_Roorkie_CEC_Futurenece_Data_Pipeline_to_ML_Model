"""
NammaMart data-quality rules, reconciliation and PII masking.

One source of truth for both pipelines:
  * ETL DAG imports these functions directly (pandas).
  * ELT SQL in include/sql/ must implement the same check names, so dq_results is comparable.

Agreed decisions (see SUBMISSION.md, Part 0):
  D1  order_amount != formula        -> Critical (quarantined) + reconciliation PRICING_ERROR_AT_CHECKOUT
  D2  DELIVERED with FAILED / no pay -> loads, but excluded from net revenue; shown in reconciliation only
  D3  DELIVERED without a usable
      delivery_minutes (blank or >120) -> Warning: counts as revenue, excluded from delivery-SLA metrics
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

import pandas as pd

# --------------------------------------------------------------------------------------
# Reference values
# --------------------------------------------------------------------------------------
KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}

VALID_STATUS = {"DELIVERED", "CANCELLED", "RETURNED"}
VALID_MODES = {"UPI", "Card", "Cash on Delivery", "Wallet"}
VALID_TIERS = {"Bronze", "Silver", "Gold", "Platinum"}
VALID_ZONES = {"North", "South", "East", "West"}
PAYMENT_MODE_MAP = {"UPI": "UPI", "CARD": "Card", "WALLET": "Wallet",
                    "CASH ON DELIVERY": "Cash on Delivery", "COD": "Cash on Delivery",
                    "CASHH": "Cash on Delivery"}
NULL_TOKENS = {"", "N/A", "NA", "NULL", "NONE"}
MAX_DELIVERY_MIN = 120
PROMO_SHORTFALLS = {10.0, 25.0, 50.0, 100.0}      # observed coupon / cashback denominations
AMOUNT_TOLERANCE = 0.01

EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}

# Decision Q1 (flagged member): a master row that fails ONLY these checks is still loaded into its dimension
# with dq_status='QUARANTINED' and the bad field neutralised, so its orders keep their revenue.
# Any other Critical failure on a master row (e.g. a duplicate / '(old)' row) means the row is rejected.
FLAGGED_MEMBER_FIXES = {
    "stores_zone_valid": {"zone": "Unknown"},
    "products_category_present": {"category": "Unknown"},
    "products_mrp_positive": {"mrp": None},
    "products_cost_le_mrp": {"cost_price": None},          # no trusted cost -> excluded from margin
    "customers_signup_not_future": {"signup_date": None},
}

# Timeliness: a record cannot be newer than the period its file covers.
# The client's exports cover 1-30 September 2026 (brief, section 3); next-day files carry their date in the
# file name (orders_2026-10-01_increment.csv). Any other file is assumed to be yesterday's export.
SOURCE_PERIOD_END = {"orders.csv": "2026-09-30", "payments.csv": "2026-09-30", "customers.csv": "2026-09-30"}

# Reconciliation statuses that keep a DELIVERED order out of net revenue (decision D2)
REVENUE_BLOCKING_RECON = {"DELIVERED_PAYMENT_FAILED", "DELIVERED_NO_PAYMENT", "AWAITING_PAYMENT_FEED"}


# --------------------------------------------------------------------------------------
# Small helpers (work on all-text DataFrames, i.e. raw CSVs read with dtype=str)
# --------------------------------------------------------------------------------------
def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def ts(s: pd.Series) -> pd.Series:
    """Strict: only ISO 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD' are accepted."""
    full = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    return full.fillna(pd.to_datetime(s, errors="coerce", format="%Y-%m-%d"))


def business_date(source_file: str | None, run_as_of) -> pd.Timestamp:
    """Last day a file is supposed to cover: date in its name > agreed period end > yesterday."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", source_file or "")
    if m:
        return pd.Timestamp(m.group(1))
    if source_file in SOURCE_PERIOD_END:
        return pd.Timestamp(SOURCE_PERIOD_END[source_file])
    return (pd.Timestamp(run_as_of) - pd.Timedelta(days=1)).normalize()


def _period_end(d: pd.DataFrame, m: dict, default_file: str) -> pd.Series:
    """Per-row business date (rows from different files can sit in one batch)."""
    if "_source_file" in d:
        return d["_source_file"].map(lambda f: business_date(f, m["as_of"]))
    return pd.Series(business_date(default_file, m["as_of"]), index=d.index)


def is_null(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.upper().isin(NULL_TOKENS)


def std_status(s: pd.Series) -> pd.Series:
    return s.fillna("").str.strip().str.upper()


def std_payment_mode(s: pd.Series) -> pd.Series:
    """Canonical mode, or NaN if not recognisable."""
    return s.fillna("").str.strip().str.upper().map(PAYMENT_MODE_MAP)


def expected_amount(df: pd.DataFrame) -> pd.Series:
    return (num(df.quantity) * num(df.unit_price) * (1 - num(df.discount_pct) / 100)).round(2)


# --------------------------------------------------------------------------------------
# PII masking
# --------------------------------------------------------------------------------------
def mask_email(email) -> str | None:
    """priya.hegde199@example.com -> pr***@example.com ; invalid -> None"""
    if not isinstance(email, str) or not re.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$", email.strip()):
        return None
    local, domain = email.strip().split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(phone) -> str | None:
    """9774349021 -> 97******21 ; invalid -> None"""
    digits = re.sub(r"\D", "", phone) if isinstance(phone, str) else ""
    if len(digits) != 10:
        return None
    return f"{digits[:2]}******{digits[-2:]}"


def mask_name(name) -> str | None:
    """Priya Hegde -> Priya H.  (decision Q4: names are personal data too)"""
    if not isinstance(name, str) or not name.strip():
        return None
    parts = name.split()
    return parts[0] if len(parts) == 1 else f"{parts[0]} {parts[-1][0]}."


def check_schema(file: str, df: pd.DataFrame, source: str = "") -> None:
    """Fail loudly if columns are missing or renamed (observability: schema pillar)."""
    expected = EXPECTED_COLUMNS[file]
    cols = [c for c in df.columns if not c.startswith("_")]
    missing = [c for c in expected if c not in cols]
    extra = [c for c in cols if c not in expected]
    if missing:
        raise ValueError(f"Schema change in {source or file}: missing columns {missing}; unexpected columns {extra}")


def dataset_checks(orders: pd.DataFrame, as_of: pd.Timestamp, previous_rows: int | None) -> list[dict]:
    """File-level checks that cannot be expressed per row (Timeliness - stale, Volume)."""
    latest = ts(orders.order_ts)
    latest = latest[latest <= business_date("orders.csv", as_of) + pd.Timedelta(days=1)].max()
    stale = pd.isna(latest) or latest < as_of - pd.Timedelta(days=1)
    out = [{"check_name": "orders_freshness", "dimension": "Timeliness", "severity": "Warning",
            "rows_checked": len(orders), "rows_failed": int(stale), "status": "WARN" if stale else "PASS",
            "detail": f"latest valid order_ts={latest}, as_of={as_of}"}]
    if previous_rows:
        drop = 1 - len(orders) / previous_rows
        out.append({"check_name": "orders_volume_drop", "dimension": "Completeness", "severity": "Warning",
                    "rows_checked": len(orders), "rows_failed": int(drop > 0.9),
                    "status": "WARN" if drop > 0.9 else "PASS",
                    "detail": f"rows={len(orders)}, previous={previous_rows}, change={-drop:.0%}"})
    return out


# --------------------------------------------------------------------------------------
# Check registry
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class Check:
    name: str            # stable id, also used by the ELT SQL and dq_results
    file: str            # orders | payments | customers | products | stores
    column: str
    dimension: str       # Completeness | Uniqueness | Validity | Accuracy | Consistency | Timeliness
    severity: str        # Critical (quarantine) | Warning (load + log)
    problem: str
    rule: str
    root_cause: str
    fn: Callable[[pd.DataFrame, dict], pd.Series]   # returns True where the row FAILS


def _orders_checks() -> list[Check]:
    st =lambda d: std_status(d.order_status)  # noqa: E731
    return [
        Check("orders_order_id_unique", "orders", "order_id", "Uniqueness", "Critical",
              "Duplicate order_id (exact repeat row)", "order_id unique; keep first occurrence",
              "Export duplicated rows",
              lambda d, m: d.order_id.duplicated(keep="first")),
        Check("orders_customer_id_not_null", "orders", "customer_id", "Completeness", "Critical",
              "customer_id missing", "customer_id NOT NULL", "Order captured without customer link",
              lambda d, m: is_null(d.customer_id)),
        Check("orders_order_ts_valid", "orders", "order_ts", "Validity", "Critical",
              "order_ts missing or not a valid ISO timestamp", "order_ts parses as YYYY-MM-DD HH:MM:SS",
              "Mixed date formats / impossible dates (31/09, month 13)",
              lambda d, m: ts(d.order_ts).isna()),
        Check("orders_order_ts_not_future", "orders", "order_ts", "Timeliness", "Critical",
              "order_ts later than the period the file covers", "order_ts <= file business date",
              "Placeholder / test timestamps (same value repeated across customers)",
              lambda d, m: ts(d.order_ts) > _period_end(d, m, "orders.csv") + pd.Timedelta(days=1)),
        Check("orders_status_casing", "orders", "order_status", "Consistency", "Warning",
              "order_status has wrong casing / whitespace", "UPPER(TRIM(order_status))",
              "Free-text entry", lambda d, m: (d.order_status != st(d)) & st(d).isin(VALID_STATUS)),
        Check("orders_status_allowed", "orders", "order_status", "Validity", "Critical",
              "order_status not in allowed list", "order_status IN (DELIVERED, CANCELLED, RETURNED)",
              "Unmapped interim status", lambda d, m: ~st(d).isin(VALID_STATUS)),
        Check("orders_payment_mode_casing", "orders", "payment_mode", "Consistency", "Warning",
              "Non-canonical spelling of a valid payment_mode", "Map upi / 'UPI ' / cod / CARD / Cashh",
              "Free-text entry",
              lambda d, m: ~d.payment_mode.isin(VALID_MODES) & std_payment_mode(d.payment_mode).notna()),
        Check("orders_payment_mode_allowed", "orders", "payment_mode", "Validity", "Critical",
              "payment_mode not recognisable", "payment_mode IN (UPI, Card, Cash on Delivery, Wallet)",
              "Unknown mode", lambda d, m: std_payment_mode(d.payment_mode).isna()),
        Check("orders_quantity_positive", "orders", "quantity", "Accuracy", "Critical",
              "quantity zero or negative", "quantity > 0",
              "Returns / voids keyed as orders (payment mirrors the negative amount)",
              lambda d, m: num(d.quantity) <= 0),
        Check("orders_discount_range", "orders", "discount_pct", "Validity", "Critical",
              "discount_pct outside 0-100", "0 <= discount_pct <= 100", "Data-entry error",
              lambda d, m: ~num(d.discount_pct).between(0, 100)),
        Check("orders_amount_formula", "orders", "order_amount", "Accuracy", "Critical",
              "order_amount != quantity x unit_price x (1 - discount/100)", "|recomputed - order_amount| <= 0.01",
              "Checkout pricing bug: amount is 10x / 1.5x / 0.5x and the customer paid it (D1)",
              lambda d, m: ((expected_amount(d) - num(d.order_amount)).abs() > AMOUNT_TOLERANCE)
                           & num(d.discount_pct).between(0, 100) & (num(d.quantity) > 0)),
        Check("orders_delivery_minutes_present", "orders", "delivery_minutes", "Completeness", "Warning",
              "DELIVERED order without delivery_minutes", "delivery_minutes NOT NULL when DELIVERED",
              "Rider app did not record drop time (D3: revenue yes, SLA no)",
              lambda d, m: (st(d) == "DELIVERED") & num(d.delivery_minutes).isna()),
        Check("orders_delivery_minutes_plausible", "orders", "delivery_minutes", "Accuracy", "Warning",
              f"delivery_minutes > {MAX_DELIVERY_MIN}", f"delivery_minutes <= {MAX_DELIVERY_MIN}",
              "Values are 480/900/1440 = 8h/15h/24h: hours or default filler (D3: revenue yes, SLA no)",
              lambda d, m: num(d.delivery_minutes) > MAX_DELIVERY_MIN),
        Check("orders_customer_fk", "orders", "customer_id", "Consistency", "Critical",
              "customer_id not in customers master", "FK -> customers", "Orphan key",
              lambda d, m: ~is_null(d.customer_id) & ~d.customer_id.isin(m["customer_ids"])),
        Check("orders_product_fk", "orders", "product_id", "Consistency", "Critical",
              "product_id not in products master", "FK -> products", "Orphan key (P9xxx range)",
              lambda d, m: ~d.product_id.isin(m["product_ids"])),
        Check("orders_store_fk", "orders", "store_id", "Consistency", "Critical",
              "store_id not in stores master", "FK -> stores", "Orphan key (S00 / S17 / S99)",
              lambda d, m: ~d.store_id.isin(m["store_ids"])),
    ]


def _payments_checks() -> list[Check]:
    return [
        Check("payments_payment_id_unique", "payments", "payment_id", "Uniqueness", "Critical",
              "Duplicate payment_id (exact repeat row)", "payment_id unique; keep first", "Export duplicated rows",
              lambda d, m: d.payment_id.duplicated(keep="first")),
        Check("payments_order_fk", "payments", "order_id", "Consistency", "Critical",
              "Payment for an order that does not exist", "FK -> orders (goes to reconciliation)",
              "Orphan payment (ORD9xxxxx range)", lambda d, m: ~d.order_id.isin(m["order_ids"])),
        Check("payments_amount_positive", "payments", "amount_paid", "Accuracy", "Critical",
              "amount_paid zero or negative", "amount_paid > 0",
              "Mirrors a quantity<=0 order (same root cause as orders_quantity_positive) or promo > order value",
              lambda d, m: num(d.amount_paid) <= 0),
        Check("payments_ts_not_future", "payments", "payment_ts", "Timeliness", "Critical",
              "payment_ts later than the period the file covers", "payment_ts <= file business date", "Follows placeholder order_ts",
              lambda d, m: ts(d.payment_ts) > _period_end(d, m, "payments.csv") + pd.Timedelta(days=1)),
        Check("payments_mode_casing", "payments", "payment_mode", "Consistency", "Warning",
              "Non-canonical spelling of payment_mode", "Map variants to canonical", "Free-text entry",
              lambda d, m: ~d.payment_mode.isin(VALID_MODES)),
        Check("payments_mode_matches_order", "payments", "payment_mode", "Consistency", "Warning",
              "payment_mode differs from the order's payment_mode", "Order and payment mode agree",
              "Customer switched mode at checkout, or mapping error",
              lambda d, m: d.order_id.map(m["order_mode"]).notna()
                           & (std_payment_mode(d.payment_mode) != d.order_id.map(m["order_mode"]))),
        Check("payments_gateway_ref_present", "payments", "gateway_ref", "Completeness", "Warning",
              "gateway_ref missing for a non-cash payment", "gateway_ref NOT NULL unless COD",
              "Gateway callback missing",
              lambda d, m: is_null(d.gateway_ref) & (std_payment_mode(d.payment_mode) != "Cash on Delivery")),
    ]


def _customers_checks() -> list[Check]:
    return [
        Check("customers_customer_id_unique", "customers", "customer_id", "Uniqueness", "Critical",
              "customer_id repeated with a different phone/area", "One row per customer_id; keep first",
              "Customer moved / changed number -> candidate for SCD Type 2",
              lambda d, m: d.customer_id.duplicated(keep="first")),
        Check("customers_email_valid", "customers", "email", "Validity", "Warning",
              "Invalid or missing email", "email matches name@domain.tld", "No validation at signup",
              lambda d, m: ~d.email.fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")),
        Check("customers_phone_valid", "customers", "phone", "Validity", "Warning",
              "Phone not a 10-digit Indian mobile", "phone ~ ^[6-9][0-9]{9}$", "Digits dropped at entry",
              lambda d, m: ~d.phone.fillna("").str.match(r"^[6-9]\d{9}$")),
        Check("customers_pincode_valid", "customers", "pincode", "Validity", "Warning",
              "Pincode not 6 digits", "pincode ~ ^[0-9]{6}$", "Partial pincode (5600XX)",
              lambda d, m: ~d.pincode.fillna("").str.match(r"^\d{6}$")),
        Check("customers_city_present", "customers", "city", "Completeness", "Warning",
              "city missing", "city NOT NULL (all stores are Bengaluru)", "Optional field at signup",
              lambda d, m: is_null(d.city)),
        Check("customers_tier_casing", "customers", "loyalty_tier", "Consistency", "Warning",
              "loyalty_tier casing inconsistent", "INITCAP(loyalty_tier)", "Free-text entry",
              lambda d, m: ~d.loyalty_tier.isin(VALID_TIERS)),
        Check("customers_signup_not_future", "customers", "signup_date", "Timeliness", "Critical",
              "signup_date later than the period the file covers", "signup_date <= file business date", "Bad date entry",
              lambda d, m: ts(d.signup_date) > _period_end(d, m, "customers.csv")),
    ]


def _products_checks() -> list[Check]:
    return [
        Check("products_product_id_unique", "products", "product_id", "Uniqueness", "Critical",
              "product_id has a superseded '(old)' row", "One row per product_id; drop '(old)' rows",
              "Catalogue keeps old MRP version under the same id",
              lambda d, m: d.product_id.duplicated(keep=False) & d.product_name.str.contains(r"\(old\)", regex=True)),
        Check("products_category_present", "products", "category", "Completeness", "Critical",
              "category missing", "category NOT NULL", "Incomplete catalogue entry",
              lambda d, m: is_null(d.category)),
        Check("products_mrp_positive", "products", "mrp", "Accuracy", "Critical",
              "mrp zero or negative", "mrp > 0", "Sign flipped (P0109 sells at 203, mrp -203)",
              lambda d, m: num(d.mrp) <= 0),
        Check("products_cost_le_mrp", "products", "cost_price", "Accuracy", "Critical",
              "cost_price higher than mrp", "cost_price <= mrp", "Cost entered with markup / wrong field",
              lambda d, m: (num(d.mrp) > 0) & (num(d.cost_price) > num(d.mrp))),
    ]


def _stores_checks() -> list[Check]:
    return [
        Check("stores_zone_valid", "stores", "zone", "Completeness", "Critical",
              "zone missing", "zone IN (North, South, East, West)", "Store master incomplete (S07 is South)",
              lambda d, m: ~d.zone.isin(VALID_ZONES)),
        Check("stores_capacity_positive", "stores", "capacity_orders_per_day", "Accuracy", "Warning",
              "capacity_orders_per_day is zero", "capacity_orders_per_day > 0", "Not maintained",
              lambda d, m: num(d.capacity_orders_per_day) <= 0),
    ]


CHECKS: list[Check] = (_orders_checks() + _payments_checks() + _customers_checks()
                       + _products_checks() + _stores_checks())


# --------------------------------------------------------------------------------------
# Runner
# --------------------------------------------------------------------------------------
def build_context(raw: dict[str, pd.DataFrame], as_of: pd.Timestamp) -> dict:
    """Lookups the checks need from other files (FKs, order payment_mode)."""
    orders = raw["orders"]
    first = orders.drop_duplicates("order_id").set_index("order_id")
    return {
        "as_of": pd.Timestamp(as_of),
        "customer_ids": set(raw["customers"].customer_id),
        "product_ids": set(raw["products"].product_id),
        "store_ids": set(raw["stores"].store_id),
        "order_ids": set(orders.order_id),
        "order_mode": std_payment_mode(first.payment_mode),
    }


def validate(file: str, df: pd.DataFrame, ctx: dict, source_file: str | None = None):
    """
    Run every check for one file.

    Returns
      passed       rows with no Critical failure (+ dq_warnings column)
      failed       rows with >=1 Critical failure (+ failure_reason column) -> quarantine
      results      one row per check: check_name, dimension, severity, rows_checked, rows_failed, status
      issues       one row per (record, failed check): the Data Issues Log
    """
    df = df.reset_index(drop=True)
    key = KEYS[file]
    crit = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    warn = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    results, issues = [], []
    for chk in (c for c in CHECKS if c.file == file):
        mask = chk.fn(df, ctx).fillna(False).astype(bool)
        target = crit if chk.severity == "Critical" else warn
        for i in df.index[mask]:
            target.at[i] = target.at[i] + [chk.name]
        results.append({"check_name": chk.name, "dimension": chk.dimension, "severity": chk.severity,
                        "rows_checked": len(df), "rows_failed": int(mask.sum()),
                        "status": "PASS" if mask.sum() == 0 else ("FAIL" if chk.severity == "Critical" else "WARN")})
        for _, r in df[mask].iterrows():
            issues.append({"file": source_file or f"{file}.csv", "row_key": r[key], "column": chk.column,
                           "value": r.get(chk.column, ""), "check_name": chk.name, "problem": chk.problem,
                           "dimension": chk.dimension, "severity": chk.severity,
                           "proposed_rule": chk.rule, "root_cause": chk.root_cause})
    is_bad = crit.map(len) > 0
    failed = df[is_bad].assign(failure_reason=crit[is_bad].map("; ".join))
    passed = df[~is_bad].assign(dq_warnings=warn[~is_bad].map("; ".join))
    return passed, failed, pd.DataFrame(results), pd.DataFrame(issues)


# --------------------------------------------------------------------------------------
# Reconciliation: orders <-> payments
# --------------------------------------------------------------------------------------
RECON_OWNER = {
    "MATCHED": "-",
    "REFUND_MATCHED": "-",
    "PROMO_SHORTFALL": "Marketing / promo ops",
    "AMOUNT_MISMATCH": "Finance",
    "PRICING_ERROR_AT_CHECKOUT": "Tech (checkout) + Finance",
    "DELIVERED_PAYMENT_FAILED": "Collections",
    "DELIVERED_NO_PAYMENT": "Finance + Tech (payment export)",
    "AWAITING_PAYMENT_FEED": "- (timing difference: order newer than the payment export)",
    "RETURNED_NO_REFUND": "Finance",
    "CANCELLED_WITH_PAYMENT": "Finance (refund due)",
    "NEGATIVE_ORDER": "Ops (returns keyed as orders)",
    "PAYMENT_FOR_QUARANTINED_ORDER": "Data team",
    "ORPHAN_PAYMENT": "Payments team",
    "DUPLICATE_PAYMENT": "Payments team",
}


def reconcile(orders_raw: pd.DataFrame, orders_failed: pd.DataFrame, payments_raw: pd.DataFrame,
              as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    One row per order (non-cancelled, deduped) and per unmatched payment, with recon_status.

    orders_raw     all order rows as received (base file, then increments - later rows win)
    orders_failed  output 'failed' of validate('orders', ...) - carries failure_reason
    payments_raw   all payment rows as received
    as_of          run date; the payment export is assumed complete up to its latest valid payment_ts
    """
    o = orders_raw.drop_duplicates("order_id", keep="last").copy()   # last = latest correction wins
    o["status"] = std_status(o.order_status)
    o["expected"] = num(o.order_amount)
    o["formula"] = expected_amount(o)
    # A repeat row of an order that also passed is not a problem order -> ignore uniqueness-only failures
    real_fail = orders_failed[orders_failed.failure_reason != "orders_order_id_unique"]
    fail_reason = real_fail.drop_duplicates("order_id", keep="last").set_index("order_id").failure_reason

    pay = payments_raw.copy()
    pay["dup"] = pay.payment_id.duplicated(keep="first")
    pay["amount"] = num(pay.amount_paid)
    feed_cutoff = ts(pay.payment_ts)
    as_of = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp.now()
    pay_end = business_date("payments.csv", as_of)
    feed_cutoff = feed_cutoff[feed_cutoff <= pay_end + pd.Timedelta(days=1)].max()   # same rule as payments_ts_not_future
    p1 = pay[~pay.dup]
    best = (p1.assign(rank=p1.payment_status.map({"SUCCESS": 0, "REFUNDED": 1, "FAILED": 2}).fillna(3))
              .sort_values("rank").drop_duplicates("order_id").set_index("order_id"))

    rows = []
    for _, r in o.iterrows():
        oid, reason = r.order_id, fail_reason.get(r.order_id, "")
        p = best.loc[oid] if oid in best.index else None
        pid, pst = (p.payment_id, p.payment_status) if p is not None else (None, None)
        paid = p.amount if p is not None else None

        if "orders_quantity_positive" in reason:
            status = "NEGATIVE_ORDER"
        elif "orders_amount_formula" in reason:
            status = "PRICING_ERROR_AT_CHECKOUT"
        elif reason:
            if p is None:
                continue                     # quarantined order, no money involved -> DQ problem only
            status = "PAYMENT_FOR_QUARANTINED_ORDER"
        elif r.status == "CANCELLED":
            if p is None:
                continue                     # expected: cancelled orders have no payment
            status = "CANCELLED_WITH_PAYMENT"
        elif r.status == "RETURNED":
            status = "REFUND_MATCHED" if pst == "REFUNDED" else "RETURNED_NO_REFUND"
        elif p is None:
            placed = ts(pd.Series([r.order_ts])).iloc[0]
            status = ("AWAITING_PAYMENT_FEED" if pd.notna(placed) and placed > feed_cutoff
                      else "DELIVERED_NO_PAYMENT")
        elif pst == "FAILED":
            status = "DELIVERED_PAYMENT_FAILED"
        else:
            diff = round(r.expected - paid, 2)
            status = ("MATCHED" if abs(diff) <= AMOUNT_TOLERANCE
                      else "PROMO_SHORTFALL" if diff in PROMO_SHORTFALLS else "AMOUNT_MISMATCH")
        rows.append({"order_id": oid, "payment_id": pid, "order_status": r.status, "payment_status": pst,
                     "expected_amount": r.expected, "received_amount": paid,
                     "formula_amount": r.formula, "dq_failure_reason": reason or None,
                     "recon_status": status})

    for _, p in pay[~pay.order_id.isin(o.order_id)].iterrows():
        rows.append({"order_id": p.order_id, "payment_id": p.payment_id, "order_status": None,
                     "payment_status": p.payment_status, "expected_amount": None, "received_amount": p.amount,
                     "formula_amount": None, "dq_failure_reason": None, "recon_status": "ORPHAN_PAYMENT"})
    for _, p in pay[pay.dup].iterrows():
        rows.append({"order_id": p.order_id, "payment_id": p.payment_id, "order_status": None,
                     "payment_status": p.payment_status, "expected_amount": None, "received_amount": p.amount,
                     "formula_amount": None, "dq_failure_reason": None, "recon_status": "DUPLICATE_PAYMENT"})

    rec = pd.DataFrame(rows)
    # Variance = cash actually collected - what we should have collected
    collected = rec.received_amount.where(rec.payment_status == "SUCCESS", 0).fillna(0)
    collected = collected.where(rec.payment_status != "REFUNDED", 0)
    should = rec.expected_amount.where(rec.order_status == "DELIVERED", 0).fillna(0)
    should = should.where(rec.recon_status != "PRICING_ERROR_AT_CHECKOUT", rec.formula_amount)
    rec["variance"] = (collected - should).round(2)
    rec["owner"] = rec.recon_status.map(RECON_OWNER)
    return rec
