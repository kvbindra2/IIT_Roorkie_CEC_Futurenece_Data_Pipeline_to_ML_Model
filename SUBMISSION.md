# NammaMart ETL & ELT Pipelines: Submission (Karamvir)

FDAIE Module 2 · Data Engineering Foundations

| Item | Location |
| --- | --- |
| ETL DAG | `dags/nammamart_etl_karamvir.py` (logic in `include/etl_karamvir.py`) |
| ELT DAG | `dags/nammamart_elt_karamvir.py` (SQL in `include/sql/`, runner in `include/elt_karamvir.py`) |
| Shared quality rules, reconciliation, masking | `include/dq_rules.py` |
| Profiling notebook / Data Issues Log | `assignment/part0_profiling.ipynb`, `assignment/data_issues_log.csv` |
| Warehouse | `include/warehouse_karamvir.duckdb` (git-ignored: raw layer holds unmasked PII) |

---

## Decision log (agreed during discovery; referenced throughout)

| # | Decision | Where it lives in code |
| --- | --- | --- |
| **D1** | An order whose `order_amount` breaks the formula (10×, 1.5×, 0.5×) is **quarantined** from finance tables, but the customer *was* charged that amount, so it is also listed in reconciliation as `PRICING_ERROR_AT_CHECKOUT`. | `orders_amount_formula` (Critical); `reconcile()` / `30_mart_payment_reconciliation.sql` |
| **D2** | A DELIVERED order with a FAILED payment, no payment, or a payment not yet received in the feed **stays in the fact table but earns no `net_revenue`** until cash is confirmed. It appears only in reconciliation. | `REVENUE_BLOCKING_RECON`; `revenue_eligible` |
| **D3** | A DELIVERED order with a missing or impossible delivery time (> 120 min) **counts for revenue but not for the delivery SLA**: severity Warning. | `sla_eligible`, `within_15_min` |
| **Q1** | A master row (product / store / customer) failing one field is **quarantined *and* kept as a flagged dimension member**: `dq_status='QUARANTINED'`, bad field set to `Unknown`/NULL. Its orders keep their revenue (₹1.6L, incl. all 172 orders of S07). | `FLAGGED_MEMBER_FIXES`; `stg_stores` / `stg_products` / `stg_customers` |
| **Q2** | Next-day files are picked up from `include/data/nammamart/incoming/`. A correction from an increment file is **never overwritten by the older base file**. | `extract_increments()`; `MERGE … WHEN MATCHED AND NOT (…increment…)` |
| **Q3** | Email and phone are **masked everywhere outside the source files**, including quarantine files. | `mask_email`, `mask_phone` |
| **Q4** | Customer names appear as **first name + last initial** ("Priya H.") in reporting tables. | `mask_name` |

---

## Part 0: Discovery and data profiling

**Notebook:** `assignment/part0_profiling.ipynb`, which reads every file **as text**, the way the ELT raw layer does. It uses the same rule module as both pipelines (`include/dq_rules.py`), so the issues log and the pipelines can't disagree.
**Data Issues Log:** `assignment/data_issues_log.csv`, **209 rows**, one per failing record. Columns: `file, row_key, column, value, check_name, problem, dimension, severity, proposed_rule, root_cause`.

### What the profiling found

| File | Rows | Key findings |
| --- | ---: | --- |
| orders.csv | 2,412 (2,400 unique) | 12 exact duplicate rows; 18 blank `customer_id`; 6 unparseable `order_ts` (`N/A`, `31/09/2026`, `2026-13-05`); 7 placeholder future dates (the same 3 timestamps repeated, e.g. `2027-01-01 09:00`); 15 amounts breaking the formula, **exactly 10×, 1.5× or 0.5× the correct value, and paid at that wrong amount**; 10 orders with quantity ≤ 0 (returns keyed as orders, mirrored by ≤ 0 payments); 3 discounts over 100%; 3 invalid statuses (`Pending??`); 9 unknown products (P9xxx), 5 unknown stores (S00, S17, S99); 6 deliveries of 480/900/1440 minutes (8 h / 15 h / 24 h) and 8 delivered orders with no time; casing variants (`upi`, `UPI `, `cod`, `Cashh`, `delivered`) |
| payments.csv | 2,228 | 4 duplicate rows; 8 payments for orders that don't exist; 10 negative or zero amounts (mirroring the quantity ≤ 0 orders); 7 future timestamps; 14 SUCCESS payments short by exactly **₹10 / 25 / 50 / 100** (promo or cashback); 44 delivered orders with only a FAILED payment; 27 delivered orders with no payment at all; REFUNDED ↔ RETURNED match 1:1 (68) |
| customers.csv | 405 | 5 customer_ids repeated with a different phone/area (a customer who moved: SCD2 candidate); 12 invalid emails, 7 short phones, 3 pincodes `5600XX`, 6 blank cities, 8 tier casing variants (`BRONZE`, `bronze`), 3 future signup dates |
| products.csv | 152 | P0030 and P0140 each have a current row and an "(old)" row with a lower MRP; 3 blank categories; **P0109 MRP −203** (sign flipped; it sells at ₹203); 4 products with cost above MRP |
| stores.csv | 15 | S07 (BTM Layout) has no zone; S12 has capacity 0 |
| orders_2026-10-01_increment.csv | 160 | Clean: 150 new orders plus exactly 10 corrections (DELIVERED → RETURNED) |

**Referential integrity:** orders → customers: 0 orphans (the 18 problems are blank IDs). Orders → products: 9 orphans. Orders → stores: 5 orphans. Payments → orders: 8 orphans; cancelled orders never have a payment (as the client said).

### Data Issues Log: summary

Issues were found in **every one of the 5 data files and all 6 dimensions.** 136 rows are Critical (quarantined) and 73 are Warnings (loaded and logged).

| File | Accuracy | Completeness | Consistency | Timeliness | Uniqueness | Validity | **Total** | Critical / Warning |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| orders.csv | 31 | 26 | 26 | 7 | 12 | 12 | **114** | 88 / 26 |
| customers.csv | 0 | 6 | 8 | 3 | 5 | 22 | **44** | 8 / 36 |
| payments.csv | 10 | 0 | 18 | 7 | 4 | 0 | **39** | 29 / 10 |
| products.csv | 5 | 3 | 0 | 0 | 2 | 0 | **10** | 10 / 0 |
| stores.csv | 1 | 1 | 0 | 0 | 0 | 0 | **2** | 1 / 1 |
| **All** | **47** | **36** | **52** | **17** | **23** | **34** | **209** | **136 / 73** |

Each check's rule, dimension and severity is in Part D. The log adds a `root_cause` per row (e.g. "Checkout pricing bug: amount is 10x / 1.5x / 0.5x and the customer paid it"), so each issue can be routed to the team that fixes it at source.

### Questions for the client before building

1. **Timestamps:** `order_ts` has values like `31/09/2026`, `2026-13-05` and `N/A`. Is `order_ts` the order-placed time from the app server, and can ops send a corrected export, or should these rows stay quarantined until fixed at source?
2. **Which number is "truth" for the CFO:** order revenue, or cash collected at the gateway? Some SUCCESS payments are ₹10–100 short of the order (a promo?), and 15 orders were charged 10× or 0.5× the formula. And is a return reversed in the month of the return or the month of the sale?
3. **Duplicate masters:** `C00036` appears twice with different phones and areas, and `P0030` has a current and an "(old)" row with different MRPs. Which record wins: the latest, or does the old MRP apply to orders placed before a change date (do you need price history)?
4. *(Optional)* **Delivery outliers:** are 480 / 900 / 1440-minute deliveries data-entry errors (hours vs minutes) or genuinely failed deliveries that should be excluded from the 10-minute-promise KPI but kept in revenue?

The answers we agreed are the **decision log** at the top of this document (D1–D3, Q1–Q4).

---

## Part A / B: The two pipelines

### Part A: ETL, `dags/nammamart_etl_karamvir.py` (Extract → Validate → Transform → Load)

```
extract_orders ─┐
extract_payments┤                ┌─► quarantine ─┐
extract_customers┼─► validate ───┤               ├─► load_star_schema ─► reconcile ─► report ─► run_audit (all_done)
extract_products┤                └─► transform ──┘
extract_stores ─┤
extract_order_increments ┘
```

| Brief requirement | How it's done |
| --- | --- |
| Extract: 5 parallel tasks | 6 parallel tasks: one per file, plus `extract_order_increments`, which picks up next-day files from `include/data/nammamart/incoming/` (Q2). Files are read as text, with a schema check before anything else. |
| Validate | `dq_rules.validate()` runs every check for that file, splits rows into passed / failed, and writes every result to `dq_results`. |
| Quarantine | `include/quarantine/karamvir/<file>_failed_<run_id>.csv` with `failure_reason` (email/phone masked, Q3), plus the `etl_quarantine_log` table. |
| Transform | Standardise (status upper-case, payment-mode mapping, tier casing); de-duplicate customers and products; mask email, phone and name (Q3/Q4); join; derive `order_date`, `net_revenue` (D2), `gross_margin`, `within_15_min` (D3). Bad master rows are kept as **flagged members** (Q1). |
| Load: star schema, idempotent | `etl_fact_orders` + `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`. Dimensions are fully replaced; the fact is **upserted on `order_id` in one transaction**. An increment correction is never overwritten or deleted by the older base file. |
| Reconcile | `etl_payment_reconciliation`: one row per mismatch with `recon_status`, variance, owner. |
| Report | `etl_daily_zone_revenue`, `etl_category_margin`, printed in the task log. |
| Plus | `pipeline_runs` audit row on every run (Part F) |

Logic lives in `include/etl_karamvir.py` and can run without Airflow (`python -m include.etl_karamvir`).

### Part B: ELT, `dags/nammamart_elt_karamvir.py` (Extract → Load raw → Transform in DuckDB with SQL)

| Layer | Tables | How |
| --- | --- | --- |
| **Raw** | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | Every CSV row **as text**, plus `_loaded_at`, `_source_file`, `_run_id` and `_row_num`. Append-only, never cleaned. A retry replaces only its own run's rows. |
| **Staging** | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | `include/sql/10–14_*.sql`: cast, standardise, dedupe, mask PII, and apply every check. Each row carries `dq_status` + `failure_reason`, and per-row results go to `stg_dq_row_results` → `dq_results`, `elt_quarantine`. |
| **Mart** | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers` (+ `mart_order_facts`, the order-grain twin of the ETL fact) | `include/sql/30–35_*.sql`. Built only from rows that pass every Critical check. |

- **One SQL file per table, one Airflow task per file** (21 tasks). Shared rule helpers live in `include/sql/00_macros.sql` and mirror `dq_rules.py`. The check catalogue is seeded into `ref_dq_checks`.
- **Incremental load** (`14_stg_orders.sql`): the increment is appended to `raw_orders`, then **`MERGE INTO stg_orders … ON order_id`** runs, with an `elt_merge_log` row recording inserted / updated / protected / status changes. In Airflow it showed **150 inserted, 2,400 updated, 10 status changes**, with no duplicates.
- **Totals match the ETL:** the `parity_check` task compares September order by order on every run and writes `etl_elt_parity`. Final run: ETL = ELT, **2,324 September orders, ₹10,96,306.20 net revenue, ₹2,57,534.55 gross margin**. All 37 `dq_results` checks have identical counts.

### The six business questions: SQL and results from both pipelines

Results come from the final run on 16 October 2026 (September export plus the 1 October increment). **Every answer is identical in the ETL and the ELT**, with the one documented exception in Q4.

**Q1. Daily net revenue by zone and by store** (delivered orders only, after discount; D2: unpaid deliveries are held)

By zone, whole period:
<details><summary>SQL</summary>

```sql
-- ETL
SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue),2) AS net_revenue
FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC;
-- ELT
SELECT zone, sum(delivered_orders) AS delivered_orders, round(sum(net_revenue),2) AS net_revenue
FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC;
```
</details>

| zone | delivered_orders | net_revenue |
| --- | --- | --- |
| East | 548 | ₹288,675.75 |
| West | 542 | ₹281,928.80 |
| South | 389 | ₹225,871.45 |
| North | 400 | ₹217,924.40 |
| Unknown | 144 | ₹81,905.80 |


By store, whole period:

| zone | store_id | store_name | delivered_orders | net_revenue |
| --- | --- | --- | --- | --- |
| South | S06 | NammaMart Banashankari | 138 | ₹83,183.00 |
| West | S13 | NammaMart Malleshwaram | 139 | ₹82,531.40 |
| Unknown | S07 | NammaMart BTM Layout | 144 | ₹81,905.80 |
| East | S10 | NammaMart Marathahalli | 134 | ₹78,084.75 |
| South | S05 | NammaMart JP Nagar | 127 | ₹76,808.70 |
| West | S15 | NammaMart Basaveshwaranagar | 147 | ₹75,595.25 |
| North | S03 | NammaMart RT Nagar | 133 | ₹74,351.40 |
| East | S11 | NammaMart HSR Layout | 141 | ₹74,064.00 |
| North | S01 | NammaMart Hebbal | 132 | ₹72,470.55 |
| East | S08 | NammaMart Whitefield | 143 | ₹71,564.35 |
| North | S02 | NammaMart Yelahanka | 135 | ₹71,102.45 |
| West | S14 | NammaMart Vijayanagar | 147 | ₹68,028.25 |
| South | S04 | NammaMart Jayanagar | 124 | ₹65,879.75 |
| East | S09 | NammaMart Indiranagar | 130 | ₹64,962.65 |
| West | S12 | NammaMart Rajajinagar | 109 | ₹55,773.90 |

Daily grain: example day, 15 September 2026 (the tables hold one row per date × store):

<details><summary>SQL</summary>

```sql
-- ETL
SELECT order_date, zone, round(sum(net_revenue),2) AS net_revenue FROM etl_daily_zone_revenue
WHERE order_date = DATE '2026-09-15' GROUP BY ALL ORDER BY zone;
-- ELT
SELECT order_date, zone, round(sum(net_revenue),2) AS net_revenue FROM mart_daily_store_kpis
WHERE order_date = DATE '2026-09-15' GROUP BY ALL ORDER BY zone;
```
</details>

| order_date | zone | net_revenue |
| --- | --- | --- |
| 2026-09-15 00:00:00 | East | ₹9,415.60 |
| 2026-09-15 00:00:00 | North | ₹9,051.15 |
| 2026-09-15 00:00:00 | South | ₹3,638.20 |
| 2026-09-15 00:00:00 | Unknown | ₹2,695.00 |
| 2026-09-15 00:00:00 | West | ₹12,648.60 |

Zone `Unknown` is store S07 (BTM Layout), a flagged member whose zone is missing in the store master (Q1). Its ₹81,905.80 is real revenue the store-master owner needs to fix.
**Q2. Revenue and gross margin by category** (margin = revenue − quantity × cost_price)

<details><summary>SQL</summary>

```sql
-- ETL
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct, orders_without_cost FROM etl_category_margin ORDER BY net_revenue DESC;
-- ELT
SELECT category, delivered_orders, net_revenue, gross_margin, margin_pct, orders_without_cost FROM mart_category_margin ORDER BY net_revenue DESC;
```
</details>

| category | delivered_orders | net_revenue | gross_margin | margin_pct | orders_without_cost |
| --- | --- | --- | --- | --- | --- |
| Staples | 297 | ₹258,026.70 | ₹60,369.21 | 25.1 | 30 |
| Personal Care | 319 | ₹195,918.10 | ₹43,184.70 | 22.1 | 11 |
| Household | 332 | ₹194,731.55 | ₹49,675.01 | 25.5 | 0 |
| Dairy & Bakery | 367 | ₹161,957.05 | ₹42,748.66 | 26.4 | 0 |
| Snacks & Beverages | 312 | ₹157,101.85 | ₹36,838.30 | 23.4 | 0 |
| Fruits & Vegetables | 356 | ₹89,673.30 | ₹20,295.82 | 24.1 | 17 |
| Unknown | 40 | ₹38,897.65 | ₹4,422.85 | 11.4 | 0 |

`orders_without_cost` = orders for products whose cost failed validation (cost > MRP, Q1). They count for revenue but are excluded from margin, so margin is never computed from an untrusted cost. `Unknown` = the 3 products with a blank category.
**Q3. Average delivery time per store, and % of delivered orders within 15 minutes** (D3: only recorded times ≤ 120 min count)

<details><summary>SQL</summary>

```sql
-- ETL
SELECT s.zone, f.store_id, s.store_name,
       round(avg(f.delivery_minutes) FILTER (WHERE f.sla_eligible),1) AS avg_delivery_minutes,
       round(100.0*count(*) FILTER (WHERE f.within_15_min)/count(*) FILTER (WHERE f.sla_eligible),1) AS pct_within_15_min
FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
GROUP BY ALL ORDER BY pct_within_15_min DESC;
-- ELT
SELECT zone, store_id, store_name, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla ORDER BY pct_within_15_min DESC;
```
</details>

| zone | store_id | store_name | avg_delivery_minutes | pct_within_15_min |
| --- | --- | --- | --- | --- |
| East | S08 | NammaMart Whitefield | 20.6 | 35.4 |
| Unknown | S07 | NammaMart BTM Layout | 21.1 | 32.7 |
| North | S01 | NammaMart Hebbal | 20.5 | 32.2 |
| North | S02 | NammaMart Yelahanka | 20.9 | 31.3 |
| West | S14 | NammaMart Vijayanagar | 21.5 | 31.3 |
| South | S05 | NammaMart JP Nagar | 21.2 | 29.6 |
| North | S03 | NammaMart RT Nagar | 20.7 | 29.4 |
| East | S11 | NammaMart HSR Layout | 20.8 | 28.6 |
| South | S04 | NammaMart Jayanagar | 21.3 | 27.5 |
| West | S13 | NammaMart Malleshwaram | 21.9 | 27.3 |
| East | S09 | NammaMart Indiranagar | 21.1 | 26.5 |
| East | S10 | NammaMart Marathahalli | 22.1 | 26.0 |
| West | S12 | NammaMart Rajajinagar | 21.8 | 24.4 |
| West | S15 | NammaMart Basaveshwaranagar | 22.0 | 22.9 |
| South | S06 | NammaMart Banashankari | 22.5 | 21.6 |


No store gets close to the "10-minute promise": the best store delivers about a third of orders within 15 minutes, and the average is about 21 minutes. That's a strong discussion point for Kavya's review.

**Q4. Revenue by payment mode, and the amount that doesn't reconcile with the gateway**

<details><summary>SQL</summary>

```sql
-- ETL
SELECT payment_mode, count(*) FILTER (WHERE revenue_eligible) AS orders, round(sum(net_revenue),2) AS net_revenue
FROM etl_fact_orders GROUP BY 1 ORDER BY net_revenue DESC;
-- ELT
SELECT payment_mode, count(*) FILTER (WHERE revenue_eligible) AS orders, round(sum(net_revenue),2) AS net_revenue
FROM mart_order_facts GROUP BY 1 ORDER BY net_revenue DESC;
```
</details>

| payment_mode | orders | net_revenue |
| --- | --- | --- |
| UPI | 1133 | ₹599,902.90 |
| Card | 401 | ₹221,657.20 |
| Cash on Delivery | 275 | ₹152,193.15 |
| Wallet | 214 | ₹122,552.95 |


What doesn't reconcile:

<details><summary>SQL</summary>

```sql
-- ETL
SELECT recon_status, owner, count(*) AS rows, round(sum(variance),2) AS net_variance, round(sum(abs(variance)),2) AS abs_variance
FROM etl_payment_reconciliation GROUP BY ALL ORDER BY abs_variance DESC;
-- ELT
SELECT recon_status, owner, count(*) AS rows, round(sum(variance),2) AS net_variance, round(sum(abs(variance)),2) AS abs_variance
FROM mart_payment_reconciliation WHERE recon_status NOT IN ('MATCHED','REFUND_MATCHED') GROUP BY ALL ORDER BY abs_variance DESC;
```
</details>

| recon_status | owner | rows | net_variance | abs_variance |
| --- | --- | --- | --- | --- |
| AWAITING_PAYMENT_FEED | - (timing difference: order newer than the payment export) | 150 | ₹-79,652.20 | ₹79,652.20 |
| PRICING_ERROR_AT_CHECKOUT | Tech (checkout) + Finance | 15 | ₹36,480.33 | ₹38,843.67 |
| DELIVERED_PAYMENT_FAILED | Collections | 42 | ₹-25,722.25 | ₹25,722.25 |
| DELIVERED_NO_PAYMENT | Finance + Tech (payment export) | 27 | ₹-16,664.35 | ₹16,664.35 |
| ORPHAN_PAYMENT | Payments team | 8 | ₹4,580.57 | ₹4,580.57 |
| RETURNED_NO_REFUND | Finance | 11 | ₹4,193.70 | ₹4,193.70 |
| PAYMENT_FOR_QUARANTINED_ORDER | Data team | 50 | ₹518.90 | ₹1,584.50 |
| DUPLICATE_PAYMENT | Payments team | 4 | ₹1,330.50 | ₹1,330.50 |
| PROMO_SHORTFALL | Marketing / promo ops | 14 | ₹-785.00 | ₹785.00 |
| NEGATIVE_ORDER | Ops (returns keyed as orders) | 10 | ₹0.00 | ₹0.00 |

**Total that doesn't reconcile: ₹1,73,356.74 gross (sum of |variance|) across 331 rows; net −₹75,719.80** (ETL). Excluding the ₹79,652.20 that is only a timing difference (1 October orders newer than the payment export), the real breaks are **₹93,704.54**, each with an owner. Three matter most:
- **₹42,386.60 delivered but not collected:** 42 failed payments and 27 with no payment.
- **₹36,480.33 net overcharged** by the checkout pricing bug, which is owed back to customers.
- **₹4,193.70 not yet refunded** on returns.

**One documented ETL ≠ ELT difference:** the ELT shows 11 `NEGATIVE_ORDER` rows (+₹720) against the ETL's 10. The extra row is ORD900002, the quantity −2 test order from experiment 5. ELT staging is persistent and keeps a row after the source stops sending it, while the ETL reconciles only the current file. It never reaches revenue (it's a FAIL row); Part F covers it.
**Q5. Top 10 customers by spend, with loyalty tier** (no raw email or phone)

<details><summary>SQL</summary>

```sql
-- ETL
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank, c.customer_id, c.customer_name, c.loyalty_tier, c.email_masked,
       count(*) FILTER (WHERE f.revenue_eligible) AS orders, round(sum(f.net_revenue),2) AS total_spend
FROM etl_fact_orders f JOIN etl_dim_customer c USING (customer_id)
GROUP BY c.customer_id, c.customer_name, c.loyalty_tier, c.email_masked QUALIFY spend_rank <= 10 ORDER BY spend_rank;
-- ELT
SELECT spend_rank, customer_id, customer_name, loyalty_tier, email_masked, delivered_orders AS orders, total_spend FROM mart_top_customers ORDER BY spend_rank;
```
</details>

| spend_rank | customer_id | customer_name | loyalty_tier | email_masked | orders | total_spend |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | C00136 | Sanjay B. | Bronze | sa***@example.com | 9 | ₹8,738.10 |
| 2 | C00236 | Sneha N. | Platinum | sn***@example.com | 12 | ₹8,132.40 |
| 3 | C00310 | Kiran P. | Bronze | NULL (invalid source email) | 8 | ₹7,258.95 |
| 4 | C00071 | Mahesh P. | Silver | ma***@example.com | 11 | ₹6,933.30 |
| 5 | C00209 | Kavya P. | Platinum | ka***@example.com | 8 | ₹6,519.40 |
| 6 | C00338 | Bhavana I. | Silver | bh***@example.com | 8 | ₹6,506.55 |
| 7 | C00115 | Sanjay K. | Bronze | sa***@example.com | 7 | ₹6,483.70 |
| 8 | C00072 | Nandini M. | Silver | na***@example.com | 11 | ₹6,433.95 |
| 9 | C00265 | Vikram H. | Gold | vi***@example.com | 7 | ₹6,330.50 |
| 10 | C00135 | Deepa D. | Bronze | de***@example.com | 9 | ₹6,322.10 |

Customer 3's source email is invalid (Warning `customers_email_valid`), so the masked value is NULL rather than a guess.
**Q6. Rows rejected today, by file and reason** (latest run of each pipeline)

<details><summary>SQL</summary>

```sql
-- ETL
SELECT source_file, failure_reason, count(*) AS rows FROM etl_quarantine_log
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_etl_karamvir' ORDER BY start_time DESC LIMIT 1)
GROUP BY ALL ORDER BY source_file, rows DESC;
-- ELT
SELECT source_file, failure_reason, count(*) AS rows FROM elt_quarantine
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id='nammamart_elt_karamvir' ORDER BY start_time DESC LIMIT 1)
GROUP BY ALL ORDER BY source_file, rows DESC;
```
</details>

| source_file | failure_reason | rows |
| --- | --- | --- |
| customers.csv | customers_customer_id_unique | 5 |
| customers.csv | customers_signup_not_future | 3 |
| orders.csv | orders_customer_id_not_null | 18 |
| orders.csv | orders_amount_formula | 15 |
| orders.csv | orders_order_id_unique | 12 |
| orders.csv | orders_quantity_positive | 10 |
| orders.csv | orders_product_fk | 9 |
| orders.csv | orders_order_ts_not_future | 7 |
| orders.csv | orders_order_ts_valid | 6 |
| orders.csv | orders_store_fk | 5 |
| orders.csv | orders_discount_range | 3 |
| orders.csv | orders_status_allowed | 3 |
| payments.csv | payments_amount_positive | 10 |
| payments.csv | payments_order_fk | 8 |
| payments.csv | payments_ts_not_future | 7 |
| payments.csv | payments_payment_id_unique | 4 |
| products.csv | products_cost_le_mrp | 4 |
| products.csv | products_category_present | 3 |
| products.csv | products_product_id_unique | 2 |
| products.csv | products_mrp_positive | 1 |
| stores.csv | stores_zone_valid | 1 |

**136 rows rejected today**: orders 88, payments 29, products 10, customers 8, stores 1. Identical in both pipelines. A row failing several Critical checks is listed once with all its reasons.

## Part C: Scheduling both DAGs for 9:00 AM IST

### Configuration (identical in both DAGs)

```python
IST = pendulum.timezone("Asia/Kolkata")

@dag(
    schedule="0 9 * * 1-6",                                   # 09:00, Monday-Saturday
    start_date=pendulum.datetime(2026, 10, 1, 9, 0, tz=IST),  # timezone-aware -> cron is read in IST
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
)
```

The next eight runs, generated by Airflow's own timetable for `nammamart_etl_karamvir` (Airflow 3.2, `CronTriggerTimetable`):

| Run (IST) | Shown in Airflow UI (UTC) |
| --- | --- |
| Thu 2026-10-01 09:00 | 03:30 |
| Fri 2026-10-02 09:00 | 03:30 |
| Sat 2026-10-03 09:00 | 03:30 |
| *(Sunday skipped)* | |
| Mon 2026-10-05 09:00 | 03:30 |
| Tue 2026-10-06 09:00 | 03:30 |
| Wed 2026-10-07 09:00 | 03:30 |

### 1. What is your cron expression?

**`0 9 * * 1-6`**: minute 0, hour 9, any day of month, any month, weekdays 1–6 (Monday–Saturday; 0 = Sunday).

Airflow reads the cron expression **in the DAG's timezone**, which it takes from `start_date`. Because `start_date` is `Asia/Kolkata`, "hour 9" means 09:00 IST. In Airflow 3, a cron schedule creates a `CronTriggerTimetable`, so the run starts **at** 09:00 and its `logical_date` is that same time. (In Airflow 2, the run for an interval started at the *end* of that interval, which made "the 9 AM run" confusing.)

### 2. What time does the run show in UTC in the Airflow UI?

**03:30 UTC.** IST is UTC+05:30 all year (India has no daylight saving), so 09:00 IST is always 03:30 UTC. The Airflow UI shows UTC by default, so the schedule column and "Next Run" show 03:30. Switching the UI's display timezone to Asia/Kolkata (in the UI's user/timezone settings) shows 09:00. The stored value is the same instant either way.

`airflow dags next-execution nammamart_etl_karamvir` returns `…T03:30:00+00:00`.

### 3. What would happen at 9 AM if you had left the `start_date` in UTC?

The DAG's timezone would be UTC, so `0 9 * * 1-6` would mean **09:00 UTC = 14:30 IST**. I checked this against Airflow's timetable: with a UTC timezone, the first run lands on Thu 14:30 IST.

- **Nothing happens at 9 AM IST.** The ops review at 9:30 starts on yesterday's numbers again, which is exactly the problem NammaMart hired us to fix.
- The run arrives **five hours after** the stand-up. Finance and ops would see the report at lunchtime.
- It fails **silently**: every run shows green, just at the wrong time, so no alert ever fires.

The alternative of writing `30 3 * * 1-6` in UTC gives the right time but hides the business intent ("9 AM IST") in arithmetic. It also breaks if the business ever moves to a timezone with daylight saving. Keeping the timezone explicit is the safer pattern.

### 4. What happens if your laptop or Docker is off at 9 AM?

**No run happens at 9:00.** Airflow is software running inside Docker on this laptop. With no scheduler running, nothing checks the clock and nothing triggers.

When Docker and Airflow come back up (say at 11:15), the timetable with `catchup=False` takes the **latest** scheduled time that has already passed (today 09:00). If that hasn't run yet, it **runs once, immediately**. Older missed days are skipped. I confirmed this in Airflow's `CronTriggerTimetable` code: with catchup off, the next run starts from the most recent schedule time before "now".

| Situation | What happens |
| --- | --- |
| Laptop off 08:30–11:15 on Tuesday | One run at ~11:15 for Tuesday 09:00. The report is late, not missing. |
| Laptop off Friday evening to Monday 10:00 | One run at ~10:00 for Monday 09:00. Saturday is **not** back-filled. |
| Docker on, Airflow container crashed | Same as off; `astro dev start` restores it. |
| DAG paused at 09:00 | No run; unpausing later runs the latest missed slot once. |

**Why this is acceptable here:** each run re-reads the full current extract, so the late run produces exactly the report the 9:00 run would have produced. Nothing is lost, only delayed.

**What it means for NammaMart:** a laptop cannot meet a daily 9 AM SLA. In production the scheduler must run somewhere always on, such as a small cloud VM or managed Airflow (Astro, AWS MWAA, GCP Composer). There should also be an alert if `pipeline_runs` has no SUCCESS row for today by 09:15.

### `catchup=False`: justification

- **The source files are full snapshots, not daily slices.** `orders.csv` holds all of September every time, so replaying ten missed days would load the same file ten times. Thanks to the idempotent upsert / MERGE that wouldn't corrupt anything, but it would waste time and fill `pipeline_runs` and `dq_results` with ten identical rows.
- **The 9 AM report needs today's state, not a replay of history.** If Airflow was down all weekend, ops on Monday wants Monday's numbers immediately, not three queued runs ahead of them.
- **Back-fills stay possible on purpose.** If a day ever needs re-running, `airflow dags backfill` or **Clear** in the UI does it deliberately.

### Retries, retry delay and execution timeout: justification

| Setting | Value | Why |
| --- | --- | --- |
| `retries` | 2 | The realistic transient failures are: the export job still writing a file at 09:00; DuckDB's single-writer lock held by the other DAG (both run at 09:00 on one warehouse); a container restart. Two retries absorb these. A genuinely missing file or schema change fails all three attempts and goes red, as experiments 4 and 6 show. |
| `retry_delay` | 1 minute | Long enough for a file copy or lock to clear, and short enough to finish before 09:30. A normal run takes under a minute, so the worst case of two retries on one task is still ready by about 09:05. |
| `execution_timeout` | 10 minutes per task | A healthy task finishes in seconds on 5,000 rows. A task still running after 10 minutes is stuck (hung lock, runaway query), and killing it turns a silent hang into a visible failure. |
| `max_active_runs` | 1 | Two runs of the same DAG must never write the warehouse at the same time (DuckDB allows one writer; overlapping upserts would race). |
| `run_audit` | `trigger_rule="all_done"`, `retries=0` | It must always write the `pipeline_runs` row, including on failure. Retrying it would only repeat a known outcome. |

**Known limit to flag to the client:** in the worst case, a hung task runs 3 attempts × 10 minutes plus 2 × 1-minute delays, about 32 minutes. A run that hits that would finish *after* the 9:30 stand-up. With a 9:00 schedule there is no slack for that case. My recommendation is to run at **08:00** with an alert at 09:00 if the day's run hasn't succeeded. That leaves an hour for a person to fix and **Clear** the failed task before Kavya's review. I kept 09:00 here because it is the brief's acceptance criterion.

### Screenshots for criterion 1

**Taken:** `assignment/screenshots/exp_DAG_run_successful.PNG` shows both DAGs with schedule `0 9 * * 1-6`, a green latest run, and **Next Run `2026-10-17 09:00:00`** with the UI timezone set to **+05:30 (IST)**.

Also useful:

1. Airflow UI → **Dags** → both DAGs listed with schedule `0 9 * * 1-6` and **Next Run** at 03:30 UTC.
2. The same view after switching the UI's display timezone to **Asia/Kolkata**: Next Run shows 09:00.
3. A DAG's **Details** tab showing `timezone: Asia/Kolkata`, `catchup: False`, `max_active_runs: 1`.

---

## Part D: Data quality dimensions

### How the checks are built

- **One catalogue, two implementations.** Every check is defined once in `include/dq_rules.py` (`CHECKS`): name, file, dimension, severity, rule and root cause.
  - The **ETL** runs those Python functions on pandas DataFrames.
  - The **ELT** implements the same check names in SQL inside `include/sql/1x_stg_<file>.sql`, using the helpers in `00_macros.sql`. At the start of each run the catalogue is seeded into `ref_dq_checks`, so `dq_results` uses the same names, dimensions and severities in both pipelines.
- **The two implementations are proven to agree.** On the same data, all 36 row-level checks report **identical `rows_failed` in ETL and ELT** (table below). This matters because the obvious risk of writing every rule twice is that the Python and SQL quietly drift apart. The ELT's `parity_check` task re-proves the outcome on every run, order by order.
- **Every check writes a row on every run**, including checks that pass, to `dq_results` (`run_id, dag_id, file, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at`). A check with zero failures is evidence too; leaving it out would make the scorecard look emptier than it is.

### Severity: the decision rule

| Severity | Rule I applied | Effect |
| --- | --- | --- |
| **Critical** | The row would make a **finance number wrong**, or **cannot be joined** to its masters (bad key, duplicate, impossible amount, unparseable or future date). | ETL: quarantined to `include/quarantine/karamvir/` with `failure_reason`. ELT: `dq_status = 'FAIL'`, excluded from marts, listed in `elt_quarantine`. |
| **Warning** | The row is **still right for revenue**, but a secondary attribute is unreliable (contact details, casing, delivery time). | Loads; the check name goes into `dq_warnings`/`dq_status = 'WARN'` and is logged in `dq_results`. |
| **Critical on a master row, kept as a flagged member** (decision Q1) | A product, store or customer row fails on a single field, but its orders are real sales. | The row is quarantined **and** kept in the dimension with `dq_status = 'QUARANTINED'` and the bad field neutralised: S07 zone becomes `Unknown`, and a bad cost becomes NULL so it is excluded from margin. This kept ₹1.6L of real revenue that strict quarantine would have dropped. |

**Where I deviated from the brief's suggested severity, and why:**

| Check | Brief suggests | Mine | Reason (decision D3) |
| --- | --- | --- | --- |
| `orders_delivery_minutes_present` (DELIVERED with no time) | Critical | Warning | The order was delivered and paid, so revenue is correct; only the delivery time is missing. The row counts for revenue but `sla_eligible = false` keeps it out of the "within 15 min" KPI. Quarantining would remove 8 real orders from finance to protect an ops metric. |
| `orders_delivery_minutes_plausible` (> 120 min) | Critical | Warning | Same reasoning. The values are 480, 900 and 1440 (8 h, 15 h, 24 h): a units or default-value problem in the delivery-time field, not a fake order. |
| SUCCESS payment ≠ order amount | Critical (consistency) | Not a quality failure: goes to **reconciliation** | The 14 shortfalls are exactly ₹10/25/50/100 (promo or cashback at the gateway), and the 15 pricing errors were really charged to customers. These are money questions with an owner (`mart_payment_reconciliation.owner`), not bad data. Treating them as quality failures would hide them in a quarantine file. |

### Checks by dimension (latest run, September files)

Python: `include/dq_rules.py`. SQL: `include/sql/10–14_stg_*.sql`, same check name. "Failed" = `rows_failed` in `dq_results`, identical for ETL and ELT.

| Dimension | Check | File | Severity | Rule | Checked | Failed |
| --- | --- | --- | --- | --- | ---: | ---: |
| **Completeness** | `orders_customer_id_not_null` | orders | Critical | customer_id NOT NULL | 2,412 | 18 |
| | `orders_delivery_minutes_present` | orders | Warning | delivery_minutes NOT NULL when DELIVERED | 2,412 | 8 |
| | `products_category_present` | products | Critical | category NOT NULL | 152 | 3 |
| | `stores_zone_valid` | stores | Critical | zone IN (North, South, East, West) | 15 | 1 |
| | `customers_city_present` | customers | Warning | city NOT NULL | 405 | 6 |
| | `payments_gateway_ref_present` | payments | Warning | gateway_ref NOT NULL unless COD | 2,228 | 0 |
| | `orders_volume_drop` *(dataset-level)* | orders | Warning | rows ≥ 10% of previous run | 2,412 | 0 |
| **Uniqueness** | `orders_order_id_unique` | orders | Critical | order_id unique, keep first | 2,412 | 12 |
| | `payments_payment_id_unique` | payments | Critical | payment_id unique, keep first | 2,228 | 4 |
| | `customers_customer_id_unique` | customers | Critical | one row per customer_id | 405 | 5 |
| | `products_product_id_unique` | products | Critical | one row per product_id; drop "(old)" rows | 152 | 2 |
| **Validity** | `orders_order_ts_valid` | orders | Critical | parses as `YYYY-MM-DD HH:MM:SS` | 2,412 | 6 |
| | `orders_status_allowed` | orders | Critical | IN (DELIVERED, CANCELLED, RETURNED) | 2,412 | 3 |
| | `orders_payment_mode_allowed` | orders | Critical | IN (UPI, Card, Cash on Delivery, Wallet) after mapping | 2,412 | 0 |
| | `orders_discount_range` | orders | Critical | 0 ≤ discount_pct ≤ 100 | 2,412 | 3 |
| | `customers_email_valid` | customers | Warning | `name@domain.tld` | 405 | 12 |
| | `customers_phone_valid` | customers | Warning | `^[6-9][0-9]{9}$` | 405 | 7 |
| | `customers_pincode_valid` | customers | Warning | `^[0-9]{6}$` | 405 | 3 |
| **Accuracy** | `orders_quantity_positive` | orders | Critical | quantity > 0 | 2,412 | 10 |
| | `orders_amount_formula` | orders | Critical | \|qty × price × (1 − disc/100) − order_amount\| ≤ 0.01 | 2,412 | 15 |
| | `orders_delivery_minutes_plausible` | orders | Warning | delivery_minutes ≤ 120 | 2,412 | 6 |
| | `payments_amount_positive` | payments | Critical | amount_paid > 0 | 2,228 | 10 |
| | `products_mrp_positive` | products | Critical | mrp > 0 | 152 | 1 |
| | `products_cost_le_mrp` | products | Critical | cost_price ≤ mrp | 152 | 4 |
| | `stores_capacity_positive` | stores | Warning | capacity_orders_per_day > 0 | 15 | 1 |
| **Consistency** | `orders_customer_fk` | orders | Critical | customer_id exists in customers | 2,412 | 0 |
| | `orders_product_fk` | orders | Critical | product_id exists in products | 2,412 | 9 |
| | `orders_store_fk` | orders | Critical | store_id exists in stores | 2,412 | 5 |
| | `payments_order_fk` | payments | Critical | order_id exists in orders | 2,228 | 8 |
| | `orders_status_casing` | orders | Warning | UPPER(TRIM(status)) | 2,412 | 2 |
| | `orders_payment_mode_casing` | orders | Warning | upi / `UPI ` / cod / CARD / Cashh mapped | 2,412 | 10 |
| | `payments_mode_casing` | payments | Warning | same mapping | 2,228 | 10 |
| | `payments_mode_matches_order` | payments | Warning | payment mode = order's payment mode | 2,228 | 0 |
| | `customers_tier_casing` | customers | Warning | Bronze / Silver / Gold / Platinum | 405 | 8 |
| **Timeliness** | `orders_order_ts_not_future` | orders | Critical | order_ts ≤ end of the period the file covers* | 2,412 | 7 |
| | `payments_ts_not_future` | payments | Critical | payment_ts ≤ end of the period the file covers* | 2,228 | 7 |
| | `customers_signup_not_future` | customers | Critical | signup_date ≤ end of the period the file covers* | 405 | 3 |
| | `orders_freshness` *(dataset-level)* | orders | Warning | latest valid order_ts ≥ run date − 1 day | 2,412 | **WARN** |

\* **"Future" means later than the period the file covers, not later than today.** The September export covers 1–30 September (brief, section 3), and next-day files carry their date in the name (`orders_2026-10-01_increment.csv`). The first version compared against the wall clock. Testing showed the placeholder orders dated `2026-10-21` would stop being "future" on 20 October and silently enter revenue, so the verdict depended on *when* you ran the DAG. With the file's business date (`dq_rules.business_date`, SQL macro `biz_date`), runs on 16 October, 25 October and 5 November give identical results. Staleness (`orders_freshness`) is the one check that should use the wall clock, and still does.

**Coverage:** 38 checks; every dimension has at least 3 checks and covers at least 2 files. Every file has at least 2 dimensions. The increment file runs the same 16 order checks and passes them all (160/160).

**Checks that found nothing, and why they stay:**

- **`orders_customer_fk` (0):** every non-blank customer_id exists. The 18 problem rows are *blank* IDs, caught by the completeness check.
- **`orders_payment_mode_allowed` (0):** every variant, including the typo `Cashh`, maps to a valid mode. I treated `Cashh` as a **typo of Cash**, so it's a Consistency warning and not a rejection. That is a judgement call to confirm with finance.
- **`payments_mode_matches_order` (0)** and **`payments_gateway_ref_present` (0):** clean today. All 301 blank gateway refs are Cash on Delivery, which is expected. These checks guard against a gateway integration failing in future.
- **`orders_volume_drop` (0):** only fires when compared with a previous run, as experiment 7 will show.

**Timeliness warning fires every run, correctly:** the extract ends on 30 September and runs happen in October, so `orders_freshness` says WARN ("latest order is 16 days old"). In production a stale feed is the first sign that the export job broke.

### Scorecard queries (for the `dq_results` screenshot)

```sql
-- Today's scorecard, both pipelines side by side
SELECT dimension, check_name, severity,
       max(rows_failed) FILTER (WHERE dag_id = 'nammamart_etl_karamvir') AS etl_failed,
       max(rows_failed) FILTER (WHERE dag_id = 'nammamart_elt_karamvir') AS elt_failed,
       max(status)                                                        AS status
FROM dq_results
WHERE checked_at::DATE = current_date AND file NOT LIKE '%increment%'
GROUP BY ALL
ORDER BY dimension, severity, check_name;

-- Quality trend over time (one row per run): the scorecard the brief asks to build up
SELECT dag_id, run_id, min(checked_at) AS run_time,
       count(*) FILTER (WHERE status = 'FAIL') AS critical_checks_failing,
       count(*) FILTER (WHERE status = 'WARN') AS warning_checks,
       sum(rows_failed) FILTER (WHERE severity = 'Critical') AS critical_rows
FROM dq_results
GROUP BY ALL
ORDER BY run_time;
```

Latest run, per pipeline: **37 check results** (36 row-level + freshness): **4 PASS, 12 WARN, 21 FAIL**. Identical in ETL and ELT.

## Part E: Data governance

All examples below come from the pipelines as built, with figures from an actual run.

### 1. Ownership

Each source belongs to the business team that **creates the data and can fix it at source**. The data team (today the FDE, later a NammaMart data lead) is **steward** of everything in the warehouse: it runs the pipelines but does not decide what "correct" means.

| Dataset | Owner (fixes it, answers for it) | Why |
| --- | --- | --- |
| `orders.csv` | Ops / order-management product owner | Bad timestamps, returns keyed as orders and checkout pricing bugs all start in the OMS |
| `payments.csv` | Finance (payments team) | Gateway reconciliation and refunds are finance processes |
| `customers.csv` | CRM / Marketing, with the DPO overseeing personal data | Holds the personal data; signup and contact-detail validation happen there |
| `products.csv` | Category / catalogue team | MRP, cost and category are commercial decisions (e.g. P0109's negative MRP, P0019's cost > MRP) |
| `stores.csv` | Store operations | Zone and capacity (S07 has no zone; S12 has capacity 0) |
| `etl_*`, `stg_*`, `mart_*`, `dq_results`, `pipeline_runs` | Data team (steward) | Builds and runs them; each reconciliation status carries a named `owner` for follow-up |

**Who approves a quality rule change?** The **owner of the metric the rule protects**, never the data team alone. The 120-minute delivery limit protects the delivery-SLA KPI, so **Kavya (COO)** approves it. Revenue rules (D1, D2, what counts as revenue) need **Finance / the CFO**. The data team proposes the change as a pull request to `include/dq_rules.py` plus the matching SQL. The ELT's parity check then proves both pipelines still agree, and the merged PR is the audit trail. The decision log above is the first entries of exactly that record.

### 2. Data dictionary: `etl_fact_orders` and its dimensions

The ELT twins (`mart_order_facts`, `stg_customers`, `stg_products`, `stg_stores`) have the same columns and meanings.

**`etl_fact_orders`**: one row per order that passed every Critical check.

| Column | Type | Meaning | Source | Allowed values | PII |
| --- | --- | --- | --- | --- | --- |
| order_id | VARCHAR | Order key | orders.order_id | `ORD` + 6 digits, unique | No |
| order_ts / order_date | TIMESTAMP / DATE | When the order was placed (IST) | orders.order_ts | valid; not after the period the file covers | No |
| customer_id | VARCHAR | Who ordered (FK → etl_dim_customer) | orders.customer_id | exists in customers | **Yes**, a pseudonymous identifier |
| store_id | VARCHAR | Fulfilling dark store (FK) | orders.store_id | S01–S15 | No |
| product_id | VARCHAR | Item (FK) | orders.product_id | exists in products | No |
| quantity | BIGINT | Units | orders.quantity | > 0 | No |
| unit_price / discount_pct | DOUBLE / BIGINT | Price per unit; discount % | orders | as received; discount 0–100 | No |
| order_amount | DOUBLE | Amount billed = qty × price × (1 − disc/100) | orders.order_amount | matches formula ±0.01 | No |
| payment_mode | VARCHAR | Standardised mode | orders.payment_mode | UPI, Card, Cash on Delivery, Wallet | No |
| order_status | VARCHAR | Latest status (increment wins) | orders / increment | DELIVERED, CANCELLED, RETURNED | No |
| channel | VARCHAR | Ordering channel | orders.channel | App, Web | No |
| delivery_minutes | DOUBLE | Minutes to deliver | orders.delivery_minutes | NULL allowed (D3) | No |
| recon_status | VARCHAR | Result of matching against payments | derived: reconciliation | MATCHED, PROMO_SHORTFALL, DELIVERED_PAYMENT_FAILED, … | No |
| revenue_eligible | BOOLEAN | DELIVERED and cash confirmed (D2) | derived | true/false | No |
| net_revenue | DOUBLE | Revenue counted for finance | derived: order_amount if eligible else 0 | ≥ 0 | No |
| cost_amount / gross_margin | DOUBLE | qty × cost_price; net_revenue − cost | derived + products.cost_price | NULL if cost untrusted (Q1) | No |
| sla_eligible / within_15_min | BOOLEAN | Counts toward SLA (D3); delivered ≤ 15 min | derived | true/false/NULL | No |
| dq_warnings | VARCHAR | Warning-level checks the row failed | validation | check names | No |
| _source_file / _run_id / _loaded_at | VARCHAR / VARCHAR / TIMESTAMP | Lineage: which file, which Airflow run, when | pipeline | | No |

**Dimensions** (all carry `dq_status` = OK / WARNING / QUARANTINED and `dq_notes` = check names):

| Table.column | Type | Meaning / allowed | Source | PII |
| --- | --- | --- | --- | --- |
| etl_dim_customer.customer_id | VARCHAR | Key, unique | customers | **Yes** (pseudonymous) |
| .customer_name | VARCHAR | "Priya H.", first name + last initial (Q4) | customers.full_name | **Yes**, masked |
| .email_masked / .phone_masked | VARCHAR | `pr***@example.com`, `97******21`; NULL if source invalid | customers.email / phone | **Yes**, masked |
| .city / .area / .pincode | VARCHAR | Location; invalid pincode → NULL | customers | **Yes** (quasi-identifier) |
| .signup_date, .loyalty_tier, .is_active | DATE, VARCHAR, BOOLEAN | Bronze/Silver/Gold/Platinum; future date → NULL (Q1) | customers | **Yes** (about an identifiable person) |
| etl_dim_product.product_id … launch_date | VARCHAR / DOUBLE / DATE | Catalogue; category `Unknown` or cost NULL when flagged | products | No |
| etl_dim_store.store_id … capacity_orders_per_day | VARCHAR / DATE / BIGINT | Store master; zone `Unknown` when flagged (S07) | stores | `manager_name`: **Yes** (employee) |

### 3. Lineage: tracing one number on `mart_daily_store_kpis`

**Number traced:** S07 · NammaMart BTM Layout · 2026-09-15 · `net_revenue = ₹2,695.00` (zone shows `Unknown`: S07 is a flagged member, Q1).

```sql
SELECT f.order_id, f.recon_status, f.net_revenue, r._source_file, r._run_id, r._row_num, r.order_amount
FROM mart_order_facts f
JOIN stg_orders s USING (order_id)
JOIN raw_orders r ON r.order_id = f.order_id AND r._run_id = s._run_id AND r._source_file = s._source_file
WHERE f.store_id = 'S07' AND f.order_date = DATE '2026-09-15';
```

| order_id | recon_status | net_revenue | source row (`orders.csv`, raw `_row_num`) |
| --- | --- | ---: | --- |
| ORD001093 | MATCHED | 185.30 | 1296 |
| ORD001094 | MATCHED | 1,491.00 | 675 |
| ORD001100 | MATCHED | 219.60 | 844 |
| ORD001107 | MATCHED | 288.80 | 1259 |
| ORD001156 | MATCHED | 510.30 | 1836 |
| ORD001131 | DELIVERED_PAYMENT_FAILED | 0.00 (₹336.60 held, D2) | 1349 |
| ORD001167 | REFUND_MATCHED (returned) | 0.00 | 567 |
| **Total** | | **2,695.00** | |

- **ELT: yes, in one query.** Mart → `mart_order_facts` → `stg_orders` (`_run_id`, `_source_file`) → `raw_orders`, where `_row_num` is the exact line of the file as received in that run, still stored even after later files arrive.
- **ETL: partly.** `etl_fact_orders` gives the same 7 order_ids, and `_source_file`/`_run_id` say which file and run they came from. But the ETL **keeps no copy of the source**, so the last hop means reopening `orders.csv` and hoping it hasn't been overwritten by tomorrow's export. Any order that was later corrected has also lost its old version.
- **ELT is easier** because "load everything raw, transform later" turns lineage into a join instead of an investigation.

### 4. Personal data

| File | Personal data columns | Treatment in reporting tables |
| --- | --- | --- |
| customers | `full_name`, `email`, `phone` (direct identifiers) | Name → "Priya H." (Q4); email → `pr***@example.com`; phone → `97******21` (Q3). Invalid values → NULL. Masked in quarantine files too. |
| customers | `area`, `pincode`, `city`, `signup_date`, `loyalty_tier`, `is_active` | Kept: needed for analysis; personal data because they describe an identifiable person. Access-controlled, not masked. |
| customers / orders | `customer_id`, and the order history behind it | Kept as a pseudonymous key; still personal data, so marts need access control (masking ≠ anonymisation) |
| payments | `gateway_ref` | A transaction reference linkable to a person; not used in marts |
| stores | `manager_name` | Employee personal data; kept for ops, restricted |

**Why mask rather than delete:**

- The data is still needed. Ops recognise "Priya H." in the top-customer list, the format checks (valid email, 10-digit phone) still run, and customer-level analysis (spend, tier) works.
- Deleting the columns would break deduplication and the bonus SCD2 history, and would leave nothing to show an auditor about why a row was rejected. Masking keeps the *usefulness* without the *exposure*.
- The full values stay only in the source files and `raw_customers`, which is why access to raw is the strictest (answer 5).

**Law: the Digital Personal Data Protection Act, 2023 (DPDP Act), with the DPDP Rules notified on 13 November 2025.** NammaMart is a **Data Fiduciary**. The substantive obligations apply from **13 May 2027**, seven months from now:

- notice and consent, and use only for the stated purpose;
- **"reasonable security safeguards"**, which the Rules list explicitly: encryption, obfuscation, **masking**, access controls, monitoring and logging;
- notifying the Data Protection Board of a breach without delay, with a detailed report within **72 hours**, and informing affected customers;
- **erasing** data once its purpose is served, while keeping logs for at least **one year**;
- honouring customers' rights to access, correction and erasure.

Masking, access control by layer and the run audit are this pipeline's down-payment on those safeguards.

### 5. Access control

| Layer | Who reads it | Why |
| --- | --- | --- |
| `raw_*` | Data engineers running the pipeline; DPO / compliance for audits | Exact copy incl. **unmasked** names, emails, phones; nobody analyses here |
| `stg_*` | Data team | Typed and masked, but holds failed rows and flags; it's a working layer |
| `mart_*`, `etl_*` | Ops (Kavya, store managers, ideally filtered to their stores), Finance (CFO, reconciliation), analysts | Business-ready, masked |
| Quarantine (`include/quarantine/karamvir/`, `etl_quarantine_log`, `elt_quarantine`) | Data team, plus each **dataset owner** for their own file | So owners can fix rows at source; customer rows are already masked (Q3) |
| `dq_results`, `pipeline_runs` | Everyone, including Sameer | Health is meant to be visible |

**Who should see the unmasked phone numbers in `raw_customers`?** **No one, as a matter of routine.** Customer support already sees phone numbers in the CRM, which is the system that owns them. The warehouse copy exists only for audit replay. Access should be break-glass only (DPO, logged, for an erasure request or an investigation).

**Honest gap:** DuckDB is a single file with **no user-level permissions**. Anyone who can open `warehouse_karamvir.duckdb` can read `raw_customers`. That's why the file is git-ignored. In production, raw would sit in a separate restricted database or schema (or Postgres / BigQuery with role-based grants), and analysts would get read-only access to marts only.

### 6. Retention

| Data | Keep for | Then | Reason |
| --- | --- | --- | --- |
| Raw orders and payments | **8 years** | Archive to cold storage, then delete | They're books of account (Companies Act s.128: 8 financial years; GST: 72 months from the annual return). They hold `customer_id` but no contact details. |
| Raw customers (unmasked) | Latest **30 daily snapshots** | Purge older snapshots (scheduled job), logging the deletion | The current ELT appends a full PII copy **every run**, so it is the fastest-growing personal data in the warehouse. 30 days covers replay and audit of recent runs. Erasure requests delete the customer from every snapshot. |
| Masked customer dimension / marts | While the customer is active, then **3 years after last activity** | Anonymise: break the `customer_id` link and keep only aggregates | DPDP: erase when purpose is served. The Rules set 3 years of inactivity for large e-commerce platforms, a sensible benchmark even below that size. |
| Quarantine files and tables | **90 days after the issue is fixed** | Delete | Long enough for month-end close and a finance query; customer rows already masked |
| `dq_results`, `pipeline_runs`, `elt_merge_log` | **2 years** | Aggregate to monthly trend, delete detail | DPDP asks for at least 1 year of logs; 2 years gives a year-on-year quality trend |
| `include/staging/` run files | **7 days** | Delete | Scratch space for task hand-offs only |

The purges would themselves be a scheduled Airflow DAG, so retention is enforced by code rather than by memory.

### 7. Secrets

- **Neither of my DAGs needs a secret.** Files and DuckDB are local, so there's nothing to leak.
- **Where the class setup keeps them:** credentials such as `SLACK_WEBHOOK_URL` and the Azure OpenAI key are **Airflow Variables** (Admin → Variables). They live in Airflow's metadata database inside Docker and are read at runtime with `Variable.get("SLACK_WEBHOOK_URL")`. For local development they could also come from `airflow_settings.yaml` or `.env`; both are in `.gitignore`. The bonus Slack alert would read the webhook the same way.
- **Why never in a DAG file or on GitHub:**
  - DAG files are code. Anyone with repo access can read them, and the Airflow UI shows DAG source to its users.
  - Git history keeps a secret **forever**, even after it's deleted from the file.
  - Public repos are scanned for keys within minutes.
  - A leaked webhook lets anyone post fake "pipeline OK" or phishing messages into the ops channel, and a leaked AI key runs up cost.
  - Rotating a secret stored in a Variable is one edit; rotating one in code is a commit, a deploy and a history rewrite.

### 8. Audit: "Show me a rejected order, the rule it broke, and prove it isn't in the CFO's total"

**Order picked: `ORD002305`.**

- **As received** (`raw_orders`, `orders.csv`, row 311): `2026-09-29 21:58:50, C00033, S14, P0141, qty 3, unit_price 265.0, discount 15, order_amount 6757.5, Cash on Delivery, DELIVERED, App, 29`.
- **Rule broken:** `orders_amount_formula` (Accuracy, Critical). 3 × 265 × (1 − 0.15) = **₹675.75**, but the order says ₹6,757.50: a 10× checkout pricing error (D1).
- **Not in the CFO's total:** it is absent from both `etl_fact_orders` and `mart_order_facts`. Finance's September net revenue of **₹11,00,499.90** is `SUM(net_revenue)` over those tables, so ORD002305 contributes ₹0.
- **What finance actually needs to know:** reconciliation shows the customer **paid ₹6,757.50**. Status `PRICING_ERROR_AT_CHECKOUT`, owner "Tech (checkout) + Finance", variance **+₹6,081.75 overcharged**: a refund the customer is owed.

| | How the ETL answers | How the ELT answers |
| --- | --- | --- |
| As received | Row in `include/quarantine/karamvir/orders_failed_<run_id>.csv`: all original columns plus `failure_reason` | `SELECT * FROM raw_orders WHERE order_id = 'ORD002305'`: exact text, with file, row number and run |
| Rule broken | Same file's `failure_reason`; `etl_quarantine_log` | `elt_quarantine.failure_reason`; `dq_results` for the run |
| Not in revenue | `SELECT count(*) FROM etl_fact_orders WHERE order_id = 'ORD002305'` → 0 | Same query on `mart_order_facts` → 0 |
| Money impact | `etl_payment_reconciliation` | `mart_payment_reconciliation` |

Both can answer. The ELT does it with SQL alone, while the ETL depends on a CSV per run.

### 9. Change history: delivered on 4 September, returned on 1 October

The brief's 3-September example in our data is **ORD000271**: DELIVERED on 2026-09-04 for ₹445.00, then RETURNED in `orders_2026-10-01_increment.csv`.

| | Before the increment | After the increment |
| --- | --- | --- |
| September net revenue (ETL and ELT, identical) | **₹11,00,499.90** | **₹10,96,306.20** |
| ORD000271 | DELIVERED, ₹445.00 | RETURNED, ₹0 |
| The 10 corrected orders together | | −₹4,193.70; status `RETURNED_NO_REFUND` (paid, refund not yet in the payment feed) |

- **What each pipeline shows:** both show September **restated**. The upsert/MERGE overwrites the status, so September loses ₹4,193.70 after the fact.
  - The **ETL** has also lost the old version: the fact row was replaced.
  - The **ELT** still has both versions in `raw_orders` (DELIVERED from `orders.csv` in run 1, RETURNED from the increment in run 2), so the original view can be rebuilt from raw for any run.
- **What finance wants:** the **booked view**. September's books close on the number reported at close (₹11,00,499.90), and the return is booked in **October**, the month it happened, as −₹4,193.70 (a credit note). Ops and analytics want the **restated view** to see true product and store performance. Both are legitimate and answer different questions.
- **How to keep both:**
  1. An order-status history table: SCD Type 2 on status with `valid_from`/`valid_to`, built in the ELT from the versions already in `raw_orders`.
  2. A monthly **close snapshot** (`fin_revenue_close_2026_09`), frozen at month end.
  3. A **returns-adjustment** line dated by the return, not the original order.

  Finance then reports "close + adjustments", while ops reports "restated".

## Part F: Observability tests

All ten experiments were run **on both DAGs in Airflow (Astro Runtime 3.3, Docker Desktop)** on 16 October 2026. Each was set up with `python -m include.experiments_karamvir setup <n>` (which works on a copy of the export and never hand-edits the data) and observed with `python -m include.observe_karamvir`, the Airflow Grid / Logs views, and the tables below. Step-by-step procedure: `assignment/PART_F_RUNBOOK.md`.

### Run audit: `pipeline_runs`

The last task of each DAG, `run_audit` (`trigger_rule="all_done"`, no retries), writes one row per run even when upstream tasks fail. The row holds `run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status`. The task then fails itself if the run failed, so the DAG run is red too. All 22 runs from the session:

| # | Experiment | ETL: extracted / loaded / quarantined | ETL status (secs) | ELT: extracted / loaded / quarantined | ELT status (secs) |
| --- | --- | --- | --- | --- | --- |
| 1 | Happy path | 5,212 / 2,324 / 136 | SUCCESS (21–22 s) | 5,212 / 2,324 / 136 | SUCCESS (37–54 s) |
| 2 | Re-run | 5,212 / 2,324 / 136 | SUCCESS | 5,212 / 2,324 / 136 | SUCCESS |
| 3 | Increment | 5,372 / 2,474 / 136 | SUCCESS (11 s) | 5,372 / 2,474 / 136 | SUCCESS (44 s) |
| 4 → 10 | Missing file → recovery | 5,060 / 0 / 0 → 5,212 / 2,314 / 136 | **FAILED** (129 s) → SUCCESS | 5,060 / 0 / 0 → 5,212 / 2,314 / 136 | **FAILED** (131 s) → SUCCESS |
| 5 | New bad data | 5,217 / 2,314 / 141 | SUCCESS | 5,217 / 2,314 / 141 | SUCCESS |
| 6 | Schema change | 2,800 / 0 / 0 | **FAILED** (134 s) | 2,800 / 0 / 0 | **FAILED** (139 s) |
| 7 | 50 orders | 2,850 / 48 / **2,201** | SUCCESS | 2,850 / 48 / **2,201** | SUCCESS |
| 8a | Price ×100 (S05) | 5,212 / 2,175 / 276 | SUCCESS | 5,212 / 2,175 / 276 | SUCCESS |
| 8b | Price and amount ×100 (S05) | 5,212 / 2,315 / 136 | SUCCESS | 5,212 / 2,314 / 136 | SUCCESS |
| 9 | 30-day-old orders | 5,212 / 2,317 / 134 | SUCCESS | 5,212 / 2,316 / 134 | SUCCESS |
| — | Final clean run (restore + increment) | 5,372 / 2,474 / 136 | SUCCESS | 5,372 / 2,474 / 136 | SUCCESS, parity **MATCH** |

### Results: Expected → Observed → Where I saw it → What I would change

| # | Expected | Observed (ETL / ELT) | Where I saw it | What I would change |
| --- | --- | --- | --- | --- |
| **1 Happy path** | Both green; 2,324 orders; 136 quarantined; ETL = ELT | As expected in both. **ETL:** 6 `extract_*` tasks ran in parallel, then `validate`; `quarantine` and `transform` ran in parallel. **ELT:** 6 `load_raw_*` + `seed_reference_data` in parallel, then the 5 `stg_*` in parallel (serialised briefly by the DuckDB write lock). ETL about 21 s, ELT 37–54 s. Quarantine: orders 88, payments 29, products 10, customers 8, stores 1. Parity: ₹11,00,499.90 in both. | Graph and Gantt views; `pipeline_runs`; `parity_check` log; `include/quarantine/karamvir/*_failed_<run_id>.csv` | ELT tasks spend time waiting for the single DuckDB writer. At scale, use a warehouse with concurrent writes, or an Airflow pool of 1 instead of lock-retry. |
| **2 Re-run** | Fact unchanged; raw grows in ELT | ETL fact stays **2,324** (`inserted 0, updated 2,324`). ELT merge `inserted 0, updated 2,400`; `raw_orders` **2,412 → 4,824** (2 runs). Revenue unchanged. **Correct:** raw is the append-only record of every file received; marts are de-duplicated. | `load_star_schema` log; `elt_merge_log`; `observe counts` | Raw growth includes a fresh **unmasked** copy of `customers.csv` every run: needs the retention purge from Part E answer 6. |
| **3 Incremental load** | 150 inserted, 10 updated to RETURNED, no duplicates | ETL `inserted 150, updated 2,324` → **2,474** rows. ELT merge **150 inserted, 2,400 updated, 10 status changes**, 0 duplicates. ORD000271 DELIVERED → RETURNED; `raw_orders` keeps both versions. September revenue **₹11,00,499.90 → ₹10,96,306.20** in both. 11 `RETURNED_NO_REFUND` (₹4,193.70 to refund), 150 `AWAITING_PAYMENT_FEED`. | `elt_merge_log`; `stg_orders` task log; `observe order ORD000271`; parity | Add a status-history table (Part E answer 9) so the September "as booked" figure survives the restatement. |
| **4 Missing file** | Retries, then failed; downstream upstream_failed | `extract_products` / `load_raw_products`: up_for_retry ×2, 1 minute apart, then **failed**: `FileNotFoundError: Source file not found: …/products.csv`. Downstream **upstream_failed**; `run_audit` wrote **FAILED** (5,060 extracted, 0 loaded). **Reports untouched:** the last good numbers stayed live. In the ELT, the other 4 raw files were already appended for this run. **What alerted me: nothing.** I only saw it because I was looking at the UI. | Grid view colours; task Logs (attempt 1/2/3); `pipeline_runs` | **Add an alert:** `on_failure_callback` posting to Slack via `Variable.get("SLACK_WEBHOOK_URL")`. A 9 AM failure must reach a person by 9:05, not at the 9:30 stand-up. |
| **5 New bad data** | Each row quarantined with the right reason | All 5 caught, identically in ETL and ELT: ORD900001 `orders_customer_id_not_null`, ORD900002 `orders_quantity_positive`, ORD900003 `orders_store_fk`, ORD900004 `orders_status_allowed`, ORD900005 `orders_order_ts_not_future`. Each counter in `dq_results` +1 (18→19, 10→11, 5→6, 3→4, 7→8). Revenue unchanged. **ELT staging keeps the 5 FAIL rows after restore** (never in marts). | `observe quarantine`; `etl_quarantine_log`, `elt_quarantine`; `dq_results` | Nothing. This is the core design working. Optionally expire FAIL rows from `stg_orders` once the source no longer sends them. |
| **6 Schema change** | Fail loudly, load nothing | Both **FAILED** after 3 attempts (134 s / 139 s): `ValueError: Schema change in orders.csv: missing columns ['order_amount']; unexpected columns ['total']`. **No `raw_orders` rows** for the run; marts and fact untouched. **Loud, not silent.** | `extract_orders` / `load_raw_orders` Logs; `pipeline_runs` | A schema change is deterministic, so the 2 retries wasted about 2 minutes of the 9:00–9:30 window. Raise `AirflowFailException` for schema drift so it fails at once. |
| **7 Volume anomaly** | Volume check warns | `orders_volume_drop` **WARN** in both: rows **50**, previous **2,417**, change **−98%**. **The run stayed green, so nobody was alerted.** Fact stayed at 2,474 rows (the upsert never deletes), so the 9 AM report was protected. **Unexpected: 2,201 rows quarantined.** About 2,150 healthy payments failed `payments_order_fk` because their orders weren't in the short file: one bad source made a good one look broken. | ETL `validate` log; `dq_results`; `pipeline_runs.rows_quarantined` | Make a > 90% volume drop **Critical** and stop the run before validation: a truncated file is an incident, not a warning. |
| **8a Drift: price ×100** | Accuracy check catches it | `orders_amount_formula` **15 → 159** in both: all S05 orders failed and were quarantined (276 quarantined). S05 (≈₹77K) **vanished from the South numbers**. Caught, but a whole store disappeared from the 9 AM report. **This exposed an ETL bug:** removing S05 from the fact table also deleted ORD000708's RETURNED correction (from the increment), which the ETL had no raw copy to restore. **Fixed:** a failing base-file row no longer deletes an increment correction (same rule as the ELT MERGE). Re-tested through base → increment → 8a → 8b → restore; both pipelines agree at every step. | `validate` log; `observe store S05`; quarantine file; parity diff on ORD000708 | Add a store-level guard: if one store's rejection rate jumps from ~0% to 100%, alert ("feed problem at S05") rather than silently dropping the store. |
| **8b Drift: price and amount ×100** | Hard case: does anything catch it? | **No quality check fired:** `orders_amount_formula` stayed at 15. S05 revenue **₹77,22,470**; South **₹78,71,532.75**, about 7× the rest of the company combined. **Only reconciliation caught it: 127 `AMOUNT_MISMATCH`, variance −₹76.04 lakh**, because the payment gateway didn't inflate. | `report` log; `observe store S05`; `mart_payment_reconciliation` | Add distribution checks: `unit_price ≤ mrp`, and each store's average unit_price vs. its previous runs (warn above 3×). **Reconciliation is the distribution check of last resort:** feed its variance into `dq_results`. |
| **9 Freshness** | Timeliness check warns | `orders_freshness` **WARN** in both; latest valid order now 2026-09-21. **But it was already WARN on every run**: the September export is 16+ days old in October. A warning that is always on gets ignored. Side effect: quarantined **136 → 134**, because shifting dates turned two placeholder orders (`2026-10-21`) into "valid" September orders. Timeliness rules catch impossible dates, not wrong ones. | `validate` log; `dq_results` | Compare freshness against the **expected** business date ("yesterday's export by 09:00"), make > 2 days stale Critical, and alert. |
| **10 Recovery** | Only the failed task and downstream re-run | **Clear → Downstream** on `extract_products` / `load_raw_products`: only those and their downstream tasks re-ran; the other extracts kept their green state and original attempt. The **same run_id flipped FAILED → SUCCESS** in `pipeline_runs`. No raw file was loaded twice. Merge log: **10 protected**, so the returns stayed RETURNED. Clearing resets the run's `start_time`. | Clear dialog; Grid view; `pipeline_runs`; `elt_merge_log` | Keep `start_time` of the first attempt as well (add `first_attempt_at`), so the audit shows how late the recovered report really was. |

### The five pillars of data observability

| Pillar | How my pipelines cover it | Evidence | Gap |
| --- | --- | --- | --- |
| **Freshness** | `orders_freshness` every run (wall clock); "future" checks use the file's business date | WARN on every run (exp 9) | Always-on warning, no alert, no expected-arrival SLA |
| **Volume** | `orders_volume_drop` vs previous run; `rows_extracted / loaded / quarantined` per run | WARN at −98% (exp 7); `pipeline_runs` | Warning only; didn't stop the cascade into payments |
| **Schema** | `check_schema` before any row is read or loaded, in both pipelines | Loud failure, nothing loaded (exp 6) | Retries wasted on a deterministic error |
| **Distribution** | Indirectly: accuracy rules (formula, MRP, cost) and **reconciliation** | Exp 8a caught by a rule; **exp 8b caught only by reconciliation** | **No statistical checks** (per-store price or revenue vs history) |
| **Lineage** | `_source_file`, `_run_id`, `_loaded_at` on every row; ELT adds raw history with `_row_num` | Part E answer 3 traced a mart number to file rows; ELT kept ORD000708's history when the ETL lost it (exp 8a) | ETL keeps no raw copy, so its lineage stops at the file name |

**Covered best: schema** (fails loudly with an exact message before anything loads, in both pipelines), followed by **lineage in the ELT**.
**Weakest: distribution.** Experiment 8b inflated a store's revenue a hundredfold and every quality check passed. Only the reconciliation against the payment gateway raised it. Next step: per-store distribution checks fed from `dq_results` history.

## Part G: Recommendation to Sameer (CTO)

**To:** Sameer, CTO, NammaMart · **From:** Karamvir, FDE · **Re:** ETL or ELT for the 9 AM report

**Recommendation: adopt ELT as the platform, with an ETL-style quality gate in front of every finance table.** Land every file raw and untouched, transform in the warehouse with version-controlled SQL, and build the CFO's tables only from rows that passed every Critical check and are reconciled to the payment gateway. On the evidence below, this gives Finance what it asked for ("a bad row never reaches my dashboard") **and** gives Analytics and Compliance what they asked for ("keep everything, replay history").

**What I observed running both, side by side, on the same data (16 October, 22 runs, 10 failure experiments):**

| Criterion | ETL | ELT | Evidence |
| --- | --- | --- | --- |
| **Data quality guarantee** | Bad rows never reach finance tables | **Identical:** marts read only rows that passed every check | Same 136 rows quarantined; all 37 checks give identical counts; September revenue equal **to the paisa** (₹10,96,306.20) on every run (`etl_elt_parity`) |
| **Auditability** | Quarantine CSV per run; lineage stops at the file name | **Every version of every row**, with file, row number and run | Part E: a mart figure traced to 7 source rows in one query. Experiment 8a: **the ETL lost a customer's return correction** when an unrelated bad file arrived (since fixed); the ELT kept it because raw holds both versions. |
| **Ease of change** | A rule change only applies to future runs | A rule change is a SQL edit, and **history can be replayed from raw** | Experiment 3 / Part E answer 9: only the ELT can rebuild the "as booked" September from data it already holds |
| **Performance today** | **Faster: ~20 s** | ~40–50 s | `pipeline_runs`: 21 small tasks queue on DuckDB's single writer |
| **Performance at 1000×** (~2.4M orders/month) | Every row passes through Python memory and task hand-off files; that becomes the bottleneck | Set-based SQL inside the warehouse; DuckDB scans tens of millions of rows on one machine | Design-level judgement, not measured. Both would need a server and an always-on scheduler (Part C answer 4). |
| **Cost** | Open source, minimal storage | Open source; **raw storage grows with every run** (`raw_orders` 2,412 → 22,083 rows after 10 test runs; `raw_customers` holds 4,455 unmasked rows, i.e. 11 copies of the same 405 customers) | Needs the retention policy in Part E answer 6 |

**Why ELT, not ETL:** the ETL's one advantage is speed at today's tiny volume. Its weakness is structural: once a row is transformed, the original is gone. At NammaMart, the expensive questions are historical ones: "why doesn't September match the gateway?", "what did we report before the returns?", "show me the order exactly as received". Only the ELT can answer those without re-requesting exports. The quality guarantee Finance wants comes from the **gate**, not from transforming early, and the gate works identically in both.

**Before go-live, three things matter more than the ETL/ELT choice** (all seen in testing):

1. Reconciliation found **₹42,386.60 delivered but not collected**.
2. A checkout bug **overcharged customers by a net ₹36,480.33**.
3. **No failure alerted anyone:** experiments 4 and 7 were visible only to someone watching the screen.

**Top 3 risks of the recommended design, and how to contain them:**

1. **Personal data accumulates in the raw layer.** Raw keeps a full, unmasked copy of `customers.csv` from every run, and DuckDB has no per-user permissions. With India's DPDP Rules applying from 13 May 2027:
   - put raw in a separate store with restricted access;
   - purge raw customer snapshots after 30 days;
   - give analysts the masked marts only.
2. **Silent bad data passes the gate.** Experiment 8b inflated one store's revenue 100×; every quality check passed, and only reconciliation noticed. Warnings (volume −98%, stale feed) left runs green.
   - Add per-store distribution checks.
   - Make the volume drop Critical.
   - Route failures and reconciliation breaks to Slack or an on-call channel.
3. **The platform can't keep a 9 AM promise.** Airflow runs on a laptop (if the laptop is off at 9:00, there is no report), and DuckDB allows one writer, so the two DAGs queue on each other.
   - Move the scheduler to an always-on VM or managed Airflow.
   - Move to a multi-user database (e.g. PostgreSQL, also open source) when more than one team writes.
   - Schedule at 08:00, with an alert at 09:00 if there is no SUCCESS row in `pipeline_runs`.

---

## Screenshots

Folder: `assignment/screenshots/`

| File | Shows |
| --- | --- |
| `exp_DAG_run_successful.PNG` | Both DAGs: schedule `0 9 * * 1-6`, latest run green, Next Run 2026-10-17 09:00 IST (criterion 1; Part C) |
| `exp_DAG_failure.PNG` | Experiment 4: `extract_products` Failed after 3 tries; `report`, `reconcile` Upstream Failed; `run_audit` Failed; run duration 2 min 09 s |
| `exp06_schema_change_error.PNG` | Experiment 6: schema-change error in the task log |
| `exp07_volume_warn_log.PNG` | Experiment 7: `orders_volume_drop` WARN in the `validate` log |
| `exp08_southrevenue_lookslower_inreport.PNG` | Experiment 8a: `report` log, South net revenue down to ₹1,49,062.75 after S05 was quarantined |
| `exp09_freshness.PNG` | Experiment 9: `orders_freshness` WARN |
