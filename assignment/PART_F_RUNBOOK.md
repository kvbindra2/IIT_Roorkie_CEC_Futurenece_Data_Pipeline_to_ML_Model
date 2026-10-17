# Part F Runbook: Observability experiments (both DAGs)

Allow about 60–90 minutes for one sitting. Run each experiment on **both** DAGs: `nammamart_etl_karamvir` and `nammamart_elt_karamvir`.
For each one, record **Expected → Observed → Where I saw it → What I would change** in `SUBMISSION.md` → Part F (the Expected column is already filled in).

Two helper tools, run from the project root in the VS Code terminal:

| Tool | What it does |
| --- | --- |
| `python -m include.experiments_karamvir setup <n>` / `restore` / `status` | Prepares an experiment on a **copy** of the data, and puts the originals back. Never edit the CSVs by hand. Standard library only. |
| `python -m include.observe_karamvir <report>` | Read-only queries on the warehouse: `runs`, `dq`, `quarantine`, `counts`, `merge`, `recon`, `store S05`, `order ORD900003`, `all`. Needs `pip install duckdb pandas` in your `.venv`. Alternatively run it inside the container with `astro dev bash`. |

> Run `observe` **between** DAG runs. DuckDB allows one writer at a time, so if a DAG is mid-write you may get a lock error; just re-run it.
> `restore` also empties `incoming/`, so each experiment starts from the original September files.

---

## 0. Setup (once)

```powershell
astro dev restart                                  # picks up duckdb>=1.4 from requirements.txt
astro dev run dags list-import-errors              # must say: No data found
python -m include.experiments_karamvir restore     # clean starting point
Remove-Item include\warehouse_karamvir.duckdb -ErrorAction SilentlyContinue   # fresh history for Part F
```

In the Airflow UI (http://localhost:8080):

1. Unpause **both** DAGs. Unpausing may start one run straight away for today's 09:00 slot (expected with `catchup=False`, see Part C). Let it finish.
2. Always trigger the **ETL first, then the ELT**. The ELT's `parity_check` compares against the ETL tables.
3. Take the **Part C screenshots** now: the DAG list showing schedule `0 9 * * 1-6` and Next Run 03:30 UTC, the same with the UI timezone set to Asia/Kolkata, and the Details tab.

---

## 1. Happy path

**Do:** `restore`, trigger ETL, wait for green, trigger ELT.

**Observe:**
- **Graph view.** In the ETL, the 6 `extract_*` tasks run together, then `validate`; then `quarantine` and `transform` run in parallel. In the ELT, the 6 `load_raw_*` tasks plus `seed_reference_data` run together, then the 5 `stg_*`.
- **Task durations:** run → **Gantt** view.
- `python -m include.observe_karamvir runs`, then `counts`, then `quarantine`.

**Expected (from test runs):**

| | ETL | ELT |
| --- | --- | --- |
| rows extracted | 5,212 | 5,212 |
| orders loaded | 2,324 | 2,324 |
| rows quarantined | 136 (orders 88, payments 29, products 10, customers 8, stores 1) | 136 |
| September net revenue | ₹11,00,499.90 | ₹11,00,499.90 |
| `parity_check` log | | `MATCH - every September order has the same net revenue in ETL and ELT` |

**Screenshots:** both DAGs in Graph view (all green); one quarantine file open in VS Code (`include/quarantine/karamvir/orders_failed_<run_id>.csv`); query results for `dq_results`, `pipeline_runs` and `mart_payment_reconciliation`.

---

## 2. Re-run (idempotency)

**Do:** trigger both DAGs again with no changes.

**Observe:** `counts`, `merge`, and the `load_star_schema` task log.

**Expected:**
- **ETL:** `[Load] etl_fact_orders: inserted 0, updated 2324`; the fact stays at **2,324**; revenue unchanged.
- **ELT:**
  - `raw_orders` **grows** from 2,412 to 4,824 (and every raw table grows); `raw_orders runs` = 2.
  - `stg_orders` stays at **2,400**; merge log shows `inserted 0, updated 2400`; marts unchanged.
- **Is raw growth correct?** Yes. Raw is the append-only record of *every file as received*, tagged with `_run_id`. Staging and marts are de-duplicated views, so the business numbers don't double-count. The cost is storage growth, including a fresh unmasked copy of `customers.csv` every run (see Part E, answer 6, on retention).

---

## 3. Incremental load

**Do:** `python -m include.experiments_karamvir setup 3`, then trigger ETL, then ELT.

**Observe:** `merge`, `counts`, `recon`, and `order ORD000271`.

**Expected:**
- **ETL:** `inserted 150, updated 2324`; the fact grows from 2,324 to **2,474**.
- **ELT:** merge log shows `batch 2550, inserted 150, updated 2400, status_changes 10`; `stg_orders` = **2,550**, with no duplicates.
- **ORD000271:** DELIVERED (₹445) becomes RETURNED (₹0). In the ELT, `raw_orders` holds **both** versions.
- **September revenue:** ₹11,00,499.90 → **₹10,96,306.20** in both (the 10 returns, −₹4,193.70).
- **Reconciliation:** `RETURNED_NO_REFUND` = 11 (refunds not yet in the payment feed); `AWAITING_PAYMENT_FEED` = 150 (1 October orders newer than the payment export, held from revenue under D2).

**Bonus check:** `restore` (which removes the increment) and run both again. The ETL log says `10 orders keep their newer increment version`; the ELT merge log shows `protected 10`. The returns stay RETURNED.

---

## 4. Missing source file (do 10 straight after)

**Do:** `setup 4` (renames `products.csv` to `products.csv.bak`), then trigger ETL, then ELT.

**Observe:** Grid view task colours over about 2 minutes; the failed task's **Logs** (attempt 1/2/3); `runs`.

**Expected:**
- **ETL:**
  - `extract_products` goes up_for_retry (yellow) for 1 minute, then retries; after the 3rd attempt it is **failed** (red), with `FileNotFoundError: Source file not found: …/products.csv`.
  - The other extracts are **success**. `validate` and everything after it are **upstream_failed** (orange).
  - `run_audit` still runs (`all_done`), writes a `FAILED` row to `pipeline_runs`, and is itself marked failed, so the **DAG run is red**.
- **ELT:** same pattern on `load_raw_products`. The other `load_raw_*` tasks have already appended their rows for this `run_id`.
- **What alerted you:** **nothing pushed.** You only see it if you look at the UI or `pipeline_runs`. That's the finding: add an `on_failure_callback` that posts to Slack using `Variable.get("SLACK_WEBHOOK_URL")` (bonus marks).

## 10. Recovery (right after 4)

**Do:**
1. `python -m include.experiments_karamvir restore` (puts `products.csv` back).
2. In the UI, open the failed run → click the red `extract_products` (ETL) / `load_raw_products` (ELT) → **Clear** → tick **Downstream** (leave Upstream unticked) → confirm.

**Expected:**
- Only the failed task and everything downstream of it re-run. The successful extracts **stay green and do not re-run** (check their attempt numbers and start times).
- `run_audit` re-runs and **overwrites** that run's `pipeline_runs` row from FAILED to **SUCCESS**, because it's idempotent per run_id.
- In the ELT, the other raw files are not loaded twice: each raw load replaces only its own run's rows.

---

## 5. New bad data

**Do:** `setup 5` (appends ORD900001–ORD900005 to `orders.csv`), then trigger both.

**Observe:** `quarantine`, `dq`, and `order ORD900003`.

**Expected (identical in ETL and ELT):**

| Row | Lands in quarantine with |
| --- | --- |
| ORD900001 (null customer) | `orders_customer_id_not_null` |
| ORD900002 (quantity −2) | `orders_quantity_positive` |
| ORD900003 (store S42) | `orders_store_fk` |
| ORD900004 (status SHIPPED) | `orders_status_allowed` |
| ORD900005 (2027 timestamp) | `orders_order_ts_not_future` |

- `dq_results` counts go up by exactly 1 each (customer_id_not_null 18 → 19, quantity 10 → 11, store_fk 5 → 6, status_allowed 3 → 4, not_future 7 → 8).
- Revenue is unchanged and parity still MATCHes.
- **ELT detail:** `stg_orders` now holds the 5 as `dq_status = 'FAIL'` rows and **keeps them after `restore`** (staging is persistent). They never reach the marts, but staging remembers.

---

## 6. Schema change

**Do:** `setup 6` (renames `order_amount` to `total`), then trigger both.

**Expected:**
- **ETL:** `extract_orders` fails after 3 attempts with `ValueError: Schema change in orders.csv: missing columns ['order_amount']; unexpected columns ['total']`.
- **ELT:** `load_raw_orders` fails with the same message.
- In both, downstream tasks are upstream_failed and `pipeline_runs` shows FAILED.
- **Loud, not silent:** nothing is loaded, and yesterday's tables stay as they were.
- **What I'd change:** a schema error is deterministic, so the two retries just waste about 2 minutes. Raise `AirflowFailException` for schema drift so it fails at once.

---

## 7. Volume anomaly

**Do:** make sure experiment 1 or 2 ran first (that's the baseline), then `setup 7` (cuts `orders.csv` to 50 rows) and trigger both.

**Observe:** `dq`, `counts`, `runs`.

**Expected:**
- `orders_volume_drop` is **WARN** in both: rows 50, previous 2,412, change −98%. The ETL log shows `[DQ] orders_volume_drop: WARN (rows=50, previous=2412, change=-98%)`.
- **The DAG run is still green.** It's a Warning, so nothing alerts anyone.
- **The fact still has 2,324 rows and revenue is unchanged.** The upsert/MERGE never deletes orders missing from a short file. That protects the 9 AM numbers from a truncated export, but it also means an order genuinely deleted at source would never disappear.
- **What I'd change:** make a > 90% drop Critical (fail the run before loading), or at least alert on it.

---

## 8. Distribution drift: two variants

**8a. Do:** `setup 8` (multiplies `unit_price` by 100 for store S05 and leaves `order_amount` alone), then trigger both. Observe with `store S05`, `dq`, `recon`.

- `orders_amount_formula` jumps from 15 to **159** (all 144 S05 orders), so S05 is **quarantined entirely**.
- In the marts, S05 disappears: South net revenue falls from **₹2,26,732.45 to ₹1,49,507.75** (−₹77,224.70).
- The check caught it, but the business impact is a store vanishing from the 9 AM report.

**8b. Do:** `setup 8b` (multiplies `unit_price` **and** `order_amount` by 100 for S05, so each row is internally consistent), then trigger both.

- **No quality check fires.** The formula holds and every value is "valid".
- S05 revenue goes from **₹77,224.70 to ₹77,22,470.00**; South shows **₹78,71,977.75**, about seven times the rest of the company combined.
- **Only reconciliation catches it:** 128 S05 orders become `AMOUNT_MISMATCH`, variance **−₹76,45,345.30**, because the payment gateway didn't inflate.
- **Lesson:** reconciliation is the distribution check.
- **What I'd change:** add `unit_price ≤ mrp` (Accuracy) and a store-level check comparing average unit_price with the previous runs, warning if it moves more than about 3×.

---

## 9. Freshness

**Do:** `setup 9` (moves every order 30 days earlier), then trigger both.

**Expected:**
- `orders_freshness` is **WARN**. The log shows the latest valid order at **2026-09-21 10:15**: one of the shifted placeholder orders (the real orders now end on 31 August).
- **But it was already WARN at baseline:** the September export is 16+ days old at any run in October. An always-on warning is ignored, which is the real finding.
- The shift also turned 2 placeholder orders dated `2026-10-21` into "valid" 21 September orders, so they entered revenue. Timeliness rules catch impossible dates, not *wrong* ones.
- **What I'd change:** compare freshness against the **expected** business date ("yesterday's export by 09:00"), make "> 2 days stale" Critical, and alert on it.

---

## Finish

```powershell
python -m include.experiments_karamvir restore
python -m include.experiments_karamvir status    # should say: none / none
```

Trigger both DAGs one last time so the warehouse ends clean (with or without the increment, as you prefer for screenshots). Then send me your Observed notes; I'll write up Part F and use the results for Parts 0, A/B and G.
