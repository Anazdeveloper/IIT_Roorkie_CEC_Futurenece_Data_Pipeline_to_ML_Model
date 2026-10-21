# NammaMart Quick Commerce — ETL & ELT Pipeline Submission

**Learner:** Anaz A
**Module:** FDAIE Module 2 · Data Engineering Foundations
**DAGs:** `dags/nammamart_etl_anaz.py`, `dags/nammamart_elt_anaz.py`
**Warehouse:** `include/warehouse_anaz.duckdb`

> Sections marked **TODO** are filled in after the pipelines are built and the experiments are run.

## Contents

- [Part 0 — Discovery and data profiling](#part-0--discovery-and-data-profiling)
- [Part A — ETL pipeline](#part-a--etl-pipeline)
- [Part B — ELT pipeline](#part-b--elt-pipeline)
- [Part C — Scheduling](#part-c--scheduling)
- [Part D — Data quality dimensions](#part-d--data-quality-dimensions)
- [Business questions](#business-questions)
- [Part E — Data governance](#part-e--data-governance)
- [Part F — Observability tests](#part-f--observability-tests)
- [Part G — Recommendation to the client](#part-g--recommendation-to-the-client)
- [Screenshots](#screenshots)

---

## Part 0 — Discovery and data profiling

### Method

1. **Profile.** `include/profiling/profile_nammamart.py` profiles all six files (row counts, nulls, duplicate keys, min/max of numeric and date columns, distinct category values), checks referential integrity and every client business rule. Output: `include/reports/profiling/profile_summary.md` and `issues_summary.csv` (52 raw findings).
2. **Investigate.** `include/profiling/followup_checks.py` looks behind each finding (payment-mode breakdowns, raw values, which columns differ in duplicates) to separate real problems from noise.
3. **Review.** Each finding was judged on four questions: *root cause* (merge symptoms of one problem), *real or noise*, *severity* (what happens if the row loads) and *where it belongs* (quarantine vs reconciliation).

Raw files were read as text and never modified. No personal data (names, emails, phones) appears in any report.

### Profile summary

| File | Rows | Key | Duplicate keys | Issues found |
|---|---|---|---|---|
| orders.csv | 2,412 | order_id | 12 exact duplicate rows | 17 |
| payments.csv | 2,228 | payment_id | 4 exact duplicate rows | 7 |
| customers.csv | 405 | customer_id | 5 IDs with two versions | 9 |
| products.csv | 152 | product_id | 2 IDs with two versions | 4 |
| stores.csv | 15 | store_id | none | 2 |
| orders_2026-10-01_increment.csv | 160 | order_id | none | 0 (clean) |

Deduplication explains the brief's numbers: 405 − 5 = **400 customers**, 152 − 2 = **150 products**.

### Referential integrity

| Relationship | Problem rows | Detail |
|---|---|---|
| orders.customer_id → customers | 18 | customer_id blank (no orphans among filled values) |
| orders.product_id → products | 9 | 9 IDs (P9212, P9399 …) outside the real range P0001–P0152 |
| orders.store_id → stores | 5 | S00, S17, S99, outside S01–S15 |
| payments.order_id → orders | 8 | Test-like batch: identical timestamp, order IDs like ORD970544 |
| increment → customers / products / stores | 0 | Clean |

### Business rules

| Rule | Result |
|---|---|
| order_amount = quantity × unit_price × (1 − discount_pct/100) | 18 mismatches, off by exactly ×10, ×1.5 or ×0.5, or caused by discounts above 100% |
| order_status ∈ {DELIVERED, CANCELLED, RETURNED} | 3 `Pending??`, 2 lower-case `delivered` |
| payment_mode ∈ {UPI, Card, Cash on Delivery, Wallet} | 2 `Cashh`; 6 variants (`upi`, `CARD`, `cod`); 2 with spaces |
| DELIVERED orders have delivery_minutes; normal range 8–35 | 8 missing; 6 values of 480–1,440 (likely seconds) |
| SUCCESS payment = order_amount | 14 mismatches; 10 zero or negative SUCCESS payments |
| Cancelled orders have no payment | Holds |
| Increment = new orders + corrections | Holds: 150 new (all 2026-10-01) + 10 corrections (all DELIVERED → RETURNED) |

### Data Issues Log

Severity: **Critical** rows are quarantined; **Warning** rows load and are logged.
**(changed)** marks a severity that differs from the brief's suggestion. Master-file rows are never quarantined in a way that orphans facts: the bad attribute is set to NULL or `UNKNOWN` instead, because quarantining store S12 would silently remove its 135 orders from revenue.

#### orders.csv

| ID | Row key (sample) | Column | Problem | Dimension | Severity | Proposed rule | Rows |
|---|---|---|---|---|---|---|---|
| DI-01 | ORD001462, ORD001441 | all | Whole row exported twice | Uniqueness | Critical | Keep first copy, quarantine repeats | 12 |
| DI-02 | ORD002255, ORD000701 | customer_id | Blank | Completeness | Critical | Quarantine | 18 |
| DI-03 | ORD000403, ORD000362 | order_ts | Blank (one is the text `N/A`) | Completeness | Critical | Treat `NULL`/`N/A`/`-` as missing; quarantine | 2 |
| DI-04 | ORD001610, ORD000534 | order_ts | Impossible dates: month 13, 31 September | Validity | Critical | Quarantine; parse with an explicit format | 4 |
| DI-05 | ORD002119, ORD001693 | order_ts | Future-dated, with round repeated times (e.g. 2027-01-01 09:00) | Timeliness | Critical | Quarantine; likely test orders | 7 |
| DI-06 | ORD000500, ORD001970 | order_status | `Pending??` | Validity | Critical | Quarantine | 3 |
| DI-07 | ORD001066, ORD000153 | order_status | Lower-case `delivered` | Validity | Warning | Upper-case | 2 |
| DI-08 | ORD001812, ORD001414 | payment_mode | `Cashh` (its payments have a gateway_ref, so not real cash) | Validity | Critical | Quarantine; don't guess the mode | 2 |
| DI-09 | ORD000159, ORD002356 | payment_mode | `upi`, `CARD`, `cod`, extra spaces | Validity | Warning | Trim and map to canonical value | 8 |
| DI-10 | ORD000469, ORD001296 | quantity | Zero (4) or negative (6); not returns, which use RETURNED + REFUNDED | Accuracy | Critical | Quarantine | 10 |
| DI-11 | ORD002305, ORD002298 | order_amount | Formula mismatch: exact ×10 / ×1.5 / ×0.5 (a system bug) | Accuracy | Critical | Quarantine, don't overwrite; ×10 rows alone overstate revenue by up to ₹35,400 | 18 |
| DI-12 | ORD002103, ORD001468 | discount_pct | 250, 120, 150 | Validity | Critical | Quarantine | 3 |
| DI-13 | ORD000986, ORD001058 | product_id | Not in products (P9xxx) | Consistency | Critical | Quarantine | 9 |
| DI-14 | ORD001799, ORD001046 | store_id | S00, S17, S99 | Consistency | Critical | Quarantine | 5 |
| DI-15 | ORD001554, ORD001800 | delivery_minutes | Missing on DELIVERED order | Completeness | Critical | Quarantine (client rule) | 8 |
| DI-16 | ORD001037, ORD002101 | delivery_minutes | 480, 900, 1440: as seconds = 8, 15, 24 min | Accuracy | Critical | Quarantine; convert ÷60 if the client confirms seconds | 6 |
| DI-18 | ORD000398, ORD001113 | order_id | DELIVERED with no SUCCESS payment: 44 failed, 28 none | Consistency | Warning **(changed)** | Not quarantined; reconciliation reasons `PAYMENT_FAILED` / `NO_PAYMENT_RECORD` | 72 |

#### payments.csv

| ID | Row key (sample) | Column | Problem | Dimension | Severity | Proposed rule | Rows |
|---|---|---|---|---|---|---|---|
| DI-19 | PAY0000509, PAY0001651 | all | Whole row exported twice (explains the "double SUCCESS" flags) | Uniqueness | Critical | Keep first copy | 4 |
| DI-20 | PAY0002217 – PAY0002224 | order_id | Orders don't exist; identical timestamp; likely gateway test batch | Accuracy | Critical | Exclude; reconciliation `ORPHAN_PAYMENT` (₹4,580.57) | 8 |
| DI-21 | PAY0001568, PAY0000354 | amount_paid | SUCCESS amount ≠ order_amount | Consistency | Warning **(changed)** | Not quarantined; reconciliation `AMOUNT_MISMATCH` | 14 |
| DI-22 | PAY0000751, PAY0000944 | amount_paid | Zero or negative SUCCESS payment | Accuracy | Critical | Quarantine | 10 |
| DI-23 | PAY0000396, PAY0000127 | payment_ts | Future-dated; all belong to DI-05 orders | Timeliness | Critical | Quarantine | 7 |
| DI-24 | PAY0000522, PAY0000044 | payment_mode | `Cashh` | Validity | Warning **(changed)** | Load as `UNKNOWN` (quarantine would fake a reconciliation exception) | 2 |
| DI-25 | PAY0002188, PAY0002113 | payment_mode | Variants and extra spaces | Validity | Warning | Trim and map | 8 |

#### customers.csv

| ID | Row key (sample) | Column | Problem | Dimension | Severity | Proposed rule | Rows |
|---|---|---|---|---|---|---|---|
| DI-27 | C00036, C00103 | customer_id | Two versions: old and new address/phone | Uniqueness | Critical | Deduplicate before join (else 29 orders double-count) | 10 |
| DI-28 | C00400, C00398 | signup_date | Future-dated | Timeliness | Warning **(changed)** | Load; set signup_date NULL | 3 |
| DI-29 | C00307, C00063 | email | `@` missing (5) or no `.com` (3) | Validity | Warning | Load; mask; log | 8 |
| DI-30 | C00337, C00045 | email | Text `n/a` | Completeness | Warning | Treat as missing | 4 |
| DI-31 | C00151, C00240 | phone | Not a valid 10-digit mobile | Validity | Warning | Load; mask; log | 7 |
| DI-32 | C00199, C00171 | loyalty_tier | `BRONZE`, `bronze`, `SILVER` | Consistency | Warning | Title case | 8 |
| DI-33 | C00355, C00394 | city | Blank | Completeness | Warning | Load | 6 |
| DI-34 | C00132, C00083 | pincode | Not 6 digits | Validity | Warning | Load; log | 3 |
| DI-35 | C00395, C00342 | pincode | Outside Bengaluru (not 56xxxx) | Consistency | Warning | Load; ask client | 2 |

#### products.csv

| ID | Row key (sample) | Column | Problem | Dimension | Severity | Proposed rule | Rows |
|---|---|---|---|---|---|---|---|
| DI-36 | P0030, P0140 | product_id | Second copy named `(old)` with older MRP | Uniqueness | Critical | Drop rows ending `(old)` | 4 |
| DI-37 | P0051, P0060 | category | Blank | Completeness | Warning **(changed)** | Load as `UNKNOWN`; fix at source | 3 |
| DI-38 | P0019, P0073 | cost_price | Exactly 1.2 × MRP (normal ≈ 0.8 ×) | Accuracy | Warning **(changed)** | Load; cost NULL; exclude from margin | 4 |
| DI-39 | P0109 | mrp | −203 (sign error) | Accuracy | Warning **(changed)** | Absolute value if confirmed, else NULL | 1 |

#### stores.csv

| ID | Row key | Column | Problem | Dimension | Severity | Proposed rule | Rows |
|---|---|---|---|---|---|---|---|
| DI-40 | S07 | zone | Blank (176 orders) | Completeness | Warning **(changed)** | Confirm with ops; `UNKNOWN` until then | 1 |
| DI-41 | S12 | capacity_orders_per_day | 0, yet 135 orders | Accuracy | Warning **(changed)** | Set NULL | 1 |

#### Removed as noise or merged

| Raw finding | Rows | Reason |
|---|---|---|
| DI-17 unit_price above MRP | 12 | All are product P0109, whose MRP is negative (DI-39) |
| DI-26 SUCCESS without gateway_ref | 285 | All Cash on Delivery, which never has a gateway reference; rule narrowed to UPI/Card/Wallet |
| Statistical outliers (order_amount, amount_paid, quantity) | 120 | Large baskets are legitimate; real errors are caught exactly by DI-10, DI-11, DI-21, DI-22 |
| order_amount negative; double SUCCESS; signup after first order; null-as-text; whitespace | 26 | Same rows as another issue (merged) |

**Result:** 52 raw findings → **39 issues** (20 Critical, 19 Warning) across every file.

### Key findings

- **Goods delivered without payment.** 72 delivered orders have no successful payment: 44 after a failed payment, 28 with no payment at all.
- **Errors are systematic.** Exact multipliers (×10, ×1.5, ×0.5, cost = 1.2 × MRP) point to source-system bugs, which should be fixed at source, not only cleaned downstream.
- **Test entries are found by behaviour, not names.** 8 payments share one timestamp and reference non-existent orders; 7 orders have round, repeated future timestamps.
- **Returns** are recorded as status RETURNED plus a REFUNDED payment (69 returns, 68 refunds), never as negative quantities.

### Questions for the client

1. **Unpaid deliveries:** 72 orders were delivered without a successful payment (44 failed, 28 none). Should they count as revenue, and is the business aware it's delivering after failed payments?
2. **Delivery time units:** six orders show 480, 900 or 1,440 minutes, which are 8, 15 and 24 minutes if read as seconds. Does any app or store record seconds?
3. **Customer versions:** five customers appear twice with different addresses. Which version is current, and should we keep the history?

---

## Part A — ETL pipeline

**Files:** `dags/nammamart_etl_anaz.py` (DAG wiring only) and `include/nammamart_etl.py` (all pipeline logic, testable without Airflow), using the shared rules in `include/dq_rules.py`.

### Flow (17 tasks)

```
extract_orders ─┐                       ┌─ validate_orders ──────────┐
extract_payments├─(parallel)─────────── ├─ validate_payments         │
extract_customers                       ├─ validate_customers        ├─ transform → load → reconcile → report → run_audit
extract_products│                       ├─ validate_products         │                                       (all_done)
extract_stores ─┤                       ├─ validate_stores           │
extract_increment (only with load_increment=true) └─ validate_orders_increment ┘
```

| Step | What it does |
|---|---|
| **Extract** | Five files in parallel, read exactly as received (all text). Schema is checked first: a renamed or missing column fails the task loudly instead of loading wrong data. |
| **Validate** | Applies the shared rules. Each row gets `dq_status` (PASS / WARN / FAIL) and a reason. |
| **Quarantine** | FAIL rows are written to `include/quarantine/anaz/<file>_failed_<run_id>.csv` exactly as received, with `failure_reason` as the first column. Nothing is dropped silently. |
| **Transform** | Repairs master-data attributes (zone/category → `UNKNOWN`, invalid MRP/cost/capacity → NULL), masks personal data, joins orders to all three dimensions, derives `order_date`, `net_revenue`, `gross_margin`, `within_15_min`. |
| **Load** | Star schema in `include/warehouse_anaz.duckdb`. |
| **Reconcile** | Compares loaded orders with `payments.csv` → `etl_payment_reconciliation`, one row per mismatch with a reason. |
| **Report** | Builds `etl_daily_zone_revenue` and `etl_category_margin` and prints them in the task log. |
| **Run audit** | `trigger_rule="all_done"`: writes one row to `pipeline_runs` even when the run fails. |

### Star schema

| Table | Grain | Notes |
|---|---|---|
| `etl_fact_orders` | one row per order_id (primary key) | `net_revenue` = order_amount for DELIVERED orders only; `gross_margin` = net_revenue − quantity × cost_price (NULL when cost is unknown); `within_15_min` for DELIVERED orders; `dq_warnings`; lineage columns `_source_file`, `_source_date`, `_run_id`, `_loaded_at` |
| `etl_dim_customer` | one row per customer_id | `customer_name_masked` (`P*** S***`), `email_masked` (`pr***@example.com`), `phone_masked` (`98******21`); no raw name, email or phone |
| `etl_dim_product` | one row per product_id | category `UNKNOWN` when blank; cost_price NULL when ≤ 0 or above MRP |
| `etl_dim_store` | one row per store_id | zone `UNKNOWN` when blank; `manager_name` (personal data) dropped as not needed |

### How the guarantees are met

- **No bad order reaches finance tables:** only PASS and WARN rows are loaded; every Critical failure is quarantined.
- **No double counting:** duplicate customers and products are resolved before the join, and the join is checked with `validate="many_to_one"`. The task fails if any dimension key is duplicated, instead of silently doubling revenue.
- **Idempotent load:** dimensions are fully replaced each run. The fact table is loaded per source file: that file's rows are deleted, then upserted on `order_id` (`INSERT … ON CONFLICT DO UPDATE`). Re-running gives identical row counts.
- **Late corrections are applied and never undone:** each fact row records its source file's business date. An upsert only overwrites a row when the incoming file is the same age or newer, so the 1 October increment turns DELIVERED orders into RETURNED, and re-running September later cannot turn them back.
- **Data hand-off:** tasks pass small pickle files in `include/tmp/anaz/<run_id>/`; XCom carries only paths and counts.
- **One DuckDB writer at a time:** only the sequential tasks (load onwards) open the warehouse, and `connect()` waits and retries if the ELT DAG holds DuckDB's single-writer lock.

### Running it

- Scheduled: 09:00 IST, Monday–Saturday.
- Manual: *Trigger DAG*. For the increment, *Trigger DAG w/ config* with `{"load_increment": true}`.
- `as_of` (default `2026-10-01`) is the run date used by the timeliness checks.

### Results

First run on the real data: `manual__2026-10-21T23:28:38.831953+00:00` (all 17 tasks green). "Passed" includes the rows with warnings; passed + quarantined = rows in.

| File | Rows in | Passed | Of which with warnings | Quarantined | Quarantined because |
|---|---|---|---|---|---|
| orders.csv | 2,412 | 2,308 | 79 | 104 | 107 Critical flags on 104 rows (3 rows broke two rules, e.g. a 250% discount also breaks the amount formula) |
| payments.csv | 2,228 | 2,199 | 22 | 29 | duplicates 4, zero/negative SUCCESS 10, orphan test batch 8, future-dated 7 |
| customers.csv | 405 | 400 | 41 | 5 | the older copy of each duplicated customer_id |
| products.csv | 152 | 150 | 8 | 2 | the two `(old)` catalogue rows |
| stores.csv | 15 | 15 | 2 | 0 | none: S07 (zone) and S12 (capacity) are repaired, keeping their 311 orders |
| **Total** | **5,212** | **5,072** | **152** | **140** | |

Every check's row count matches the Part 0 Data Issues Log exactly (for example orders: 18 missing customer_id, 12 duplicates, 18 amount mismatches; payments: 14 amount mismatches sent to reconciliation as warnings).

What the run demonstrates:

- **Master files lose only true duplicates.** 400 customers and 150 products remain, the counts the brief describes. Every attribute problem is repaired, so no order is orphaned.
- **Reconciliation exceptions stay in the data.** The 72 unpaid deliveries and 14 payment amount mismatches load as warnings and appear in `etl_payment_reconciliation`, instead of being hidden in quarantine.
- **The narrowed gateway rule holds:** `gateway_ref_present` reports 0 failures, confirming the 285 Cash on Delivery payments were noise (DI-26).

---

## Part B — ELT pipeline

**TODO.** Describe:

- Raw / staging / mart layers and the SQL file per table in `include/sql/`
- The MERGE for `orders_2026-10-01_increment.csv`: rows inserted vs updated
- **ETL vs ELT September totals:**

| Metric | ETL | ELT | Match? | If not, why |
|---|---|---|---|---|
| Delivered orders | | | | |
| Net revenue | | | | |
| Gross margin | | | | |
| Rows rejected | | | | |

---

## Part C — Scheduling

Both DAGs use:

```python
import pendulum
from datetime import timedelta

default_args = {
    "owner": "anaz",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

@dag(
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
)
```

**What is your cron expression?**
`0 9 * * 1-6`: minute 0, hour 9, every day of the month, every month, Monday (1) to Saturday (6).

**What time does the run show in UTC in the Airflow UI?**
**03:30 UTC.** IST is UTC+5:30, so 09:00 IST = 03:30 UTC. Airflow stores all times in UTC; the UI can be switched to show IST. *(Confirm on the DAG's "Next run" field and add the screenshot.)*

**What would happen at 9 AM if the start_date had been left in UTC?**
The cron would be read in UTC, so the DAG would run at 09:00 UTC = **14:30 IST**, five hours after the 9:30 AM review. The numbers would arrive later than the manual Excel report they replace.

**What happens if the laptop or Docker is off at 9 AM?**
The scheduler isn't running, so nothing runs and nothing alerts anyone. When Airflow starts again, `catchup=False` means only the most recent missed run is created, not one per missed day, so the data arrives late but isn't processed twice. In production this requires an always-on server or a managed Airflow service, plus a freshness alert that fires if the data isn't ready by 9:15.

**Why `catchup=False`?**
Each run reprocesses the full source files, so running every missed day since `start_date` would repeat the same work many times and flood the logs. The business only needs today's numbers by 9 AM.

**Why these retry settings?**
Most failures here are transient (a file not yet copied, a DuckDB lock). Two retries five minutes apart absorb those, and a 20-minute timeout stops a hung task. Worst case (run + 2 retries) still finishes before the 9:30 review.

---

## Part D — Data quality dimensions

All checks live in **one file, `include/dq_rules.py`**, used by both pipelines. Each rule has a pandas version (ETL) and a DuckDB SQL version (ELT). Running `python include/dq_rules.py` is a parity test: it applies both versions to the same data and confirms they flag exactly the same rows, so ETL and ELT can't drift apart.

**Severity → outcome:** Critical → row quarantined (ETL) / `dq_status = 'FAIL'` (ELT). Warning → row loads, issue logged / `dq_status = 'WARN'`.
**Actions:** QUARANTINE · DEDUPE (extra copies quarantined, one version kept) · REPAIR (row loads, bad attribute set to NULL/UNKNOWN) · STANDARDISE (casing/spaces fixed) · RECONCILE (row loads and appears in the payment reconciliation) · LOG · ALERT (dataset-level).

### Rule catalogue (57 rules)

| Check | File | Dimension | Severity | Action | Rule | Issue |
|---|---|---|---|---|---|---|
| `customer_id_not_null` | customers | Completeness | Critical | QUARANTINE | customer_id is present | - |
| `email_present` | customers | Completeness | Warning | LOG | email is present | DI-30 |
| `city_present` | customers | Completeness | Warning | LOG | city is present | DI-33 |
| `order_id_not_null` | orders | Completeness | Critical | QUARANTINE | order_id is present | - |
| `order_customer_id_not_null` | orders | Completeness | Critical | QUARANTINE | customer_id is present | DI-02 |
| `order_ts_not_null` | orders | Completeness | Critical | QUARANTINE | order_ts is present | DI-03 |
| `delivered_has_delivery_minutes` | orders | Completeness | Critical | QUARANTINE | DELIVERED orders have delivery_minutes | DI-15 |
| `payment_id_not_null` | payments | Completeness | Critical | QUARANTINE | payment_id is present | - |
| `payment_order_id_not_null` | payments | Completeness | Critical | QUARANTINE | order_id is present | - |
| `gateway_ref_present` | payments | Completeness | Warning | LOG | SUCCESS UPI/Card/Wallet payments have a gateway_ref (COD never does) | DI-26 (revised) |
| `product_id_not_null` | products | Completeness | Critical | QUARANTINE | product_id is present | - |
| `category_present` | products | Completeness | Warning | REPAIR | category is present (else UNKNOWN) | DI-37 |
| `store_id_not_null` | stores | Completeness | Critical | QUARANTINE | store_id is present | - |
| `customer_id_unique` | customers | Uniqueness | Critical | DEDUPE | one version per customer_id: the LAST row in the file is kept (pending client) | DI-27 |
| `order_id_unique` | orders | Uniqueness | Critical | DEDUPE | only the first copy of an order_id loads | DI-01 |
| `payment_id_unique` | payments | Uniqueness | Critical | DEDUPE | only the first copy of a payment_id loads | DI-19 |
| `product_id_unique` | products | Uniqueness | Critical | DEDUPE | one row per product_id: rows named '(old)' lose, then first by _row | DI-36 |
| `store_id_unique` | stores | Uniqueness | Critical | DEDUPE | one row per store_id | - |
| `email_format` | customers | Validity | Warning | LOG | email looks like name@domain.tld | DI-29 |
| `phone_format` | customers | Validity | Warning | LOG | phone is a 10-digit Indian mobile after removing +91 / 0 | DI-31 |
| `customer_pincode_format` | customers | Validity | Warning | LOG | pincode is 6 digits | DI-34 |
| `loyalty_tier_allowed` | customers | Validity | Warning | REPAIR | loyalty_tier is Bronze, Silver, Gold or Platinum (else UNKNOWN) | - |
| `signup_date_valid` | customers | Validity | Warning | REPAIR | signup_date is a real date (else NULL) | - |
| `order_ts_valid` | orders | Validity | Critical | QUARANTINE | order_ts is a real ISO-8601 timestamp | DI-04 |
| `order_status_allowed` | orders | Validity | Critical | QUARANTINE | order_status is DELIVERED, CANCELLED or RETURNED | DI-06 |
| `order_payment_mode_allowed` | orders | Validity | Critical | QUARANTINE | payment_mode maps to UPI, Card, Cash on Delivery or Wallet | DI-08 |
| `discount_pct_in_range` | orders | Validity | Critical | QUARANTINE | discount_pct between 0 and 100 (blank = 0) | DI-12 |
| `order_status_standard` | orders | Validity | Warning | STANDARDISE | order_status already in canonical form | DI-07 |
| `order_payment_mode_standard` | orders | Validity | Warning | STANDARDISE | payment_mode already in canonical form (no variants or spaces) | DI-09 |
| `payment_ts_valid` | payments | Validity | Critical | QUARANTINE | payment_ts is a real ISO-8601 timestamp | - |
| `payment_status_allowed` | payments | Validity | Critical | QUARANTINE | payment_status is SUCCESS, FAILED or REFUNDED | - |
| `payment_mode_known` | payments | Validity | Warning | REPAIR | payment_mode maps to an allowed value (else loads as UNKNOWN) | DI-24 |
| `payment_mode_standard` | payments | Validity | Warning | STANDARDISE | payment_mode already in canonical form | DI-25 |
| `zone_allowed` | stores | Validity | Warning | REPAIR | zone is North, South, East or West (else UNKNOWN) | DI-40 |
| `store_pincode_format` | stores | Validity | Warning | LOG | pincode is 6 digits | - |
| `quantity_positive_integer` | orders | Accuracy | Critical | QUARANTINE | quantity is a whole number above 0 | DI-10 |
| `unit_price_positive` | orders | Accuracy | Critical | QUARANTINE | unit_price above 0 | - |
| `order_amount_matches_formula` | orders | Accuracy | Critical | QUARANTINE | order_amount = quantity x unit_price x (1 - discount_pct/100), within 0.01 | DI-11 |
| `delivery_minutes_plausible` | orders | Accuracy | Critical | QUARANTINE | delivery_minutes above 0 and below 120 | DI-16 |
| `delivery_minutes_normal_range` | orders | Accuracy | Warning | LOG | delivery_minutes inside the normal 8-35 range | - |
| `success_amount_positive` | payments | Accuracy | Critical | QUARANTINE | a SUCCESS payment is above 0 | DI-22 |
| `mrp_positive` | products | Accuracy | Warning | REPAIR | mrp above 0 (else NULL) | DI-39 |
| `cost_price_positive` | products | Accuracy | Warning | REPAIR | cost_price above 0 (else NULL, excluded from margin) | - |
| `cost_price_not_above_mrp` | products | Accuracy | Warning | REPAIR | cost_price <= mrp (else cost NULL, excluded from margin) | DI-38 |
| `capacity_positive` | stores | Accuracy | Warning | REPAIR | capacity_orders_per_day above 0 (else NULL) | DI-41 |
| `loyalty_tier_standard` | customers | Consistency | Warning | STANDARDISE | loyalty_tier uses one casing | DI-32 |
| `customer_pincode_bengaluru` | customers | Consistency | Warning | LOG | pincode is in Bengaluru (56xxxx) | DI-35 |
| `order_customer_exists` | orders | Consistency | Critical | QUARANTINE | customer_id exists in customers | - |
| `order_product_exists` | orders | Consistency | Critical | QUARANTINE | product_id exists in products | DI-13 |
| `order_store_exists` | orders | Consistency | Critical | QUARANTINE | store_id exists in stores | DI-14 |
| `delivered_has_success_payment` | orders | Consistency | Warning | RECONCILE | DELIVERED orders have a SUCCESS payment | DI-18 |
| `payment_order_exists` | payments | Consistency | Critical | QUARANTINE | order_id exists in orders (orphans look like test transactions) | DI-20 |
| `success_amount_matches_order` | payments | Consistency | Warning | RECONCILE | a SUCCESS payment equals its order's order_amount | DI-21 |
| `signup_date_not_future` | customers | Timeliness | Warning | REPAIR | signup_date is not after the run date (else NULL) | DI-28 |
| `order_ts_not_future` | orders | Timeliness | Critical | QUARANTINE | order_ts is not after the run date | DI-05 |
| `orders_fresh` | orders | Timeliness | Warning | ALERT | the latest order is no more than 1 day older than the run date | - |
| `payment_ts_not_future` | payments | Timeliness | Critical | QUARANTINE | payment_ts is not after the run date | DI-23 |

### Coverage: every dimension is checked, in both pipelines

```
dimension  Accuracy  Completeness  Consistency  Timeliness  Uniqueness  Validity
file                                                                            
customers         0             3            2           1           1         5
orders            5             4            4           2           1         6
payments          1             3            2           1           1         4
products          3             2            0           0           1         0
stores            1             1            0           0           1         2
```

### Design decisions

- **Master rows are never quarantined for a bad attribute.** Product, customer and store attribute problems are REPAIR, not QUARANTINE, so their orders are never orphaned.
- **Reconciliation exceptions are not rejects.** `delivered_has_success_payment` and `success_amount_matches_order` are Warnings with action RECONCILE.
- **No rounding in the formula check.** `order_amount` is compared with the unrounded formula, with a 0.01 tolerance, because pandas and DuckDB round halves differently.
- **`NOT EXISTS`, not `NOT IN`, for key checks in SQL.** `NOT IN` returns no rows at all if the lookup list contains a NULL, which would silently pass every orphan.
- **Deduplication rules are explicit:** first copy of an order/payment, last row of a customer (pending client), non-`(old)` row of a product.

### `dq_results` sample

**TODO:** paste a query and result after the first run:

```sql
SELECT check_name, dimension, severity, rows_checked, rows_failed, status
FROM dq_results WHERE run_id = '<run_id>' ORDER BY status, check_name;
```

---

## Business questions

**TODO.** For each question, the SQL and result from both pipelines.

| # | Question | ETL table | ELT table |
|---|---|---|---|
| 1 | Daily net revenue by zone and store | `etl_daily_zone_revenue` | `mart_daily_store_kpis` |
| 2 | Revenue and gross margin by category | `etl_category_margin` | `mart_category_margin` |
| 3 | Average delivery time, % within 15 min | | `mart_delivery_sla` |
| 4 | Revenue by payment mode; unreconciled total | `etl_payment_reconciliation` | `mart_payment_reconciliation` |
| 5 | Top 10 customers with loyalty tier | | `mart_top_customers` |
| 6 | Rows rejected today, by file and reason | quarantine files | `stg_*` where `dq_status = 'FAIL'` |

---

## Part E — Data governance

**TODO.** 2–4 lines each, using this pipeline as the example:

1. Ownership
2. Data dictionary *(table: column, type, meaning, source, allowed values, PII)*
3. Lineage
4. Personal data *(columns: full_name, email, phone; India's Digital Personal Data Protection Act, 2023)*
5. Access control
6. Retention
7. Secrets
8. Audit
9. Change history

---

## Part F — Observability tests

**TODO.** Run each on both DAGs.

| # | Experiment | Expected | Observed (ETL) | Observed (ELT) | Where I saw it | What I would change |
|---|---|---|---|---|---|---|
| 1 | Happy path | | | | | |
| 2 | Re-run (idempotency) | | | | | |
| 3 | Incremental load | | | | | |
| 4 | Missing source file | | | | | |
| 5 | New bad data | | | | | |
| 6 | Schema change | | | | | |
| 7 | Volume anomaly | | | | | |
| 8 | Distribution drift | | | | | |
| 9 | Freshness | | | | | |
| 10 | Recovery | | | | | |

**Five pillars of observability:**

| Pillar | How it is covered | Strength |
|---|---|---|
| Freshness | | |
| Volume | | |
| Schema | | |
| Distribution | | |
| Lineage | | |

---

## Part G — Recommendation to the client

**TODO.** One page to Sameer (CTO): ETL or ELT, based on observed evidence. Cover auditability, data quality guarantees, ease of change, performance at 1000× data, and cost. End with the top 3 risks.

---

## Screenshots

**TODO.** Save in `docs/screenshots/` and link here.

- [ ] ETL DAG, Graph view, green run
- [ ] ELT DAG, Graph view, green run
- [ ] Schedule showing 9:00 AM IST and next run time
- [ ] One quarantine file
- [ ] `dq_results` query result
- [ ] `pipeline_runs` query result
- [ ] Payment reconciliation table
