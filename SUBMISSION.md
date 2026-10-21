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

**TODO.** Describe:

- DAG flow (Extract ×5 in parallel → Validate → Quarantine → Transform → Load → Reconcile → Report → Run audit)
- Quarantine location and a sample of `failure_reason` values
- Masking approach for email and phone, with an example
- Star schema: `etl_fact_orders`, `etl_dim_customer`, `etl_dim_product`, `etl_dim_store`
- How idempotency is achieved
- Row counts: extracted / passed / quarantined per file

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

**TODO.** One table of checks implemented in both pipelines, based on the Part 0 issues log:

| Check name | Dimension | Severity | Applies to | ETL (Python) | ELT (SQL) | Issue ID |
|---|---|---|---|---|---|---|
| | Completeness | | | ✅ | ✅ | |
| | Uniqueness | | | | | |
| | Validity | | | | | |
| | Accuracy | | | | | |
| | Consistency | | | | | |
| | Timeliness | | | | | |

Plus a sample query and result from `dq_results`.

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
