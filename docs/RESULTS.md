# Results

Every number below comes from the code, the bookkeeping tables in Postgres (`pipeline.*`), the lake
(`data/lake`, queried with Spark on 2026-10-09), the Airflow metadata database, the Glue run ledger
(`data/glue_runs.jsonl`), test runs, or the run logs written when each step ran. The column **Source**
says which. Anything that could not be verified is marked **unverified**.

**Scope.** The Kaggle history spans 2016-09-04..2018-10-17. The pipeline has processed purchases from
2016-09-04 to **2018-01-12** (496 calendar days). All business numbers are as of the 2018-01-12 batch.

## 1. Engineering metrics

### Sources and tables

| Item | Value | Source |
|---|---|---|
| Sources | 3: Postgres (9 tables: 5 transactional, 4 reference), CRM JSONL drops, paged REST API | code |
| Lake tables | Bronze 11, Silver 11 (+ 2 `_quarantine`, 2 `_pending`), Gold 7 (4 dimensions, 1 fact, 1 aggregate, 1 metrics snapshot) | lake |
| Athena tables | 31 generated DDL files (Bronze 11, Silver 13, Gold 7) + 4 named KPI queries | `sql/athena/` |
| Code | 4,172 lines in `src/`, 2,524 in `scripts/`, `infra/` and `airflow/dags/` (Python) | `wc -l` |

### Rows per layer (lake after the 2018-01-12 batch)

| Layer | Rows | Largest tables |
|---|---|---|
| Bronze | 1,361,809 (361,646 without the one geolocation snapshot) | geolocation 1,000,163; customer_activity 70,860; orders 54,955 (every extracted version); order_items 54,307 |
| Silver | 428,149, incl. 260 quarantined rows and the 79 rows of the 2017-03-02 `_pending` partition (resolved the next day; 0 still pending) | customer_activity 70,615; order_items = order_lines 54,307; order_payments 50,700; orders 48,144; order_reviews 45,220; geolocation 19,015 |
| Gold | 728,568 | customer_metrics 578,686 (13 daily snapshots); fact_order_lines 54,307; dim_customer 47,973 versions; dim_product 32,951; agg_daily_category_sales 10,460 |

Silver row accounting over all batches (`pipeline.layer_runs`): 1,549,230 rows in = 567,727 valid +
260 quarantined + 981,164 duplicates + 79 pending. The duplicates are almost all geolocation
(1,000,163 rows collapsed to 19,015 zip prefixes) plus 16 duplicate CRM change requests.

### Days processed

| Batch | Days | How it ran | Source |
|---|---|---|---|
| 2017-02-28 | initial load (history 2016-09-04..2017-02-28) | Make targets | `pipeline.ingest_runs` |
| 2017-03-01..03-06 | 6 daily batches | `make daily` | `pipeline.ingest_runs` |
| 2017-12-31 | 1 catch-up batch covering 300 days (03-07..12-31) | Make targets | `pipeline.ingest_runs`, run log |
| 2018-01-01..01-06 | 6 daily batches | Airflow `olist_daily` (local Spark) | Airflow DB |
| 2018-01-07..01-09 | 3 daily batches | `make run-days` (local Spark + publish to S3) | run log |
| 2018-01-10 | 1 daily batch | local Bronze, Silver/Gold/DQ on Glue (`make glue-run`) | Glue ledger |
| 2018-01-11..01-12 | 2 daily batches | Airflow `olist_daily_aws` (Silver/Gold/DQ on Glue) | Airflow DB, Glue ledger |

20 Bronze/Silver batches in total; Gold was built for 13 of them (2017-12-31 onward).

### Tests and CI

| Item | Value | Source |
|---|---|---|
| Local test run | 118 passed in 165 s (unit, Spark and Postgres integration tests, with the Kaggle CSVs) | `pytest` on 2026-10-09 |
| GitHub Actions | ruff lint + pytest: 85 passed, 33 skipped (the tests that need the Kaggle CSVs) on both runs, `9b7b41a` and `a86ed94` | CI logs |
| Lint | `ruff check` and `ruff format --check` clean | CI |

### Data quality

| Item | Value | Source |
|---|---|---|
| Checks per run | Bronze 32, Silver 47, Gold 31 (110 in total; 8 check types) | `pipeline.dq_results` |
| Check results stored | 1,983 (Bronze 640 over 20 batches, Silver 940 over 20, Gold 403 over 13) | `pipeline.dq_results` |
| Blocking (`error`) failures on the stored runs | 0 | `pipeline.dq_results` |
| Warnings recorded | 75, from 8 checks: 7 geolocation zip prefixes with lat/lng outside Brazil (lat and lng checks, every batch: 140 rows each over 20 batches), API records missing optional fields (265 rows over 12 batches), sellers whose zip isn't in geolocation (91 rows over 13 batches), duplicate CRM change ids (4 rows over 2 batches), and 8 row-count jumps around the initial and catch-up loads | `pipeline.dq_results` |
| Bad rows caught by Silver | 260 quarantined with a reason (245 API records, 15 CRM requests); 79 late-arriving items/payments held in `_pending` and resolved on the next batch. The quarantine equals the dirt injected into the sources exactly (`verify-silver`) | `pipeline.layer_runs` |
| Failing checks on a dirty batch | `test_each_check_fails_on_the_dirty_batch`: not_null, unique, accepted_values, relationship and expression checks fail with samples; a check on a missing table gives `error_running` | unit test |

**Unverified:** how many blocking failures DQ raised during development. A rerun replaces a batch's rows
in `dq_results`, so only the final (passing) results are stored.

### API retries handled

| Run | Pages | Records | Retries | Waiting | Source |
|---|---|---|---|---|---|
| Catch-up, 300 days (2017-03-07..12-31) | 485 | 66,815 | 30 (15 × HTTP 429, 15 × HTTP 500) | 21.2 s | run log |
| Airflow daily runs (2018-01-01..06, 11, 12; 01-03 twice) | 18 | 2,611 | 1 | 1.0 s | Airflow task logs |

Every API run in these logs finished with all records and the record total matching the API's `total_records` (injected: 3% 429, 2% 500).

### Runtimes

| What | Time | Source |
|---|---|---|
| One day, local, Airflow `olist_daily` (10 tasks) | 137–151 s, mean 143 s over 6 runs | Airflow DB |
| Slowest local tasks (mean) | silver 38 s, dq_silver 24 s, gold 22 s, dq_gold 15 s, dq_bronze 14 s | Airflow DB |
| One day, local + DQ proof + S3 publish (`run-days`) | pipeline 130 / 134 / 127 s; with DQ check and publish 156 / 173 / 168 s (2018-01-07 / 08 / 09) | run log |
| Catch-up of 300 days | 305 s: replay 14 s, Postgres→Bronze 10 s, files→Bronze 55 s, API→Bronze 113 s, DQ Bronze 13 s, Silver 77 s, DQ Silver 23 s | run log |
| One day with Silver/Gold/DQ on Glue (`olist_daily_aws`, 20 tasks) | 1,216 s and 1,356 s (20.3 and 22.6 min) | Airflow DB |
| Glue tasks as seen by Airflow (mean, Flex start-up included) | glue_silver 329 s, glue_gold 296 s, glue_dq_silver 250 s, glue_dq_gold 234 s | Airflow DB |
| Glue job runs | 12 runs, 2,738 s of execution, 4,903 DPU-seconds; Spark time inside the job 75–310 s | Glue ledger |
| Spark on S3 from the laptop (why compute stayed local in step 8) | Bronze day 330 s (about 10 s locally); Silver stopped after 51 min | run log |

### Small files and skew

| Experiment | Result | Source |
|---|---|---|
| Daily vs monthly partitions of `fact_order_lines` on Athena | 383 files of 20.7 KB → 16 files of 322.5 KB; full scan 1.40 s → 0.47 s, one-month query 0.46 s → 0.36 s (median of 5) | `docs/SMALL_FILES.md` |
| Skewed sort-merge join on state (SP = 39% of rows) | busiest task 2,513,403 rows plain, 993,913 salted, 522,003 with AQE skew-join handling (25 tasks) | `docs/SALTING.md` |

### Idempotency and correctness

| Check | Result | Source |
|---|---|---|
| Silver rerun of 2018-01-12 (`verify-silver-idempotency`) | PASS: all 11 tables, 2 pending and 2 quarantine areas unchanged (e.g. order_lines 54,307 → 54,307), 64 s | run on 2026-10-09 |
| Gold rerun of 2018-01-12 (`verify-gold-idempotency`) | PASS: all 7 tables unchanged (fact 54,307, dim_customer 47,973, customer_metrics 578,686), 30 s | run on 2026-10-09 |
| Bronze = Postgres (`verify-bronze`) | PASS: latest version per key equals Postgres for 5 transactional tables (e.g. orders: 54,955 Bronze rows → 48,144 latest = 48,144 in Postgres) | run on 2026-10-09 |
| Silver checks (`verify-silver`) | PASS: 220 table-batches balance; quarantine equals the injected dirt exactly (245 negative_sessions, 15 unknown_customer); payments allocated to lines sum exactly for 47,585 orders; 0 rows left pending | run on 2026-10-09 |
| Gold checks (`verify-gold`) | PASS: SCD2 with 0 gaps/overlaps (47,973 versions, 46,665 customers); all 54,307 fact rows on the version valid at purchase; LTV = fact payments for 45,933 customers (R$7,565,259.72) | run on 2026-10-09 |
| Glue output = local Spark output | 2018-01-10: all 11 Silver tables, pending/quarantine and all 7 Gold tables identical | run log |
| S3 = local | 4,511 lake files (138.3 MB): 0 missing, 0 extra, 0 size or ETag differences | run log (`verify-s3-parity`) |
| Athena = Spark | 7/7 answers identical, 1.9 MB scanned | run log (`athena-check`) |

### AWS cost

| Item | Value | Source |
|---|---|---|
| Glue (12 pipeline runs) | 4,903 DPU-seconds × $0.29/DPU-hour (Flex) = **$0.395** | Glue ledger |
| Glue test job | 171 DPU-seconds = $0.014 | run log |
| S3, Athena, Secrets Manager, CloudWatch | **unverified**: Cost Explorer showed $0 with no line items for October on 2026-10-09 (data not posted yet) | Cost Explorer |
| Billed total | **unverified** for the same reason; the Glue part computes to $0.409 | |

All resources were deleted on 2026-10-09; nothing tagged `project=olist-pipeline` remains.

## 2. Business insights (as of 2018-01-12)

Queries ran with Spark on the Gold tables. "Revenue" is item prices excluding canceled and unavailable
orders (as in `agg_daily_category_sales`); order value uses allocated payments. Gold has no delivery dates
or review scores, so those two insights use Silver `orders` and `order_reviews`, joined to the fact for
the customer's state. Base: 47,363 sale orders, 54,038 sale lines, 45,933 customers, R$6,515,104.21
revenue.

**Top 10 categories by revenue**

| # | Category | Revenue (R$) | Share |
|---|---|---|---|
| 1 | bed_bath_table | 527,472.59 | 8.10% |
| 2 | watches_gifts | 514,752.70 | 7.90% |
| 3 | health_beauty | 509,216.58 | 7.82% |
| 4 | sports_leisure | 477,517.07 | 7.33% |
| 5 | computers_accessories | 434,818.31 | 6.67% |
| 6 | cool_stuff | 404,882.25 | 6.21% |
| 7 | furniture_decor | 365,294.93 | 5.61% |
| 8 | toys | 322,550.01 | 4.95% |
| 9 | garden_tools | 273,947.95 | 4.20% |
| 10 | auto | 252,359.84 | 3.87% |

**Monthly orders and revenue**

| Month | Orders | Revenue (R$) | Month | Orders | Revenue (R$) |
|---|---|---|---|---|---|
| 2016-09 | 2 | 207.86 | 2017-05 | 3,640 | 503,159.19 |
| 2016-10 | 290 | 44,507.30 | 2017-06 | 3,205 | 429,916.61 |
| 2016-11 | 0 | 0 | 2017-07 | 3,946 | 492,287.30 |
| 2016-12 | 1 | 10.90 | 2017-08 | 4,272 | 568,245.79 |
| 2017-01 | 787 | 120,098.27 | 2017-09 | 4,227 | 621,415.91 |
| 2017-02 | 1,718 | 244,959.35 | 2017-10 | 4,547 | 660,179.62 |
| 2017-03 | 2,617 | 368,341.32 | 2017-11 | 7,421 | 1,003,862.14 |
| 2017-04 | 2,377 | 353,842.98 | 2017-12 | 5,619 | 742,428.48 |
| | | | 2018-01 (1–12 only) | 2,694 | 361,641.19 |

- **Categories:** bed_bath_table is the top category with R$527,472.59 (8.10% of revenue), and the top 10 categories together bring in 62.67%.
- **Peak month:** November 2017 is the peak with 7,421 orders and R$1,003,862.14, 63.2% more orders than October 2017.
- **Growth:** monthly orders grew 7.1× within 2017, from 787 in January to 5,619 in December.
- **State concentration:** São Paulo (SP) accounts for 39.30% of orders and 35.97% of revenue, and SP, RJ and MG together for 65.07% of orders and 62.41% of revenue across 27 states.
- **RFM segments:** of 45,933 customers, 38.79% are Potential, 24.03% Hibernating, 20.00% Lost, 14.76% At risk, 1.46% Loyal and 0.95% (437) Champions.
- **Repeat customers:** only 2.82% (1,294 of 45,933) have ordered more than once, and the most orders by one customer is 9.
- **Average order value:** R$159.73 in payments per order (R$137.56 in item prices) over 47,363 orders.
- **Late delivery overall:** 4.89% of delivered orders (2,132 of 43,580) arrived after the estimated date, on average 10.4 days late. Orders still open on 2018-01-12 are not counted.
- **Late delivery by state:** among states with at least 200 delivered orders, Maranhão (MA) is worst at 14.21% and Mato Grosso do Sul (MS) best at 2.41%, while SP is at 3.45%.
- **Reviews:** late orders average 2.40 stars against 4.28 for on-time orders, and 59.06% of their reviews are 1–2 stars (9.20% on time).
- **Address changes (SCD2):** 1,286 customers have at least one address change (1,308 changes in total, up to 3 versions), and 1,050 of them moved to another state.

## 3. Project summary

**One line.** A daily batch pipeline that ingests a Postgres OLTP database, CRM file drops and a flaky REST
API into a Bronze/Silver/Gold Parquet lake with PySpark, gated by 110 data-quality checks, orchestrated by
Airflow, and run on AWS (S3, Athena, Glue).

**Short (GitHub / LinkedIn).** End-to-end data pipeline on the Olist e-commerce dataset: 3 sources (Postgres
CDC by watermark, CRM JSONL, a paged API with injected 429/500 errors) into Bronze/Silver/Gold Parquet with
PySpark, 110 DQ checks per day, SCD2 customers and RFM metrics. Orchestrated by Airflow, with Silver/Gold on
AWS Glue, the lake on S3 and Athena on top, for $0.41 of Glue. 20 batches over 496 days of history,
1.36M Bronze rows, reruns verified identical.

**Paragraph (portfolio).** This project rebuilds a realistic e-commerce data platform from the public Olist
dataset. A replay engine feeds a Postgres database one business day at a time, alongside daily CRM address-change
files and a FastAPI service that rate-limits and fails on purpose. PySpark jobs ingest all three into a
Bronze layer with watermark-based CDC (1.36M rows), clean and deduplicate into Silver with quarantine and
late-arrival handling (260 bad rows quarantined, 79 late rows resolved), and model a Gold star schema with
an SCD Type 2 customer dimension, daily category sales and RFM segments (54,307 order lines, 45,933
customers). 110 declarative data-quality checks run after every layer. Airflow runs a day end to end in
about 2.4 minutes locally; on AWS, the same Spark code runs as Glue Flex jobs against an S3 lake queried by
Athena, which cost $0.395 for 12 Glue runs, after a measured decision to keep laptop compute off S3. Every
day can be rerun with identical results, which is verified by tests, idempotency checks and S3/Athena
parity checks, and CI runs ruff and pytest on GitHub Actions.
