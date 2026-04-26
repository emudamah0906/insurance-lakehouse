# Insurance Claims Lakehouse — Interview Preparation Notes

**Project:** Insurance Claims Lakehouse (Portfolio Project)
**GitHub:** https://github.com/emudamah0906/insurance-lakehouse
**Stack:** Python · PySpark · Delta Lake · Airflow · dbt · Snowflake · MinIO · Great Expectations · Metabase
**Target Companies:** Manulife, Sun Life, Intact, Tokio Marine, RBC, TD

---

## PART 1 — PROJECT SUMMARY (30-second elevator pitch)

> "I built an end-to-end insurance claims data lakehouse entirely on Docker to
> mirror what a production DE platform looks like at a Canadian insurer.
> The pipeline ingests synthetic claims data — 500,000 claims, 100,000 policies,
> 50,000 customers — runs it through a Medallion architecture (Bronze → Silver →
> Gold), enforces data quality with Great Expectations, serves business marts
> through Snowflake via dbt, and visualises KPIs in Metabase.
> Every layer uses the tools you see in Canadian insurance DE job descriptions:
> Spark, Airflow, Delta Lake, dbt, and Terraform for IaC."

---

## PART 2 — ARCHITECTURE: THE MEDALLION PATTERN

### What is a Medallion / Lakehouse Architecture?

A way of organising data in layers, each layer adding more quality and structure.
Named after the shape of a medallion (Bronze < Silver < Gold).

```
RAW (MinIO)
  ↓  PySpark
BRONZE (Delta Lake)      ← exact copy of raw, schema enforced, nothing deleted
  ↓  Great Expectations DQ
SILVER (Delta Lake)      ← cleaned, deduped, type-cast, SCD2 on policies
  ↓  dbt models
GOLD (Delta + Iceberg)   ← business-ready aggregates and dimension tables
  ↓  dbt-snowflake
SNOWFLAKE                ← presentation layer for BI tools
  ↓
METABASE DASHBOARD       ← loss ratio, fraud rate, claims by province
```

### Why Medallion instead of a traditional Data Warehouse?

| Traditional DW | Lakehouse (Medallion) |
|---|---|
| Schema-on-write (must define schema before loading) | Schema-on-read (store raw, apply schema later) |
| Expensive to re-process historical data | Can replay from Bronze anytime |
| Poor support for semi-structured data | Handles JSON, CSV, Parquet natively |
| Vendor lock-in | Open formats (Delta, Iceberg, Parquet) |
| Hard to audit what changed | Delta Lake transaction log = full audit trail |

**Interview answer when asked "Why did you choose a Lakehouse?":**
> "Insurance data arrives from multiple source systems in different formats
> — claims from a core system (CSV), policies from an API (JSON), customer
> data from a CRM. A Lakehouse lets us land everything raw first, then apply
> quality and transformations incrementally. If we discover a bug in Silver,
> we can replay from Bronze without touching the source systems again. That
> replay ability is critical in regulated industries like insurance."

---

## PART 3 — THE DATA: WHAT WE GENERATE AND WHY

### Three Entities

**Customers (50,000 rows)**

| Column | Type | Notes |
|---|---|---|
| customer_id | string | PK — format CUST_000001 |
| first_name, last_name | string | Faker Canadian locale |
| dob | date | age 18–85 |
| province | string | 13 Canadian provinces/territories, population-weighted |
| postal_code | string | Canadian format: A1A 1A1 (letter-digit-letter space digit-letter-digit) |
| email, phone | string | Faker |
| created_at | timestamp | random between 2010–2024 |

**Policies (100,000 rows)**

| Column | Type | Notes |
|---|---|---|
| policy_id | string | PK — format POL_0000001 |
| customer_id | string | FK → customers (referential integrity maintained) |
| product_type | string | auto 50%, home 35%, life 15% |
| coverage_amount | decimal | auto: 20K–100K, home: 200K–1.5M, life: 100K–2M |
| premium | decimal | 0.5%–2.5% of coverage amount |
| start_date, end_date | date | end is 1–3 years after start |
| status | string | active 65%, expired 20%, cancelled 10%, lapsed 5% |

**Claims (500,000 rows)**

| Column | Type | Notes |
|---|---|---|
| claim_id | string | PK — format CLM_0000001 |
| policy_id | string | FK → policies |
| claim_date | date | 2015–2024 |
| loss_date | date | always 1–30 days BEFORE claim_date (business rule) |
| claim_amount | decimal | log-normal distribution — realistic heavy tail |
| claim_status | string | open/closed/pending/denied/settled |
| claim_type | string | collision, theft, fire, flood, liability, medical, etc. |
| adjuster_id | string | 200 adjusters (ADJ_0001 to ADJ_0200) |
| fraud_flag | boolean | ~3% fraud rate |

### Why log-normal for claim amounts?
Real insurance claims follow a heavy-tail distribution — most claims are small
(fender benders, minor water damage) but a few are catastrophically large
(total vehicle loss, house fire). Log-normal captures this shape. Mean ~$4,900
but with outliers above $500,000.

### Intentional Dirty Data (~5% per entity)

This is critical — it proves the DQ layer (Phase 4) is doing real work.

| Type | How injected | What DQ catches |
|---|---|---|
| Nulls | Random non-PK columns set to NULL | not_null expectation |
| Duplicates | 1% of rows copied with same PK | unique expectation |
| Negative amounts | claim_amount and coverage_amount set to negative | expect_column_values_to_be_between |
| Malformed postal codes | replaced with "INVALID999" | regex expectation |
| Impossible dates | loss_date set to 2099-12-31 (after claim_date) | custom date sanity check |
| Invalid end dates | policy end_date set to 1999-12-31 (before start) | date ordering check |

---

## PART 4 — STORAGE: MINIO (S3-COMPATIBLE)

### What is MinIO?
An open-source object store that speaks the Amazon S3 API exactly.
Any code that works against AWS S3 works against MinIO without changes.

### Why MinIO instead of a real S3 bucket?
$0 budget constraint. For a real project at Manulife or Sun Life,
this would be AWS S3 or Azure Data Lake Storage Gen2. MinIO lets us
practice locally with identical boto3 code.

### Bucket Structure

```
s3://raw/          ← Phase 2: Faker generator output
s3://bronze/       ← Phase 3: Delta tables, schema-enforced copy of raw
s3://silver/       ← Phase 3: Cleaned, deduped, SCD2 applied
s3://gold/         ← Phase 5/6: Business marts (Delta + 1 Iceberg table)
s3://quarantine/   ← Phase 4: Rows that failed DQ checks
```

### File Partitioning
Raw files land at: `s3://raw/claims/dt=2024-01-15/claims.json`

This is **Hive-style date partitioning**. The `dt=` prefix is a convention
Apache Spark recognises natively — it can read just one day's partition
without scanning the entire dataset (partition pruning).

**Interview answer when asked about partitioning:**
> "We partition by ingest date using Hive-style `dt=YYYY-MM-DD` prefixes.
> This means when we process today's data, Spark only reads today's partition,
> not the full history. For 500K daily claims at an insurer, this dramatically
> reduces the data scanned per job. In a real scenario I'd also consider
> partitioning claims by claim_date year/month for time-range queries."

---

## PART 5 — FILE FORMATS: CSV, JSON, PARQUET, DELTA, ICEBERG

### Raw Zone — CSV and JSON
- **Customers and Policies → CSV**: tabular, no nested fields, easy to inspect
- **Claims → JSON Lines**: demonstrates format diversity; real claim systems
  often output JSON with nested arrays (coverage details, sub-claims)

### Bronze and Silver — Delta Lake
Delta Lake is a storage layer built on top of Parquet files. It adds:

1. **ACID transactions** — multiple writers don't corrupt the table
2. **Transaction log** (`_delta_log/`)— every change recorded as a JSON entry
3. **Time travel** — `SELECT * FROM table VERSION AS OF 3` to read old data
4. **Schema enforcement** — rejects rows with wrong types on write
5. **Schema evolution** — can add columns without rewriting history
6. **Upsert (MERGE)** — critical for SCD Type 2 (Phase 3)

**Interview answer when asked "Why Delta Lake over plain Parquet?":**
> "Plain Parquet is immutable — you can't update or delete rows. Insurance
> data constantly changes: claims get closed, policies get amended, fraudulent
> claims get flagged retrospectively. Delta Lake's MERGE operation lets us
> apply these changes correctly. The transaction log also gives us auditability,
> which is a regulatory requirement in insurance — we can prove exactly what
> the data looked like on any given date."

### Gold — Apache Iceberg (one table, demo)
Iceberg is Delta Lake's main competitor. Key differences:

| Feature | Delta Lake | Apache Iceberg |
|---|---|---|
| Creator | Databricks | Netflix |
| Spec | Open | Open |
| Catalog | Needs Delta catalog or metastore | Hive/Glue/Nessie/REST |
| Hidden partitioning | No | Yes |
| Partition evolution | Limited | First-class |
| Engine support | Spark, Flink, Trino | Spark, Flink, Trino, Dremio |

We use Iceberg on one Gold table to show **format awareness**. In an interview,
this demonstrates you understand both options and can argue trade-offs.

---

## PART 6 — COMPUTE: APACHE SPARK / PYSPARK

### What Spark does in this project
1. Reads raw CSV/JSON from MinIO
2. Applies schema (casts string columns to proper types)
3. Writes Delta tables to Bronze
4. Deduplicates records
5. Applies SCD Type 2 logic on policies (Silver)
6. Uses MERGE (upsert) to write incrementally

### Why Spark instead of pandas or dbt for Bronze/Silver?
- **Scale**: pandas loads everything into RAM. Spark distributes across multiple
  cores/nodes. Our 500K claims dataset is manageable in pandas, but a real
  insurer has millions of claims per day.
- **Delta MERGE**: pandas has no native Delta support. Spark + delta-spark
  package gives us the MERGE INTO statement needed for SCD2.
- **Ecosystem**: Spark is the industry standard at every major Canadian bank
  and insurer. It's on every DE job description.

### Key Spark Concepts for Interviews

**Lazy evaluation**: Spark doesn't execute transformations immediately.
It builds a DAG (Directed Acyclic Graph) of operations and executes
only when you call an action (`.write()`, `.count()`, `.show()`).
This allows Spark to optimise the whole plan before running.

**Partitions**: Spark breaks data into partitions processed in parallel.
For our 500K claims, Spark might create 8 partitions (one per CPU core).
Each partition is processed independently.

**Driver vs Executor**: The Driver node coordinates the job and holds
the DAG. Executor nodes do the actual data processing. In our local
Docker setup, both run in the same container (LocalMode).

**Catalyst Optimizer**: Spark's query optimizer rewrites your PySpark
code into the most efficient execution plan — similar to how a SQL
database's query planner works.

---

## PART 7 — SCD TYPE 2 (CRITICAL CONCEPT FOR INSURANCE)

### What is Slowly Changing Dimension Type 2?

A technique for tracking historical changes in dimension tables.
Instead of overwriting old values, we keep every version of a record
with validity dates.

### Why SCD2 on Policies?

Insurance policies change over time:
- Customer moves province → new postal code
- Coverage amount increased → new coverage_amount
- Policy renewed → new end_date
- Status changes active → cancelled

Without SCD2, if you overwrite the record, you lose history.
With SCD2, you can answer: "What was this policy's coverage amount
when the claim was filed on March 15, 2022?"

### SCD2 Table Structure

```
policy_id  | coverage | status  | effective_start | effective_end | is_current
-----------+----------+---------+-----------------+---------------+-----------
POL_001    | 50,000   | active  | 2020-01-01      | 2022-03-14    | false
POL_001    | 75,000   | active  | 2022-03-15      | 9999-12-31    | true
```

### SCD2 Implementation Logic (MERGE statement)

```sql
MERGE INTO silver_policies AS target
USING new_data AS source ON target.policy_id = source.policy_id
  AND target.is_current = true

WHEN MATCHED AND (target.coverage_amount != source.coverage_amount
                  OR target.status != source.status) THEN
  -- Close the old record
  UPDATE SET effective_end = current_date(), is_current = false

WHEN NOT MATCHED THEN
  -- Insert new record (both genuinely new and "changed" records)
  INSERT (policy_id, coverage_amount, ..., effective_start,
          effective_end, is_current)
  VALUES (source.policy_id, source.coverage_amount, ...,
          current_date(), '9999-12-31', true)
```

**Interview answer when asked "How does SCD2 work in your project?":**
> "In my pipeline, whenever a policy record arrives in Silver that has
> different coverage or status from what we have, I close the existing
> record by setting effective_end to today and is_current to false.
> Then I insert a new row representing the current state with
> effective_end = 9999-12-31. Downstream dbt models join to the policy
> dimension using: WHERE is_current = true AND claim_date BETWEEN
> effective_start AND effective_end — this lets us correctly reconstruct
> what the policy looked like at the exact time of each claim."

---

## PART 8 — ORCHESTRATION: APACHE AIRFLOW

### What Airflow does
Airflow is a workflow orchestrator — it schedules and monitors data pipeline
jobs. Think of it as a smart cron that knows about dependencies, can retry
failed tasks, and shows you a UI with run history.

### Key Airflow Concepts

**DAG (Directed Acyclic Graph)**: A Python file that defines a pipeline.
"Directed" = tasks have order. "Acyclic" = no circular dependencies.

**Operator**: A building block for a task.
- `PythonOperator` — runs a Python function (we use this for the generator)
- `BashOperator` — runs a shell command
- `SparkSubmitOperator` — submits a Spark job (Phase 3)

**TaskInstance**: One execution of one task in one DAG run.

**Execution Date vs Run Date**: Confusing but important.
Execution date is the *logical* date the DAG is processing (e.g., yesterday).
Run date is when Airflow actually ran it (e.g., today at 2am).

**Backfill**: Running a DAG for historical dates it missed.
`airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag`

### Our DAGs

**DAG 01 — `01_generate_and_land`**
- Schedule: daily
- Task: `generate_and_land` (PythonOperator)
- Passes `ingest_date={{ ds }}` so each run knows its date
- Retries: 2, delay: 5 minutes

**DAG 02 — `02_bronze_silver`** (Phase 3)
- Depends on DAG 01 completing successfully
- Tasks: bronze_ingest → run_dq → silver_transform
- Has SLA and Slack failure alert

### Why Airflow over cron?
> "Cron has no retry logic, no visibility into failures, no dependency
> management between jobs, and no way to pass parameters between tasks.
> With Airflow, when the Silver job fails at 3am, I see it in the UI,
> I see which specific task failed and why, and the scheduler automatically
> retries it. For a production pipeline at an insurer where business teams
> expect fresh data every morning, that observability is essential."

---

## PART 9 — DATA QUALITY: GREAT EXPECTATIONS

### What Great Expectations does
It lets you write assertions (called "expectations") about your data and
run them as part of your pipeline. Failed rows get quarantined; you get
an HTML report showing exactly what failed.

### 15+ Expectations We Write (Phase 4)

**On Customers:**
1. `expect_column_values_to_not_be_null` on customer_id
2. `expect_column_values_to_be_unique` on customer_id
3. `expect_column_values_to_match_regex` on postal_code: `^[A-Z]\d[A-Z] \d[A-Z]\d$`
4. `expect_column_values_to_be_in_set` on province: [all 13 Canadian provinces]

**On Policies:**
5. `expect_column_values_to_not_be_null` on policy_id
6. `expect_column_values_to_be_unique` on policy_id
7. `expect_column_values_to_be_between` on coverage_amount: min=0
8. `expect_column_values_to_be_between` on premium: min=0
9. Custom: end_date > start_date

**On Claims:**
10. `expect_column_values_to_not_be_null` on claim_id
11. `expect_column_values_to_be_unique` on claim_id
12. `expect_column_values_to_be_between` on claim_amount: min=0
13. Custom: loss_date <= claim_date (business rule)
14. `expect_column_values_to_be_in_set` on claim_type
15. Referential: claim.policy_id must exist in policies table

### Quarantine Pattern
Rows failing DQ checks go to `s3://quarantine/` instead of being
loaded into Silver. The pipeline continues — bad rows are isolated,
not lost. This is important for interviews:

> "We don't stop the pipeline when data quality fails — we quarantine the
> bad rows and let the good data flow through. The data team gets an HTML
> report showing exactly which rows failed which checks. This is the
> pattern used in production: fail fast on systemic issues (like a schema
> change from the source system), but tolerate individual bad rows by
> quarantining them."

---

## PART 10 — TRANSFORMATION: dbt

### What dbt does
dbt (data build tool) manages SQL transformations. You write SELECT
statements; dbt compiles them, runs them in the right order, tests them,
and generates documentation.

### Why dbt instead of writing SQL directly?
- **Version control**: SQL models are `.sql` files in Git
- **Dependency management**: `ref('model_name')` lets dbt figure out run order
- **Built-in testing**: not_null, unique, accepted_values, relationships
- **Documentation**: auto-generated data catalog with lineage graph
- **Modularity**: staging → intermediate → marts layers

### Our dbt Models (Phase 5)

**Staging (views, no materialisation cost):**
- `stg_customers` — clean column names, type casts
- `stg_policies` — clean column names
- `stg_claims` — clean column names

**Marts (tables, pre-computed for BI):**

`mart_loss_ratio_by_product`
```sql
-- Loss Ratio = Total Claims Paid / Total Premiums Earned
SELECT
    product_type,
    SUM(claim_amount) AS total_claims,
    SUM(premium)      AS total_premium,
    SUM(claim_amount) / NULLIF(SUM(premium), 0) AS loss_ratio
FROM ...
GROUP BY product_type
```

`mart_fraud_summary`
```sql
SELECT
    province,
    claim_type,
    COUNT(*) FILTER (WHERE fraud_flag) AS fraud_count,
    COUNT(*) AS total_claims,
    AVG(claim_amount) FILTER (WHERE fraud_flag) AS avg_fraud_amount
FROM ...
GROUP BY province, claim_type
```

`mart_claims_by_province_month`
— Monthly claims volume and average amount by Canadian province

`dim_policy_scd2`
— Slowly changing dimension exposing all policy versions with validity dates

### dbt Profiles: dev vs prod
- **dev**: `dbt-duckdb` — runs locally, no cloud needed, fast iteration
- **prod**: `dbt-snowflake` — runs against real Snowflake warehouse

**Interview answer when asked "How do you manage dev vs prod in dbt?":**
> "I use dbt profiles. In dev, I target DuckDB so I can run `dbt build`
> locally in under a minute and iterate on model logic without touching
> production. In prod, I point at Snowflake. The models are identical —
> only the connection changes. I also use the `{{ target.name }}` variable
> in models to apply row limits in dev: `WHERE 1=1 {{ limit_dev() }}`."

---

## PART 11 — CLOUD WAREHOUSE: SNOWFLAKE

### Why Snowflake?
- Separates compute from storage (pay per query, not per GB stored)
- Instant scaling (warehouse sizes XS to 4XL)
- Native semi-structured data support (VARIANT column for JSON)
- Strong Canadian data residency options (AWS ca-central-1)
- Industry standard at Canadian financial institutions

### Our Snowflake Setup (Terraform-provisioned)
```
INSURANCE_DW (database)
├── RAW        (schema) ← external tables over MinIO Silver layer
├── STAGING    (schema) ← dbt staging models (views)
└── MARTS      (schema) ← dbt mart models (tables)

Roles:
├── TRANSFORMER ← dbt service account, SELECT on RAW, CREATE on STAGING/MARTS
└── ANALYST     ← Metabase service account, SELECT on MARTS only

Warehouse: COMPUTE_WH (X-Small, auto-suspend 60s)
```

### What is Terraform doing here?
Instead of clicking through the Snowflake UI to create databases/roles/
warehouses, we define them in HCL (HashiCorp Configuration Language) and
run `terraform apply`. Benefits:
- Reproducible — same infrastructure every time
- Version-controlled — infrastructure changes go through code review
- Auditable — `terraform plan` shows exactly what will change

---

## PART 12 — CI/CD: GITHUB ACTIONS

### What runs on every push to `main`

**Job 1 — lint-and-test:**
- `ruff check .` — catches unused imports, bad style
- `black --check .` — enforces consistent formatting
- `pytest tests/` — runs all 33+ unit tests

**Job 2 — dbt-parse:**
- `dbt parse` — catches SQL syntax errors without a live database

**Job 3 — terraform-validate:**
- `terraform validate` — ensures IaC HCL is syntactically correct

### Why this matters in interviews
> "Every commit to main triggers CI. If a developer introduces a bad SQL
> model or breaks a test, the PR is blocked before it can merge. In a
> team of 5+ data engineers, this prevents 'works on my machine' problems.
> The dbt parse step is particularly useful — it catches Jinja template
> errors and missing model references without needing a Snowflake connection,
> so CI stays fast and cheap."

---

## PART 13 — KEY NUMBERS TO REMEMBER

| Metric | Value |
|---|---|
| Customers | 50,000 |
| Policies | 100,000 |
| Claims | 500,000 |
| Claims file size | ~106 MB (JSON lines) |
| Dirty row rate | ~5% per entity |
| Fraud rate in data | ~3% of claims |
| Adjusters | 200 |
| DQ expectations | 15+ |
| Unit tests | 33 (Phase 2 alone) |
| Docker services | 6 (MinIO, minio-init, Postgres, Airflow × 3) |
| dbt mart models | 4 |
| S3 buckets | 5 (raw, bronze, silver, gold, quarantine) |

---

## PART 14 — LIKELY INTERVIEW QUESTIONS + ANSWERS

### Q1: "Walk me through your project."

> "I built an end-to-end insurance claims data lakehouse on Docker.
> The pipeline starts with a Faker-based Python generator that creates
> 500K synthetic claims, 100K policies, and 50K customers — all with
> realistic dirty data built in so the quality layer has real work to do.
>
> Airflow orchestrates the pipeline daily. The raw CSV and JSON files land
> in MinIO — our local S3. PySpark reads them and writes schema-enforced
> Delta Lake tables to the Bronze layer. Between Bronze and Silver, Great
> Expectations runs 15+ validations and quarantines bad rows. Silver applies
> deduplication and SCD Type 2 on the policies table to track coverage changes.
>
> dbt then transforms Silver data into business marts in Snowflake — loss ratio
> by product type, fraud summary by province, and a full policy history
> dimension. Metabase connects to Snowflake for the dashboard. GitHub Actions
> runs lint, tests, and dbt parse on every commit."

### Q2: "Why did you choose Delta Lake over Apache Parquet?"

> "Plain Parquet is immutable — you can write new files but you can't update
> or delete rows. Insurance data constantly changes: claims get closed or
> denied, policies get amended, fraud flags get added retrospectively.
> Delta Lake adds ACID transactions and a MERGE operation on top of Parquet,
> which is exactly what SCD Type 2 needs. It also gives us time travel —
> we can query what the Silver table looked like six months ago, which is
> valuable for auditing and debugging. The transaction log is essentially
> a free audit trail."

### Q3: "What is SCD Type 2 and why does insurance need it?"

> "SCD Type 2 is a technique for tracking the full history of a slowly
> changing dimension. Instead of overwriting a policy record when coverage
> changes, we close the old record — setting effective_end to today — and
> insert a new record with the new coverage and effective_start = today.
>
> In insurance, this is critical because claims are often filed months after
> the loss event. When we calculate loss ratios or adjudicate a claim, we
> need to know what the policy terms were at the time of the loss, not what
> they are today. Without SCD2, an insurer can't correctly answer 'what was
> the coverage limit when this claim was filed?' — which is a compliance issue."

### Q4: "How do you handle data quality failures in your pipeline?"

> "I use a quarantine pattern. Great Expectations runs between Bronze and
> Silver. Rows that fail any expectation — negative claim amounts, null PKs,
> malformed postal codes, loss_date after claim_date — get written to a
> separate quarantine bucket in MinIO with a failure reason column attached.
> The pipeline continues with clean rows.
>
> The data team gets an HTML report from Great Expectations showing exactly
> which expectations failed, the failure percentage, and sample bad rows.
> Airflow marks the DQ task as success if the failure rate is under a
> threshold (say 5%), and failed if it exceeds it — triggering a Slack alert."

### Q5: "What would you do differently if this were a real production system?"

> "Three main things. First, I'd replace MinIO with AWS S3 and Airflow
> on MWAA, removing the operational overhead of running these services.
> Second, I'd add Apache Kafka in front of the pipeline for real-time
> claims ingestion — high-severity claims should be visible within seconds,
> not next morning. Third, I'd add OpenLineage for column-level data lineage
> so we can trace any metric in Metabase back through every transformation
> to the source record. In a regulated industry, lineage is often a
> compliance requirement."

### Q6: "How do you make your pipeline idempotent?"

> "Two mechanisms. First, all PySpark write operations use
> `mode('overwrite')` with partition-level granularity — re-running a
> day's job overwrites only that day's partition, not the entire table.
> Second, the SCD2 MERGE in Silver is inherently idempotent — if the
> source data hasn't changed, the MERGE finds no matching rows to update
> and no new rows to insert, so running it twice produces the same result.
> I also test this in CI by running the Bronze job twice against the same
> input and asserting row counts don't double."

### Q7: "What's the difference between dbt-duckdb and dbt-snowflake in your project?"

> "They're two profiles for the same dbt project. When I'm developing a
> new mart model, I run `dbt build --target dev` which executes against
> a local DuckDB file. DuckDB reads Parquet and Delta files directly from
> disk without a running server. A full dbt build takes under 60 seconds.
> When I'm satisfied with the logic, `dbt build --target prod` runs against
> Snowflake. The SQL is identical — only the connection and warehouse change.
> This pattern keeps development fast and free while production runs on
> a properly governed, scalable warehouse."

### Q8: "How do you handle schema evolution?"

> "Delta Lake has built-in schema evolution. If a new column appears in
> the source data, I can add `mergeSchema=true` to the Spark write options
> and Delta will automatically add the column to the existing table without
> rewriting historical partitions. For breaking changes — like a column
> being renamed or its type changing — I treat that as a pipeline version
> bump: I update the Bronze schema, run a full backfill from raw, and
> add a dbt test to verify the new column is populated."

### Q9: "Why Terraform for Snowflake setup?"

> "Because clicking through a UI doesn't scale and isn't auditable. With
> Terraform, our entire Snowflake setup — databases, schemas, roles,
> grants, warehouse size — is defined in version-controlled HCL files.
> If a new data engineer joins and needs to set up a dev Snowflake account,
> they run `terraform apply` and have an identical environment in minutes.
> If someone accidentally drops the MARTS schema, we can recreate it with
> one command. And every change to infrastructure goes through a PR with
> `terraform plan` output showing exactly what will change."

### Q10: "What is partitioning in your pipeline and why does it matter?"

> "I use Hive-style date partitioning throughout. Raw files land at paths
> like `s3://raw/claims/dt=2024-01-15/claims.json`. When PySpark reads this
> folder, it automatically infers `dt` as a partition column without reading
> any data files. If my Bronze job only needs to process today's data, Spark
> does partition pruning — it skips every other folder entirely.
>
> For a 500K-row synthetic dataset this is minor. But at a real insurer
> processing 2 million claims per day over 10 years, that's 7 billion rows.
> Without partitioning, every job would scan all 7 billion rows. With daily
> partitioning, a single day's job scans only 2 million."

---

## PART 15 — CONCEPTS CHEAT SHEET

### ACID Transactions
- **A**tomicity — either all writes succeed or none do
- **C**onsistency — data always moves from one valid state to another
- **I**solation — concurrent readers/writers don't see partial results
- **D**urability — committed data survives crashes

Delta Lake provides ACID on object storage (S3/MinIO) which normally
doesn't support transactions.

### CAP Theorem (if asked)
Distributed systems can guarantee at most 2 of:
- **C**onsistency — all nodes see the same data at the same time
- **A**vailability — every request gets a response
- **P**artition tolerance — system works despite network splits

Object stores like S3 sacrifice strong Consistency for Availability and
Partition tolerance (eventual consistency). Delta Lake's transaction log
layer adds Consistency back.

### ETL vs ELT
- **ETL** (Extract, Transform, Load): transform BEFORE loading to warehouse.
  Traditional approach, used in legacy DW (Informatica, SSIS).
- **ELT** (Extract, Load, Transform): load raw THEN transform inside warehouse.
  Modern approach — our project. Raw lands in Bronze, transformed by dbt in Snowflake.

ELT works because modern cloud warehouses (Snowflake, BigQuery) are powerful
enough to transform data after loading, and storage is cheap enough to keep raw.

### Data Lakehouse vs Data Lake vs Data Warehouse

| | Data Lake | Data Warehouse | Lakehouse |
|---|---|---|---|
| Storage format | Any (CSV, JSON, Parquet) | Proprietary | Open (Delta, Iceberg, Parquet) |
| ACID | No | Yes | Yes (via Delta/Iceberg) |
| BI tools | Limited | Native | Yes |
| Schema | On read | On write | Both |
| Cost | Low | High | Low-Medium |
| Examples | S3 + raw files | Redshift, Teradata | Databricks, this project |

### Spark Execution Modes
- **Local mode**: single machine, multiple threads. What we use in Docker.
- **Cluster mode**: Spark driver on cluster. Used in production (EMR, Databricks).
- **Client mode**: driver on your laptop, executors on cluster. Used for interactive development.

---

## PART 16 — RESUME BULLETS (draft, refine in Phase 9)

```
• Architected an end-to-end Insurance Claims Lakehouse processing 500K daily
  claims through a Medallion pipeline (Bronze → Silver → Gold) using PySpark 3.5,
  Delta Lake, and Apache Airflow 2.9 on Docker — demonstrating production-grade
  DE patterns on zero infrastructure spend.

• Implemented SCD Type 2 on a 100K-row insurance policy dimension using
  Delta Lake MERGE operations, enabling historically accurate loss ratio
  calculations and audit-ready claim adjudication.

• Built a data quality framework using Great Expectations with 15+ validations
  (null checks, Canadian postal code regex, referential integrity, date sanity)
  with automated quarantine of non-conforming records to S3.

• Delivered four business intelligence marts (loss ratio by product,
  fraud summary by province, claims volume by month) via dbt models running on
  Snowflake, accessible through a Metabase dashboard.

• Provisioned Snowflake data warehouse infrastructure (databases, schemas,
  RBAC roles, compute warehouse) using Terraform, replacing manual UI
  configuration with version-controlled IaC.
```

---

## PART 17 — CONVERTING THIS FILE TO PDF

### Option 1 — VS Code (easiest)
1. Open this file in VS Code
2. Install the "Markdown PDF" extension (yzane.markdown-pdf)
3. Right-click in the file → "Markdown PDF: Export (pdf)"

### Option 2 — Pandoc (best formatting)
```bash
# Install pandoc
brew install pandoc

# Install a PDF engine
brew install --cask basictex   # or: brew install wkhtmltopdf

# Convert
pandoc docs/portfolio/interview-prep-notes.md \
  -o docs/portfolio/interview-prep-notes.pdf \
  --pdf-engine=xelatex \
  -V geometry:margin=1in \
  -V fontsize=11pt
```

### Option 3 — Browser
1. Open this `.md` file in any Markdown viewer (GitHub, Typora, Obsidian)
2. File → Print → Save as PDF

---

*Last updated: Phase 2 complete (Phases 0–2 content covered in full)*
*Phases 3–9 concepts will be added as we build them*
