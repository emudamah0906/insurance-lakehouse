# Insurance Claims Lakehouse

[![CI](https://github.com/emudamah0906/insurance-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/emudamah0906/insurance-lakehouse/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD8.svg)](https://delta.io/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt--snowflake-1.8.3-FF694B.svg)](https://www.getdbt.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A production-style, end-to-end Insurance Claims Lakehouse running on a single `docker compose up`. Covers the full medallion architecture from synthetic data generation to Metabase BI dashboards on Snowflake, with data quality gates, SCD Type 2 history, and a CI/CD pipeline.

**Built to demonstrate: Apache Spark, Delta Lake, Airflow, dbt, Snowflake, Great Expectations, Metabase, Terraform, and GitHub Actions — all wired together.**

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                    Local Docker Stack                            │
│                                                                  │
│  [Faker + NumPy]                                                 │
│       │  50K customers · 100K policies · 500K claims            │
│       ▼                                                          │
│  [MinIO]  s3a://raw/  (CSV / JSON, partitioned by dt=YYYY-MM-DD) │
│       │                                                          │
│       ▼  PySpark + Delta Lake (replaceWhere idempotency)        │
│  [Bronze]  s3a://bronze/  – schema enforcement + metadata cols  │
│       │                                                          │
│       ▼  Great Expectations V3 + PySpark quarantine             │
│  [Quarantine]  s3a://quarantine/  – bad rows + failure reason   │
│       │                                                          │
│       ▼  PySpark MERGE / SCD Type 2                             │
│  [Silver]  s3a://silver/  – deduped, typed, SCD2 on policies    │
│       │                                                          │
└───────┼──────────────────────────────────────────────────────────┘
        │  snowflake-connector write_pandas
        ▼
┌──────────────────────────────────────────────────────────────────┐
│               Snowflake  INSURANCE_DW                            │
│                                                                  │
│  STAGING.*   ← Silver mirror (full-refresh per run)             │
│       │                                                          │
│       ▼  dbt-snowflake (views + tables, 17/17 tests pass)       │
│  MARTS.mart_loss_ratio          – loss ratio by product/province │
│  MARTS.mart_claims_analysis     – monthly fraud + severity KPIs  │
│  MARTS.mart_customer_ltv        – net LTV + 5-tier segmentation  │
│  MARTS.mart_policy_performance  – per-policy risk flag           │
│       │                                                          │
│       ▼                                                          │
│  [Metabase]  4 dashboards × 4 charts  http://localhost:3000     │
└──────────────────────────────────────────────────────────────────┘

Orchestration: Apache Airflow 2.9.3 (LocalExecutor)
Pipeline:  bronze_ingest → dq_check → silver_transform → snowflake_load → dbt_run
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.12 |
| Compute | Apache Spark / PySpark | 3.5.1 |
| Table format | Delta Lake | 3.2.0 |
| Object storage | MinIO (S3-compatible) | 2024-04 |
| Orchestration | Apache Airflow | 2.9.3 |
| Data quality | Great Expectations | 0.18.19 |
| Cloud warehouse | Snowflake (ca-central-1) | — |
| Transformation | dbt-snowflake | 1.8.3 |
| BI | Metabase | 0.50.3 |
| IaC | Terraform + Snowflake provider | ~0.90 |
| CI/CD | GitHub Actions | — |
| Linting | ruff + black | pinned |

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Metabase | http://localhost:3000 ||

---

## Quickstart

**Prerequisites:** Docker Desktop (≥ 12 GB RAM), Python 3.12, Git

```bash
# 1. Clone
git clone https://github.com/emudamah0906/insurance-lakehouse.git
cd insurance-lakehouse

# 2. Copy credentials template and fill in Snowflake details
cp .env.example .env   # then edit SNOWFLAKE_PASSWORD

# 3. Set up local Python venv (for tests, dbt, linting)
make setup-venv && source .venv/bin/activate

# 4. Start all 5 services (MinIO, Postgres, Airflow, Metabase)
make up

# 5. Trigger the full pipeline manually
make dag-trigger DAG=01_generate_and_land   # generate raw data
make dag-trigger DAG=02_bronze_silver       # bronze→dq→silver→snowflake→dbt

# 6. Run CI checks locally
make ci-check
```

---

## Pipeline Deep Dive

### DAG 01 — `01_generate_and_land`
Runs daily at midnight UTC. Generates synthetic Canadian insurance data using Faker + NumPy (population-weighted provinces, valid postal codes, lognormal claim amounts) and uploads partitioned CSV/JSON to MinIO raw zone.

### DAG 02 — `02_bronze_silver` (5-task chain)

| Task | What it does |
|---|---|
| `bronze_ingest` | Reads raw CSV/JSON → enforces schema → writes Delta with `replaceWhere` |
| `dq_check` | Great Expectations validation + PySpark quarantine (bad rows tagged with `_dq_failure_reason`) |
| `silver_transform` | Dedup → type-cast dates → MERGE upsert (customers/claims) → SCD Type 2 (policies) |
| `snowflake_load` | Reads Silver Delta → `write_pandas` to `INSURANCE_DW.STAGING` |
| `dbt_run` | `dbt run` + `dbt test` (17/17 pass) building 4 Gold mart tables |

### SCD Type 2 on Policies
Two-step MERGE:
1. Close changed records: `is_current=False, effective_end=today`
2. Append new current versions for changed + brand-new policies

### Data Quality
- **GE expectations**: null/unique PKs, postal code regex, province set, positive amounts, valid enums, `loss_date ≤ claim_date`
- **Quarantine rules**: 19 bad rows caught on a 650K-row dataset
- **HTML data docs** auto-generated at `great_expectations/data_docs/local_site/`

### dbt Gold Marts

| Mart | Key metrics |
|---|---|
| `mart_loss_ratio` | Loss ratio, claims frequency %, avg severity — by product × province |
| `mart_claims_analysis` | Monthly fraud rate, avg severity, days-to-report — by claim type × product |
| `mart_customer_ltv` | Net LTV, tenure, platinum/gold/silver/bronze/unprofitable tiers |
| `mart_policy_performance` | Per-policy net profit, loss ratio, claims/year, risk flag |

---

## Project Structure

```
insurance-lakehouse/
├── airflow/dags/           # 01_generate_and_land.py, 02_bronze_silver.py
├── data_generator/         # Faker + NumPy synthetic data (generate.py)
├── spark_jobs/             # bronze_ingest, silver_transform, dq_check, snowflake_load, utils
├── dbt/
│   ├── models/staging/     # stg_customers, stg_policies, stg_claims + sources.yml
│   └── models/marts/       # 4 Gold mart tables + schema.yml tests
├── great_expectations/     # GE context + expectation suites (3 entities)
├── terraform/              # Snowflake IaC (warehouse, DB, schemas, role, grants)
├── docker/                 # Dockerfile.airflow (Java 17 + PySpark + GE + dbt + Snowflake)
├── tests/                  # 33 pytest unit tests (data generator)
├── .github/workflows/      # ci.yml (lint + pytest + dbt-parse + terraform-validate)
├── .env                    # Snowflake credentials (gitignored)
├── docker-compose.yml      # MinIO + Postgres + Airflow × 3 + Metabase
├── Makefile                # up, down, test, lint, format, ci-check, dbt-run, dbt-test
└── README.md
```

---

## Phase Status

| Phase | Description | Status |
|---|---|---|
| 0 | Repo scaffold, Docker stack, MinIO buckets | ✅ |
| 1 | Faker + NumPy data generator (50K/100K/500K) | ✅ |
| 2 | Airflow DAG wiring (generate → land) | ✅ |
| 3 | Bronze ingest + Silver (PySpark, Delta, SCD2) | ✅ |
| 4 | Data quality — Great Expectations + quarantine | ✅ |
| 5 | Snowflake + dbt Gold layer (4 marts, 17 tests) | ✅ |
| 6 | Metabase dashboards (4 × 4 charts) | ✅ |
| 7 | CI/CD + Terraform IaC + portfolio polish | ✅ |

---

## CI/CD

GitHub Actions (`.github/workflows/ci.yml`) runs on every push to `main`/`develop`:

| Job | What runs |
|---|---|
| `lint-and-test` | `ruff check` + `black --check` + `pytest` (33 tests) |
| `dbt-parse` | `dbt parse` with Snowflake adapter (validates SQL + Jinja, no DB connection) |
| `terraform-validate` | `terraform init -backend=false && terraform validate` |
| `docker-config` | `docker compose config --quiet` (YAML syntax check) |

