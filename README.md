# Insurance Claims Lakehouse

[![CI](https://github.com/emudamah0906/insurance-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/emudamah0906/insurance-lakehouse/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-00ADD8.svg)](https://delta.io/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-E25A1C.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.8.5-FF694B.svg)](https://www.getdbt.com/)

A **production-style, end-to-end Insurance Claims Lakehouse** built entirely on a local Docker stack — no paid cloud services required. Demonstrates a complete data engineering pipeline from synthetic data generation to a Metabase BI dashboard, covering the full medallion architecture.

---

## Architecture

```
[Faker Generator] ──► [MinIO: raw zone (CSV/JSON)]
                              │
                    ┌─────────▼─────────┐
                    │  Bronze (Delta)    │  ← PySpark, schema enforcement
                    └─────────┬─────────┘
                              │ Great Expectations DQ
                    ┌─────────▼─────────┐
                    │  Silver (Delta)    │  ← Dedup, SCD Type 2 on policies
                    └─────────┬─────────┘
                              │ dbt
               ┌──────────────▼──────────────┐
               │  Gold (Delta + 1 Iceberg)    │  ← Business marts
               └──────────────┬──────────────┘
                              │ dbt-snowflake
                    ┌─────────▼─────────┐
                    │    Snowflake DW    │  ← Presentation layer
                    └─────────┬─────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Metabase Dashboard│  ← 4 tiles + fraud drill-down
                    └───────────────────┘
```

> Architecture diagram (PNG): [`docs/architecture/`](docs/architecture/)

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Language | Python | 3.11 |
| Compute | Apache Spark / PySpark | 3.5.1 |
| Lakehouse format | Delta Lake | 3.2.0 |
| Lakehouse format | Apache Iceberg | 0.7.0 |
| Object storage | MinIO (S3-compatible) | 2024-04 |
| Orchestration | Apache Airflow | 2.9.3 |
| Transformation | dbt-core | 1.8.5 |
| Local dev warehouse | dbt-duckdb | 1.8.1 |
| Cloud warehouse | Snowflake + dbt-snowflake | 1.8.3 |
| Data quality | Great Expectations | 0.18.19 |
| BI | Metabase | latest |
| CI/CD | GitHub Actions | — |
| IaC | Terraform (Snowflake provider) | ~0.90 |
| Linting | ruff, black, sqlfluff | pinned |

---

## Quickstart

### Prerequisites
- Docker Desktop (≥ 12 GB RAM allocated)
- Python 3.11
- Git

```bash
# 1. Clone
git clone https://github.com/emudamah0906/insurance-lakehouse.git
cd insurance-lakehouse

# 2. Set up local Python env (for generator + tests)
make setup-venv
source .venv/bin/activate

# 3. Start all services
make up

# 4. Open UIs
#    MinIO Console → http://localhost:9001  (minioadmin / minioadmin123)
#    Airflow UI    → http://localhost:8080  (admin / admin)
```

---

## Project Structure

```
insurance-lakehouse/
├── airflow/            # DAGs and Airflow plugins
├── data_generator/     # Faker synthetic data scripts
├── spark_jobs/         # PySpark Bronze + Silver jobs
├── dbt/                # Gold marts (dbt-duckdb dev / dbt-snowflake prod)
├── great_expectations/ # DQ suites and checkpoints
├── terraform/          # Snowflake warehouse IaC
├── docker/             # Custom Dockerfiles
├── docs/               # Architecture diagrams, screenshots, DQ reports
├── tests/              # pytest unit tests
├── .github/workflows/  # GitHub Actions CI
├── docker-compose.yml
├── Makefile
└── README.md
```

---

## Phase Walkthrough

| Phase | Description | Status |
|---|---|---|
| 0 | Discovery & setup | ✅ |
| 1 | Repo foundation | ✅ |
| 2 | Synthetic data generator + raw landing | 🔲 |
| 3 | Bronze + Silver (PySpark + SCD2) | 🔲 |
| 4 | Data quality (Great Expectations) | 🔲 |
| 5 | Gold marts in Snowflake via dbt | 🔲 |
| 6 | Apache Iceberg demo table | 🔲 |
| 7 | Metabase dashboard | 🔲 |
| 8 | CI/CD + final polish | 🔲 |
| 9 | Portfolio assets | 🔲 |

---

## Screenshots

> _Coming in Phase 7_

---

## Resume Bullets

> _Coming in Phase 9_

---

## What I'd Do Next (with a real budget)

- Replace MinIO with AWS S3 + use EMR Serverless for Spark
- Add Apache Kafka for real-time claims streaming → Bronze append
- Deploy Airflow on MWAA (managed)
- Add column-level lineage with OpenLineage / Marquez
- Implement row-level security in Snowflake for adjuster data isolation
