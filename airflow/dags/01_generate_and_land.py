"""
DAG 01 — Generate synthetic insurance data and land in MinIO raw zone.

Schedule: daily  |  No catchup  |  Retries: 2
Output:
  s3://raw/customers/dt={ds}/customers.csv
  s3://raw/policies/dt={ds}/policies.csv
  s3://raw/claims/dt={ds}/claims.json
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# data_generator is mounted at /opt/airflow/data_generator via docker-compose volume
sys.path.insert(0, "/opt/airflow")
from data_generator.generate import generate_and_land  # noqa: E402

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="01_generate_and_land",
    description="Generate synthetic insurance data → MinIO raw zone",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["ingestion", "phase-2", "raw"],
    doc_md="""
## 01_generate_and_land

Generates synthetic insurance data using Faker + NumPy and uploads to MinIO.

| Entity    | Rows   | Format | Path |
|-----------|--------|--------|------|
| customers | ~50 K  | CSV    | `s3://raw/customers/dt={ds}/` |
| policies  | ~100 K | CSV    | `s3://raw/policies/dt={ds}/`  |
| claims    | ~500 K | JSON lines | `s3://raw/claims/dt={ds}/` |

~5% of rows contain intentional dirty data (nulls, duplicates, invalid values)
to exercise the Great Expectations DQ layer in Phase 4.
    """,
) as dag:

    generate_task = PythonOperator(
        task_id="generate_and_land",
        python_callable=generate_and_land,
        op_kwargs={"ingest_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=45),
    )
