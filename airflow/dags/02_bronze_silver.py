"""
DAG 02 — Bronze → DQ → Silver layer transformation.

Task graph:
  bronze_ingest → dq_check → silver_transform

Features:
  - 2-hour SLA on the full DAG
  - 2 retries per task with 5-minute delay
  - Slack failure alert (configure SLACK_WEBHOOK_URL env var to activate)
  - Idempotent: safe to re-run for the same date
"""
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")
from spark_jobs.bronze_ingest import run_bronze      # noqa: E402
from spark_jobs.dq_check import run_dq               # noqa: E402
from spark_jobs.silver_transform import run_silver   # noqa: E402


# ── Slack failure callback (no-ops gracefully if webhook not configured) ───────

def _slack_failure(context):
    """Post a failure message to Slack when any task fails."""
    import os
    import urllib.request, json

    webhook = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook:
        return  # Slack not configured — skip silently

    task_id  = context["task_instance"].task_id
    dag_id   = context["dag"].dag_id
    exec_dt  = context["execution_date"]
    log_url  = context["task_instance"].log_url

    payload = json.dumps({
        "text": (
            f":red_circle: *{dag_id}* › `{task_id}` failed\n"
            f"Execution date: {exec_dt}\n"
            f"<{log_url}|View logs>"
        )
    }).encode()

    try:
        req = urllib.request.Request(
            webhook, data=payload,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # never let the alert crash the DAG


# ── DAG definition ─────────────────────────────────────────────────────────────

default_args = {
    "owner":           "data-engineering",
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
    "on_failure_callback": _slack_failure,
}

with DAG(
    dag_id="02_bronze_silver",
    description="Bronze ingestion + Silver transformation (dedup, type-cast, SCD2)",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    sla_miss_callback=_slack_failure,
    tags=["bronze", "silver", "dq", "spark", "phase-4"],
    doc_md="""
## 02_bronze_silver

Reads raw CSV/JSON from MinIO, writes schema-enforced Delta tables to Bronze,
validates quality with Great Expectations, quarantines bad rows, then promotes
clean data to Silver.

### Bronze
- Schema enforcement (raw string dates preserved for DQ filtering)
- Partition: `_ingest_date`
- Idempotent via `replaceWhere`

### DQ Check
- Great Expectations V3 validation suite per entity (HTML data docs)
- PySpark quarantine: bad rows → `s3a://quarantine/{entity}/dt=YYYY-MM-DD`
- `_dq_failure_reason` column records which rule(s) failed

### Silver
- Deduplication on primary key
- Date casting (string → DateType)
- Customers & Claims: MERGE upsert (keep latest state)
- Policies: SCD Type 2 — tracks coverage_amount, premium, status changes

### SLA
2 hours from scheduled start time.
""",
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingest",
        python_callable=run_bronze,
        op_kwargs={"ingest_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=60),
        sla=timedelta(hours=1),
    )

    dq_task = PythonOperator(
        task_id="dq_check",
        python_callable=run_dq,
        op_kwargs={"ingest_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=30),
        sla=timedelta(hours=1, minutes=30),
    )

    silver_task = PythonOperator(
        task_id="silver_transform",
        python_callable=run_silver,
        op_kwargs={"ingest_date": "{{ ds }}"},
        execution_timeout=timedelta(minutes=60),
        sla=timedelta(hours=2),
    )

    bronze_task >> dq_task >> silver_task
