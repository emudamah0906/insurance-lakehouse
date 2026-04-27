"""
Snowflake load job.

Reads Silver Delta tables from MinIO, writes to Snowflake INSURANCE_DW.STAGING
via the snowflake-connector-python pandas write_pandas API.

Each entity table is TRUNCATED then fully reloaded for the given ingest_date
partition. STAGING is intentionally a full-refresh view of Silver — dbt handles
the incremental logic in MARTS.

Usage:
  Airflow PythonOperator:  run_snowflake_load(ingest_date="2024-01-15")
  Standalone:              python spark_jobs/snowflake_load.py [--date YYYY-MM-DD]
"""
import argparse
import logging
import os
import sys
import warnings
from datetime import date

warnings.filterwarnings("ignore", category=UserWarning)

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

sys.path.insert(0, "/opt/airflow")
from spark_jobs.utils import SILVER_BASE, get_spark  # noqa: E402

logger = logging.getLogger(__name__)

# ── Snowflake connection config (from environment) ────────────────────────────

def _sf_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORMER"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "INSURANCE_DW"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        schema="STAGING",
        session_parameters={"QUERY_TAG": "airflow_snowflake_load"},
    )


# ── DDL helpers ───────────────────────────────────────────────────────────────

_STAGING_DDL = {
    "CUSTOMERS": """
        CREATE TABLE IF NOT EXISTS INSURANCE_DW.STAGING.CUSTOMERS (
            CUSTOMER_ID     VARCHAR,
            FIRST_NAME      VARCHAR,
            LAST_NAME       VARCHAR,
            DOB             DATE,
            PROVINCE        VARCHAR,
            POSTAL_CODE     VARCHAR,
            EMAIL           VARCHAR,
            PHONE           VARCHAR,
            CREATED_AT      DATE,
            _INGEST_DATE    DATE,
            _SOURCE_FILE    VARCHAR,
            EFFECTIVE_START DATE,
            EFFECTIVE_END   DATE,
            IS_CURRENT      BOOLEAN
        )
    """,
    "POLICIES": """
        CREATE TABLE IF NOT EXISTS INSURANCE_DW.STAGING.POLICIES (
            POLICY_ID        VARCHAR,
            CUSTOMER_ID      VARCHAR,
            PRODUCT_TYPE     VARCHAR,
            COVERAGE_AMOUNT  FLOAT,
            PREMIUM          FLOAT,
            START_DATE       DATE,
            END_DATE         DATE,
            STATUS           VARCHAR,
            _INGEST_DATE     DATE,
            _SOURCE_FILE     VARCHAR,
            EFFECTIVE_START  DATE,
            EFFECTIVE_END    DATE,
            IS_CURRENT       BOOLEAN
        )
    """,
    "CLAIMS": """
        CREATE TABLE IF NOT EXISTS INSURANCE_DW.STAGING.CLAIMS (
            CLAIM_ID      VARCHAR,
            POLICY_ID     VARCHAR,
            CLAIM_DATE    DATE,
            LOSS_DATE     DATE,
            CLAIM_AMOUNT  FLOAT,
            CLAIM_STATUS  VARCHAR,
            CLAIM_TYPE    VARCHAR,
            ADJUSTER_ID   VARCHAR,
            FRAUD_FLAG    BOOLEAN,
            _INGEST_DATE  DATE,
            _SOURCE_FILE  VARCHAR
        )
    """,
}


def _ensure_tables(conn):
    cur = conn.cursor()
    for ddl in _STAGING_DDL.values():
        cur.execute(ddl.strip())
    cur.close()


# ── Per-entity load ───────────────────────────────────────────────────────────

def _load_entity(spark, conn, entity: str, ingest_date: str) -> int:
    """Read Silver Delta for entity, upsert to Snowflake STAGING."""
    # Policies use SCD2 path
    silver_path = (f"{SILVER_BASE}/policies_scd2"
                   if entity == "policies"
                   else f"{SILVER_BASE}/{entity}")

    if entity == "policies":
        # SCD2 — only load current rows to keep STAGING simple
        df = (spark.read.format("delta").load(silver_path)
              .filter("is_current = true"))
    else:
        df = spark.read.format("delta").load(silver_path)

    pandas_df = df.toPandas()

    # Normalize column names to UPPERCASE for Snowflake
    pandas_df.columns = [c.upper() for c in pandas_df.columns]

    table_name = entity.upper()
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE IF EXISTS INSURANCE_DW.STAGING.{table_name}")
    cur.close()

    success, nchunks, nrows, _ = write_pandas(
        conn,
        pandas_df,
        table_name=table_name,
        schema="STAGING",
        database="INSURANCE_DW",
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
    )

    if not success:
        raise RuntimeError(f"write_pandas failed for {entity}")

    logger.info(f"[sf_load] {entity}: {nrows:,} rows loaded in {nchunks} chunk(s)")
    return nrows


# ── Entry point ───────────────────────────────────────────────────────────────

def run_snowflake_load(ingest_date: str | None = None) -> dict:
    """
    Load all Silver entities into Snowflake STAGING.
    Entry point for Airflow PythonOperator and __main__.
    """
    run_date = ingest_date or date.today().isoformat()
    logger.info(f"=== snowflake_load  ingest_date={run_date} ===")

    spark = get_spark("snowflake_load")
    spark.sparkContext.setLogLevel("WARN")
    conn = _sf_conn()

    try:
        _ensure_tables(conn)
        counts = {}
        for entity in ["customers", "policies", "claims"]:
            counts[entity] = _load_entity(spark, conn, entity, run_date)
        logger.info(f"=== snowflake_load complete: {counts} ===")
        return counts
    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    print(run_snowflake_load(args.date))
