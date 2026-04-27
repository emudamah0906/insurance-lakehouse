"""
Silver transformation job.

Reads Bronze Delta tables, then:
  - Deduplicates on primary key
  - Casts string dates to DateType
  - Drops Bronze metadata columns (_source_file, _ingest_date)
  - Customers / Claims  → MERGE upsert into Silver (keep latest state)
  - Policies            → SCD Type 2 MERGE (track coverage / status history)

SCD2 logic (policies only):
  1. Detect records whose tracked columns changed vs the current Silver version.
  2. Close changed records: set is_current=False, effective_end=today.
  3. Append new current versions for changed + brand-new policies.

Idempotency:
  Running Silver for the same ingest_date twice produces the same result:
  the second run detects no changes and exits early for SCD2, and
  MERGE updateAll/insertAll is naturally idempotent for customers/claims.

Usage:
  Called by Airflow PythonOperator:  run_silver(ingest_date="2024-01-15")
  Standalone:                        python spark_jobs/silver_transform.py [--date YYYY-MM-DD]
"""

import argparse
import logging
import sys
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date, lit, to_date
from pyspark.sql.types import DateType

sys.path.insert(0, "/opt/airflow")
from spark_jobs.utils import BRONZE_BASE, SILVER_BASE, get_spark  # noqa: E402

logger = logging.getLogger(__name__)

# Columns tracked by SCD2 — any change here creates a new policy version
SCD2_TRACKED_COLS = ["coverage_amount", "premium", "status"]

FAR_FUTURE_DATE = "9999-12-31"  # cast to DateType() inside functions (needs active SparkContext)


# ── Bronze → Silver: type casting & cleanup ───────────────────────────────────


def _clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
        .withColumn("dob", to_date("dob", "yyyy-MM-dd"))
        .withColumn("created_at", to_date("created_at", "yyyy-MM-dd"))
        .drop("_source_file", "_ingest_date")
    )


def _clean_claims(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["claim_id"])
        .filter(col("claim_id").isNotNull())
        .withColumn("claim_date", to_date("claim_date", "yyyy-MM-dd"))
        .withColumn("loss_date", to_date("loss_date", "yyyy-MM-dd"))
        .drop("_source_file", "_ingest_date")
    )


def _clean_policies(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["policy_id"])
        .filter(col("policy_id").isNotNull())
        .withColumn("start_date", to_date("start_date", "yyyy-MM-dd"))
        .withColumn("end_date", to_date("end_date", "yyyy-MM-dd"))
        .drop("_source_file", "_ingest_date")
    )


# ── Upsert (MERGE) for customers and claims ───────────────────────────────────


def _upsert_silver(spark: SparkSession, source: DataFrame, path: str, pk: str):
    """
    MERGE source into Silver Delta table on primary key.
    - Matched → update all columns
    - Not matched → insert
    First run: plain overwrite (table doesn't exist yet).
    """
    from delta.tables import DeltaTable

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)
        (
            target.alias("tgt")
            .merge(source.alias("src"), f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"[silver] MERGE upsert → {path}")
    else:
        source.write.format("delta").mode("overwrite").save(path)
        logger.info(f"[silver] initial write → {path}")


# ── SCD Type 2 for policies ───────────────────────────────────────────────────


def _apply_scd2_policies(spark: SparkSession, source: DataFrame, path: str):
    """
    Two-step SCD Type 2:
      Step 1 — Close changed records (MERGE UPDATE is_current=False).
      Step 2 — Append new current versions for changed + new policies.

    First run: write all records as current with no prior history.
    """
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, path):
        # ── First load ──────────────────────────────────────────────────────
        (
            source.withColumn("effective_start", current_date())
            .withColumn("effective_end", lit(FAR_FUTURE_DATE).cast(DateType()))
            .withColumn("is_current", lit(True))
            .write.format("delta")
            .mode("overwrite")
            .save(path)
        )
        logger.info(f"[silver] SCD2 initial load → {path}")
        return

    target_dt = DeltaTable.forPath(spark, path)
    current_silver = target_dt.toDF().filter(col("is_current"))

    # ── Detect CHANGED records ──────────────────────────────────────────────
    # Join source to current Silver on policy_id; filter where any tracked
    # column differs. NULLs handled with eqNullSafe (<=>).
    change_filter = " OR ".join([f"NOT (src.{c} <=> tgt.{c})" for c in SCD2_TRACKED_COLS])

    changed = (
        source.alias("src")
        .join(current_silver.alias("tgt"), "policy_id")
        .filter(change_filter)
        .select("src.*")
    )

    # ── Detect NEW records (not in Silver at all) ───────────────────────────
    new_records = source.alias("src").join(
        target_dt.toDF().select("policy_id").distinct(), "policy_id", "left_anti"
    )

    rows_to_insert = changed.union(new_records)
    insert_count = rows_to_insert.count()

    if insert_count == 0:
        logger.info("[silver] SCD2: no changes detected — skipping")
        return

    logger.info(f"[silver] SCD2: {insert_count:,} rows to close/insert")

    # ── Step 1: Close changed records ──────────────────────────────────────
    (
        target_dt.alias("tgt")
        .merge(
            changed.select(col("policy_id").alias("src_policy_id")).alias("src"),
            "tgt.policy_id = src.src_policy_id AND tgt.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "is_current": lit(False),
                "effective_end": current_date(),
            }
        )
        .execute()
    )

    # ── Step 2: Append new current versions ────────────────────────────────
    (
        rows_to_insert.withColumn("effective_start", current_date())
        .withColumn("effective_end", lit(FAR_FUTURE_DATE).cast(DateType()))
        .withColumn("is_current", lit(True))
        .write.format("delta")
        .mode("append")
        .save(path)
    )

    logger.info(f"[silver] SCD2 complete → {path}")


# ── Orchestration entry point ─────────────────────────────────────────────────


def run_silver(ingest_date: str | None = None) -> dict:
    """
    Transform Bronze → Silver for all three entities.
    Entry point for Airflow PythonOperator and __main__.
    """
    run_date = ingest_date or date.today().isoformat()
    logger.info(f"=== silver_transform  ingest_date={run_date} ===")

    spark = get_spark("silver_transform")
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ── Read from Bronze (today's partition only) ───────────────────────
        bronze_customers = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/customers")
            .filter(col("_ingest_date") == run_date)
        )

        bronze_policies = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/policies")
            .filter(col("_ingest_date") == run_date)
        )

        bronze_claims = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/claims")
            .filter(col("_ingest_date") == run_date)
        )

        # ── Clean ───────────────────────────────────────────────────────────
        clean_customers = _clean_customers(bronze_customers)
        clean_policies = _clean_policies(bronze_policies)
        clean_claims = _clean_claims(bronze_claims)

        # ── Write to Silver ─────────────────────────────────────────────────
        _upsert_silver(spark, clean_customers, f"{SILVER_BASE}/customers", "customer_id")

        _apply_scd2_policies(spark, clean_policies, f"{SILVER_BASE}/policies_scd2")

        _upsert_silver(spark, clean_claims, f"{SILVER_BASE}/claims", "claim_id")

        counts = {
            "customers": clean_customers.count(),
            "policies": clean_policies.count(),
            "claims": clean_claims.count(),
        }
        logger.info(f"=== silver_transform complete: {counts} ===")
        return counts

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date", default=date.today().isoformat(), help="Ingest date YYYY-MM-DD (default: today)"
    )
    args = parser.parse_args()
    result = run_silver(args.date)
    print(result)
