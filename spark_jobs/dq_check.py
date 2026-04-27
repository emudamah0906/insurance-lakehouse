"""
Data quality check job.

1. Reads Bronze Delta tables into Pandas for Great Expectations validation.
2. Quarantines bad PySpark rows to s3a://quarantine/ using business-rule filters.
3. Writes a _dq_failure_reason column so downstream analysts can see why a row failed.

Idempotency: quarantine writes use replaceWhere on the _ingest_date partition.

Usage:
  Called by Airflow PythonOperator:  run_dq(ingest_date="2024-01-15")
  Standalone:                        python spark_jobs/dq_check.py [--date YYYY-MM-DD]
"""
import argparse
import logging
import sys
from datetime import date

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, to_date, when
from pyspark.sql.types import DateType

sys.path.insert(0, "/opt/airflow")
from spark_jobs.utils import (  # noqa: E402
    BRONZE_BASE,
    QUARANTINE_BASE,
    get_spark,
)

logger = logging.getLogger(__name__)

GE_CONTEXT_ROOT = "/opt/airflow/great_expectations"


# ── Great Expectations helpers ────────────────────────────────────────────────

def _get_ge_context():
    return gx.get_context(context_root_dir=GE_CONTEXT_ROOT)


def _add_customer_expectations(validator):
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_unique("customer_id")
    validator.expect_column_values_to_match_regex(
        "postal_code",
        r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$",
        mostly=0.9,
    )
    validator.expect_column_values_to_be_in_set(
        "province",
        ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"],
        mostly=0.95,
    )
    validator.expect_column_values_to_not_be_null("email", mostly=0.95)


def _add_policy_expectations(validator):
    validator.expect_column_values_to_not_be_null("policy_id")
    validator.expect_column_values_to_be_unique("policy_id")
    validator.expect_column_values_to_not_be_null("customer_id")
    validator.expect_column_values_to_be_between("coverage_amount", min_value=0, mostly=0.98)
    validator.expect_column_values_to_be_between("premium", min_value=0, mostly=0.98)
    validator.expect_column_values_to_be_in_set(
        "product_type",
        ["auto", "home", "life", "health", "commercial"],
        mostly=0.98,
    )


def _add_claim_expectations(validator):
    validator.expect_column_values_to_not_be_null("claim_id")
    validator.expect_column_values_to_be_unique("claim_id")
    validator.expect_column_values_to_not_be_null("policy_id")
    validator.expect_column_values_to_be_between("claim_amount", min_value=0, mostly=0.98)
    validator.expect_column_values_to_be_in_set(
        "claim_type",
        ["collision", "fire", "theft", "flood", "liability", "medical", "other"],
        mostly=0.98,
    )
    validator.expect_column_values_to_be_in_set(
        "claim_status",
        ["open", "closed", "pending", "denied"],
        mostly=0.98,
    )


def _run_ge_validation(entity: str, pandas_df, ingest_date: str) -> bool:
    """
    Run GE validation for one entity via the validator directly.
    Avoids serializing in-memory batch_data to the checkpoint store.
    Returns True if all expectations pass.
    """
    context = _get_ge_context()
    suite_name = f"{entity}_suite"

    # Recreate the suite fresh each run
    try:
        context.delete_expectation_suite(suite_name)
    except Exception:
        pass
    suite = context.add_expectation_suite(expectation_suite_name=suite_name)

    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name=entity,
        runtime_parameters={"batch_data": pandas_df},
        batch_identifiers={"run_id": ingest_date, "ingest_date": ingest_date},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    if entity == "customers":
        _add_customer_expectations(validator)
    elif entity == "policies":
        _add_policy_expectations(validator)
    elif entity == "claims":
        _add_claim_expectations(validator)

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Validate directly — avoids serialising the DataFrame to checkpoint store
    validation_result = validator.validate()

    # Persist HTML data docs so they're viewable at great_expectations/data_docs/
    context.build_data_docs()

    passed = validation_result.success
    logger.info(f"[dq] GE validation entity={entity} passed={passed}")
    return passed


# ── PySpark quarantine logic ──────────────────────────────────────────────────

def _split_by_rules(df: DataFrame, rules: list[tuple]) -> tuple[DataFrame, DataFrame]:
    """
    rules: list of (column_expression, failure_label) tuples.
    Returns (clean_df, dirty_df). dirty_df has _dq_failure_reason column.
    """
    failure_exprs = [
        when(cond, lit(label)) for cond, label in rules
    ]
    reason_col = concat_ws("|", *failure_exprs)
    labeled = df.withColumn("_dq_failure_reason", reason_col)
    dirty = labeled.filter(col("_dq_failure_reason") != "")
    clean = labeled.filter(col("_dq_failure_reason") == "").drop("_dq_failure_reason")
    return clean, dirty


def _quarantine_customers(df: DataFrame):
    rules = [
        (col("customer_id").isNull(), "null_customer_id"),
        (col("province").isNull(), "null_province"),
        (
            col("postal_code").isNull() |
            ~col("postal_code").rlike(r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$"),
            "invalid_postal_code",
        ),
    ]
    return _split_by_rules(df, rules)


def _quarantine_policies(df: DataFrame):
    rules = [
        (col("policy_id").isNull(), "null_policy_id"),
        (col("customer_id").isNull(), "null_customer_id"),
        (col("coverage_amount").isNull() | (col("coverage_amount") <= 0), "invalid_coverage_amount"),
        (col("premium").isNull() | (col("premium") <= 0), "invalid_premium"),
    ]
    return _split_by_rules(df, rules)


def _quarantine_claims(df: DataFrame):
    rules = [
        (col("claim_id").isNull(), "null_claim_id"),
        (col("policy_id").isNull(), "null_policy_id"),
        (col("claim_amount").isNull() | (col("claim_amount") <= 0), "invalid_claim_amount"),
        (
            to_date(col("loss_date")).isNotNull() &
            to_date(col("claim_date")).isNotNull() &
            (to_date(col("loss_date")) > to_date(col("claim_date"))),
            "loss_date_after_claim_date",
        ),
    ]
    return _split_by_rules(df, rules)


def _write_quarantine(dirty_df: DataFrame, entity: str, ingest_date: str) -> int:
    if dirty_df is None:
        return 0
    count = dirty_df.count()
    if count == 0:
        logger.info(f"[dq] no quarantine rows for {entity}")
        return 0
    path = f"{QUARANTINE_BASE}/{entity}"
    (dirty_df
     .withColumn("_ingest_date", lit(ingest_date).cast(DateType()))
     .write
     .format("delta")
     .mode("overwrite")
     .option("replaceWhere", f"_ingest_date = '{ingest_date}'")
     .partitionBy("_ingest_date")
     .save(path))
    logger.info(f"[dq] quarantined {count:,} rows → {path}")
    return count


# ── Entry point ───────────────────────────────────────────────────────────────

def run_dq(ingest_date: str | None = None) -> dict:
    """
    Run data quality checks for all three entities.
    Entry point for Airflow PythonOperator and __main__.
    """
    run_date = ingest_date or date.today().isoformat()
    logger.info(f"=== dq_check  ingest_date={run_date} ===")

    spark = get_spark("dq_check")
    spark.sparkContext.setLogLevel("WARN")

    results = {}

    try:
        for entity, quarantine_fn in [
            ("customers", _quarantine_customers),
            ("policies",  _quarantine_policies),
            ("claims",    _quarantine_claims),
        ]:
            bronze_path = f"{BRONZE_BASE}/{entity}"
            logger.info(f"[dq] reading bronze {bronze_path} partition dt={run_date}")

            spark_df = (spark.read
                        .format("delta")
                        .load(bronze_path)
                        .filter(col("_ingest_date") == run_date))

            pandas_df = spark_df.toPandas()
            logger.info(f"[dq] {entity}: {len(pandas_df):,} rows loaded for GE")

            ge_passed = _run_ge_validation(entity, pandas_df, run_date)
            clean_df, dirty_df = quarantine_fn(spark_df)
            q_count = _write_quarantine(dirty_df, entity, run_date)

            results[entity] = {
                "ge_passed":       ge_passed,
                "total_rows":      len(pandas_df),
                "quarantine_rows": q_count,
                "clean_rows":      len(pandas_df) - q_count,
            }

        logger.info(f"=== dq_check complete: {results} ===")
        return results

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat(),
                        help="Ingest date YYYY-MM-DD (default: today)")
    args = parser.parse_args()
    print(run_dq(args.date))
