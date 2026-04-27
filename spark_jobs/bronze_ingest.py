"""
Bronze ingestion job.

Reads raw CSV/JSON from MinIO raw zone, enforces schema, adds metadata columns,
and writes Delta Lake tables to s3a://bronze/ partitioned by _ingest_date.

Idempotency: uses replaceWhere on the _ingest_date partition — re-running for
the same date overwrites only that partition, leaving all other dates intact.

Usage:
  Called by Airflow PythonOperator:  run_bronze(ingest_date="2024-01-15")
  Standalone:                        python spark_jobs/bronze_ingest.py [--date YYYY-MM-DD]
"""

import argparse
import logging
import sys
from datetime import date

from pyspark.sql.functions import input_file_name, lit
from pyspark.sql.types import DateType

sys.path.insert(0, "/opt/airflow")
from spark_jobs.utils import (  # noqa: E402
    BRONZE_BASE,
    RAW_BASE,
    RAW_CLAIMS_SCHEMA,
    RAW_CUSTOMERS_SCHEMA,
    RAW_POLICIES_SCHEMA,
    get_spark,
)

logger = logging.getLogger(__name__)


def _ingest_entity(spark, entity: str, fmt: str, schema, ingest_date: str) -> int:
    """
    Read one entity from raw zone, attach metadata, write to Bronze Delta table.
    Returns the row count written.
    """
    raw_path = f"{RAW_BASE}/{entity}/dt={ingest_date}/{entity}.{fmt}"
    bronze_path = f"{BRONZE_BASE}/{entity}"

    logger.info(f"[bronze] reading {raw_path}")

    if fmt == "csv":
        raw_df = (
            spark.read.option("header", "true").option("nullValue", "").schema(schema).csv(raw_path)
        )
    else:
        raw_df = spark.read.schema(schema).json(raw_path)

    bronze_df = raw_df.withColumn("_ingest_date", lit(ingest_date).cast(DateType())).withColumn(
        "_source_file", input_file_name()
    )

    row_count = bronze_df.count()

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_ingest_date = '{ingest_date}'")
        .partitionBy("_ingest_date")
        .save(bronze_path)
    )

    logger.info(f"[bronze] wrote {row_count:,} rows → {bronze_path}")
    return row_count


def run_bronze(ingest_date: str | None = None) -> dict:
    """
    Ingest all three entities into Bronze Delta tables.
    Entry point for Airflow PythonOperator and __main__.
    """
    run_date = ingest_date or date.today().isoformat()
    logger.info(f"=== bronze_ingest  ingest_date={run_date} ===")

    spark = get_spark("bronze_ingest")
    spark.sparkContext.setLogLevel("WARN")

    try:
        counts = {
            "customers": _ingest_entity(spark, "customers", "csv", RAW_CUSTOMERS_SCHEMA, run_date),
            "policies": _ingest_entity(spark, "policies", "csv", RAW_POLICIES_SCHEMA, run_date),
            "claims": _ingest_entity(spark, "claims", "json", RAW_CLAIMS_SCHEMA, run_date),
        }
        logger.info(f"=== bronze_ingest complete: {counts} ===")
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
    result = run_bronze(args.date)
    print(result)
