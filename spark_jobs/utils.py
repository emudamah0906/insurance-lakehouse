"""
Shared SparkSession factory, schemas, and S3 path constants.
Imported by both bronze_ingest.py and silver_transform.py.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# ── S3 paths ──────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or os.getenv(
    "MINIO_ENDPOINT", "http://localhost:9000"
)
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

RAW_BASE = "s3a://raw"
BRONZE_BASE = "s3a://bronze"
SILVER_BASE = "s3a://silver"
GOLD_BASE = "s3a://gold"
QUARANTINE_BASE = "s3a://quarantine"


# ── Raw schemas (keep dates as strings — dirty data has malformed values) ─────

RAW_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("province", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

RAW_POLICIES_SCHEMA = StructType(
    [
        StructField("policy_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("coverage_amount", DoubleType(), True),
        StructField("premium", DoubleType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

RAW_CLAIMS_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("policy_id", StringType(), True),
        StructField("claim_date", StringType(), True),
        StructField("loss_date", StringType(), True),
        StructField("claim_amount", DoubleType(), True),
        StructField("claim_status", StringType(), True),
        StructField("claim_type", StringType(), True),
        StructField("adjuster_id", StringType(), True),
        StructField("fraud_flag", BooleanType(), True),
    ]
)


# ── SparkSession factory ──────────────────────────────────────────────────────


def get_spark(app_name: str) -> SparkSession:
    """
    Return a SparkSession configured for:
    - Delta Lake (extensions + catalog)
    - MinIO via s3a (hadoop-aws JARs are baked into the Docker image)
    - Local mode with 4 shuffle partitions (right-sized for our dataset)
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Performance tuning for local Docker mode
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")
        # Suppress verbose INFO logs from Spark internals
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
