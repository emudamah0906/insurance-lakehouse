"""
Synthetic insurance data generator.

Produces ~50K customers, ~100K policies, ~500K claims with ~5% intentional
dirty rows (nulls, duplicates, invalid values, malformed postal codes).
Uploads partitioned CSV/JSON files to MinIO raw zone.

Usage:
  python data_generator/generate.py              # full 500K run
  python data_generator/generate.py --sample     # 1K/2K/5K for quick local tests
"""
import argparse
import io
import json
import logging
import os
import random
from datetime import date
from typing import Optional

import boto3
import numpy as np
import pandas as pd
from botocore.client import Config
from faker import Faker

logger = logging.getLogger(__name__)

fake = Faker("en_CA")
Faker.seed(42)
np.random.seed(42)
random.seed(42)

# ── Runtime config ─────────────────────────────────────────────────────────────
# AWS_ENDPOINT_URL is set in docker-compose for Airflow containers.
# MINIO_ENDPOINT is used when running locally (make seed).
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
RAW_BUCKET = "raw"

# ── Reference data ─────────────────────────────────────────────────────────────

CANADIAN_PROVINCES = [
    "Ontario",
    "Quebec",
    "British Columbia",
    "Alberta",
    "Manitoba",
    "Saskatchewan",
    "Nova Scotia",
    "New Brunswick",
    "Newfoundland and Labrador",
    "Prince Edward Island",
    "Northwest Territories",
    "Yukon",
    "Nunavut",
]

# Population-proportional weights (sum to 1.0)
_PROVINCE_WEIGHTS = np.array(
    [0.380, 0.230, 0.130, 0.110, 0.040, 0.030, 0.030, 0.020, 0.015, 0.005, 0.003, 0.002, 0.001]
)
_PROVINCE_WEIGHTS /= _PROVINCE_WEIGHTS.sum()

# Canada Post: first letter of FSA (Forward Sortation Area) by province
_POSTAL_PREFIX: dict[str, list[str]] = {
    "Ontario": list("KLMNP"),
    "Quebec": list("GHJ"),
    "British Columbia": list("V"),
    "Alberta": list("T"),
    "Manitoba": list("R"),
    "Saskatchewan": list("S"),
    "Nova Scotia": list("B"),
    "New Brunswick": list("E"),
    "Newfoundland and Labrador": list("A"),
    "Prince Edward Island": list("C"),
    "Northwest Territories": list("X"),
    "Yukon": list("Y"),
    "Nunavut": list("X"),
}
_PC_LETTERS = list("ABCEGHJKLMNPRSTVWXYZ")  # valid Canada Post letters (no D,F,I,O,Q,U)
_PC_DIGITS = list("0123456789")

PRODUCT_TYPES = ["auto", "home", "life"]
_PRODUCT_WEIGHTS = np.array([0.50, 0.35, 0.15])

POLICY_STATUSES = ["active", "expired", "cancelled", "lapsed"]
_POLICY_STATUS_WEIGHTS = np.array([0.65, 0.20, 0.10, 0.05])

CLAIM_STATUSES = ["open", "closed", "pending", "denied", "settled"]
_CLAIM_STATUS_WEIGHTS = np.array([0.20, 0.40, 0.15, 0.10, 0.15])

CLAIM_TYPES = [
    "collision",
    "theft",
    "fire",
    "flood",
    "liability",
    "medical",
    "disability",
    "death_benefit",
    "property_damage",
    "vandalism",
]

DIRTY_RATE = 0.05  # ~5% dirty rows per entity


# ── Internal helpers ───────────────────────────────────────────────────────────

def _postal_code(province: str) -> str:
    """Return a valid-format Canadian postal code for the given province."""
    prefix = random.choice(_POSTAL_PREFIX.get(province, ["X"]))
    d, l = _PC_DIGITS, _PC_LETTERS
    return (
        f"{prefix}{random.choice(d)}{random.choice(l)}"
        f" {random.choice(d)}{random.choice(l)}{random.choice(d)}"
    )


def _random_dates(start: str, end: str, n: int) -> np.ndarray:
    """Return n random date strings (YYYY-MM-DD) between start and end."""
    start_s = int(pd.Timestamp(start).timestamp())
    end_s = int(pd.Timestamp(end).timestamp())
    seconds = np.random.randint(start_s, end_s, size=n)
    return pd.to_datetime(seconds, unit="s").strftime("%Y-%m-%d").values


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_customers(n: int = 50_000) -> pd.DataFrame:
    """Generate n synthetic Canadian insurance customers."""
    logger.info(f"Generating {n:,} customers...")
    provinces = np.random.choice(CANADIAN_PROVINCES, size=n, p=_PROVINCE_WEIGHTS)

    return pd.DataFrame(
        {
            "customer_id": [f"CUST_{i+1:06d}" for i in range(n)],
            "first_name": [fake.first_name() for _ in range(n)],
            "last_name": [fake.last_name() for _ in range(n)],
            "dob": [
                fake.date_of_birth(minimum_age=18, maximum_age=85).isoformat() for _ in range(n)
            ],
            "province": provinces,
            "postal_code": [_postal_code(p) for p in provinces],
            "email": [fake.email() for _ in range(n)],
            "phone": [fake.numerify("###-###-####") for _ in range(n)],
            "created_at": _random_dates("2010-01-01", "2024-12-31", n),
        }
    )


def generate_policies(customer_ids: list, n: int = 100_000) -> pd.DataFrame:
    """Generate n policies referencing the given customer_ids."""
    logger.info(f"Generating {n:,} policies...")
    start_dates = pd.to_datetime(_random_dates("2015-01-01", "2024-06-30", n))
    duration_days = np.random.randint(365, 1096, size=n)
    end_dates = start_dates + pd.to_timedelta(duration_days, unit="D")

    product_types = np.random.choice(
        PRODUCT_TYPES, size=n, p=_PRODUCT_WEIGHTS / _PRODUCT_WEIGHTS.sum()
    )

    # Coverage band by product type: auto 20K–100K, home 200K–1.5M, life 100K–2M
    _coverage_range = {"auto": (20_000, 100_000), "home": (200_000, 1_500_000), "life": (100_000, 2_000_000)}
    coverage = np.array(
        [round(np.random.uniform(*_coverage_range[pt]), 2) for pt in product_types]
    )
    premiums = np.round(coverage * np.random.uniform(0.005, 0.025, size=n), 2)

    return pd.DataFrame(
        {
            "policy_id": [f"POL_{i+1:07d}" for i in range(n)],
            "customer_id": np.random.choice(customer_ids, size=n),
            "product_type": product_types,
            "coverage_amount": coverage,
            "premium": premiums,
            "start_date": start_dates.strftime("%Y-%m-%d"),
            "end_date": end_dates.strftime("%Y-%m-%d"),
            "status": np.random.choice(
                POLICY_STATUSES, size=n, p=_POLICY_STATUS_WEIGHTS / _POLICY_STATUS_WEIGHTS.sum()
            ),
        }
    )


def generate_claims(policy_ids: list, n: int = 500_000) -> pd.DataFrame:
    """Generate n claims referencing the given policy_ids."""
    logger.info(f"Generating {n:,} claims...")
    claim_dates = pd.to_datetime(_random_dates("2015-01-01", "2024-12-31", n))
    days_before = np.random.randint(1, 31, size=n)
    loss_dates = claim_dates - pd.to_timedelta(days_before, unit="D")

    # Log-normal amounts: realistic heavy tail (most claims small, a few large)
    claim_amounts = np.round(np.random.lognormal(mean=8.5, sigma=1.5, size=n), 2)
    adjuster_nums = np.random.randint(1, 201, size=n)

    return pd.DataFrame(
        {
            "claim_id": [f"CLM_{i+1:07d}" for i in range(n)],
            "policy_id": np.random.choice(policy_ids, size=n),
            "claim_date": claim_dates.strftime("%Y-%m-%d"),
            "loss_date": loss_dates.strftime("%Y-%m-%d"),
            "claim_amount": claim_amounts,
            "claim_status": np.random.choice(
                CLAIM_STATUSES, size=n, p=_CLAIM_STATUS_WEIGHTS / _CLAIM_STATUS_WEIGHTS.sum()
            ),
            "claim_type": np.random.choice(CLAIM_TYPES, size=n),
            "adjuster_id": [f"ADJ_{x:04d}" for x in adjuster_nums],
            "fraud_flag": np.random.binomial(1, 0.03, size=n).astype(bool),
        }
    )


# ── Dirty data injection ───────────────────────────────────────────────────────

def inject_dirty_data(df: pd.DataFrame, entity: str) -> pd.DataFrame:
    """
    Inject ~5% dirty rows so the DQ layer (Phase 4) has real failures to catch.

    Breakdown:
      2%  null injection on non-PK columns
      1%  exact duplicate rows (same PK — dedup in Silver must catch these)
      2%  entity-specific invalid values
    """
    n = len(df)
    pk_col = {"customers": "customer_id", "policies": "policy_id", "claims": "claim_id"}[entity]
    nullable_cols = [c for c in df.columns if c != pk_col]
    dirty = df.copy()

    # 1. Null injection — skip bool columns to avoid pandas dtype warning
    null_n = max(1, int(n * 0.02))
    null_idx = np.random.choice(n, size=null_n, replace=False)
    non_bool_nullable = [c for c in nullable_cols if dirty[c].dtype != bool]
    for idx in null_idx:
        col = random.choice(non_bool_nullable) if non_bool_nullable else nullable_cols[0]
        dirty.at[idx, col] = None

    # 2. Duplicate rows (PK collision — intentional)
    dup_n = max(1, int(n * 0.01))
    dup_idx = np.random.choice(n, size=dup_n, replace=False)
    dirty = pd.concat([dirty, dirty.iloc[dup_idx]], ignore_index=True)

    # 3. Entity-specific invalid values
    inv_n = max(1, int(n * 0.02))
    inv_idx = np.random.choice(n, size=inv_n, replace=False)
    half = inv_n // 2

    if entity == "customers":
        for idx in inv_idx:
            dirty.at[idx, "postal_code"] = f"INVALID{random.randint(100, 999)}"

    elif entity == "policies":
        for idx in inv_idx[:half]:
            dirty.at[idx, "coverage_amount"] = round(
                -abs(float(dirty.at[idx, "coverage_amount"])), 2
            )
        for idx in inv_idx[half:]:
            dirty.at[idx, "end_date"] = "1999-12-31"  # before any valid start_date

    elif entity == "claims":
        for idx in inv_idx[:half]:
            dirty.at[idx, "claim_amount"] = round(
                -abs(float(dirty.at[idx, "claim_amount"])), 2
            )
        for idx in inv_idx[half:]:
            dirty.at[idx, "loss_date"] = "2099-12-31"  # loss_date after claim_date (impossible)

    logger.info(
        f"[{entity}] dirty injection: {dup_n} duplicates, {null_n} nulls, {inv_n} invalid values"
    )
    return dirty


# ── MinIO upload ───────────────────────────────────────────────────────────────

def upload_to_minio(df: pd.DataFrame, entity: str, fmt: str, ingest_date: str) -> str:
    """
    Serialize df and PUT to s3://raw/{entity}/dt={ingest_date}/{entity}.{fmt}.
    Returns the S3 URI.
    """
    client = _get_s3_client()
    key = f"{entity}/dt={ingest_date}/{entity}.{fmt}"

    buf = io.BytesIO()
    if fmt == "csv":
        df.to_csv(buf, index=False)
        content_type = "text/csv"
    else:
        df.to_json(buf, orient="records", lines=True, date_format="iso")
        content_type = "application/json"

    buf.seek(0)
    size_mb = buf.getbuffer().nbytes / 1_048_576
    client.put_object(Bucket=RAW_BUCKET, Key=key, Body=buf.read(), ContentType=content_type)

    uri = f"s3://{RAW_BUCKET}/{key}"
    logger.info(f"Uploaded {uri}  ({size_mb:.1f} MB, {len(df):,} rows)")
    return uri


# ── Orchestration entry point ──────────────────────────────────────────────────

def generate_and_land(
    n_customers: int = 50_000,
    n_policies: int = 100_000,
    n_claims: int = 500_000,
    ingest_date: Optional[str] = None,
) -> dict:
    """
    Full pipeline: generate → inject dirty data → upload to MinIO raw zone.

    Called by the Airflow PythonOperator (ingest_date = {{ ds }}) or __main__.
    Returns a dict of S3 URIs keyed by entity name.
    """
    run_date = ingest_date or date.today().isoformat()
    logger.info(f"=== generate_and_land  ingest_date={run_date} ===")

    customers = generate_customers(n_customers)
    customers = inject_dirty_data(customers, "customers")
    c_uri = upload_to_minio(customers, "customers", "csv", run_date)

    policies = generate_policies(customers["customer_id"].tolist(), n_policies)
    policies = inject_dirty_data(policies, "policies")
    p_uri = upload_to_minio(policies, "policies", "csv", run_date)

    claims = generate_claims(policies["policy_id"].tolist(), n_claims)
    claims = inject_dirty_data(claims, "claims")
    cl_uri = upload_to_minio(claims, "claims", "json", run_date)

    result = {
        "customers": c_uri,
        "policies": p_uri,
        "claims": cl_uri,
        "ingest_date": run_date,
    }
    logger.info(f"=== generation complete: {json.dumps(result)} ===")
    return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")

    parser = argparse.ArgumentParser(description="Generate synthetic insurance data → MinIO")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Small run (1K customers / 2K policies / 5K claims) for quick local testing",
    )
    args = parser.parse_args()

    if args.sample:
        result = generate_and_land(n_customers=1_000, n_policies=2_000, n_claims=5_000)
    else:
        result = generate_and_land()

    print(json.dumps(result, indent=2))
