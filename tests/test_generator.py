"""
Unit tests for the synthetic data generator.
No MinIO connection required — upload calls are mocked.
"""
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from data_generator.generate import (
    CANADIAN_PROVINCES,
    CLAIM_STATUSES,
    CLAIM_TYPES,
    PRODUCT_TYPES,
    generate_claims,
    generate_customers,
    generate_policies,
    inject_dirty_data,
    upload_to_minio,
)

# ── Expected schemas ────────────────────────────────────────────────────────────

_CUSTOMER_COLS = {
    "customer_id", "first_name", "last_name", "dob",
    "province", "postal_code", "email", "phone", "created_at",
}
_POLICY_COLS = {
    "policy_id", "customer_id", "product_type", "coverage_amount",
    "premium", "start_date", "end_date", "status",
}
_CLAIM_COLS = {
    "claim_id", "policy_id", "claim_date", "loss_date",
    "claim_amount", "claim_status", "claim_type", "adjuster_id", "fraud_flag",
}


# ── Fixtures ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def customers(sample_size):
    return generate_customers(sample_size)


@pytest.fixture(scope="module")
def policies(customers):
    return generate_policies(customers["customer_id"].tolist(), len(customers) * 2)


@pytest.fixture(scope="module")
def claims(policies):
    return generate_claims(policies["policy_id"].tolist(), len(policies) * 2)


# ── Customer tests ──────────────────────────────────────────────────────────────

class TestCustomers:
    def test_row_count(self, customers, sample_size):
        assert len(customers) == sample_size

    def test_schema(self, customers):
        assert set(customers.columns) == _CUSTOMER_COLS

    def test_pk_unique(self, customers):
        assert customers["customer_id"].is_unique

    def test_province_values(self, customers):
        assert set(customers["province"].dropna()).issubset(set(CANADIAN_PROVINCES))

    def test_ontario_most_common(self, customers):
        assert customers["province"].value_counts().index[0] == "Ontario"

    def test_postal_code_format(self, customers):
        # Valid Canadian format: A1A 1A1
        valid = customers["postal_code"].str.match(r"^[A-Z]\d[A-Z] \d[A-Z]\d$", na=False)
        assert valid.mean() > 0.95

    def test_no_null_pks(self, customers):
        assert customers["customer_id"].notna().all()


# ── Policy tests ────────────────────────────────────────────────────────────────

class TestPolicies:
    def test_row_count(self, policies, customers):
        assert len(policies) == len(customers) * 2

    def test_schema(self, policies):
        assert set(policies.columns) == _POLICY_COLS

    def test_pk_unique(self, policies):
        assert policies["policy_id"].is_unique

    def test_fk_referential_integrity(self, policies, customers):
        assert set(policies["customer_id"]).issubset(set(customers["customer_id"]))

    def test_product_types(self, policies):
        assert set(policies["product_type"].dropna()).issubset(set(PRODUCT_TYPES))

    def test_end_after_start(self, policies):
        starts = pd.to_datetime(policies["start_date"])
        ends = pd.to_datetime(policies["end_date"])
        assert (ends > starts).all()

    def test_coverage_positive(self, policies):
        assert (policies["coverage_amount"] > 0).all()

    def test_premium_positive(self, policies):
        assert (policies["premium"] > 0).all()


# ── Claim tests ─────────────────────────────────────────────────────────────────

class TestClaims:
    def test_row_count(self, claims, policies):
        assert len(claims) == len(policies) * 2

    def test_schema(self, claims):
        assert set(claims.columns) == _CLAIM_COLS

    def test_pk_unique(self, claims):
        assert claims["claim_id"].is_unique

    def test_fk_referential_integrity(self, claims, policies):
        assert set(claims["policy_id"]).issubset(set(policies["policy_id"]))

    def test_loss_date_before_claim_date(self, claims):
        claim_dates = pd.to_datetime(claims["claim_date"])
        loss_dates = pd.to_datetime(claims["loss_date"])
        assert (loss_dates <= claim_dates).all()

    def test_claim_amounts_positive(self, claims):
        assert (claims["claim_amount"] > 0).all()

    def test_fraud_rate_realistic(self, claims):
        rate = claims["fraud_flag"].mean()
        assert 0.01 <= rate <= 0.06  # expect ~3%

    def test_claim_types(self, claims):
        assert set(claims["claim_type"].dropna()).issubset(set(CLAIM_TYPES))

    def test_claim_statuses(self, claims):
        assert set(claims["claim_status"].dropna()).issubset(set(CLAIM_STATUSES))


# ── Dirty data injection tests ──────────────────────────────────────────────────

class TestDirtyData:
    def test_duplicates_added_customers(self, sample_size):
        df = generate_customers(sample_size)
        dirty = inject_dirty_data(df, "customers")
        assert len(dirty) > len(df)

    def test_invalid_postal_codes(self, sample_size):
        df = generate_customers(sample_size)
        dirty = inject_dirty_data(df, "customers")
        assert dirty["postal_code"].str.startswith("INVALID", na=False).sum() > 0

    def test_negative_coverage_amounts(self, sample_size):
        customers = generate_customers(100)
        df = generate_policies(customers["customer_id"].tolist(), sample_size)
        dirty = inject_dirty_data(df, "policies")
        assert (dirty["coverage_amount"] < 0).any()

    def test_invalid_end_dates(self, sample_size):
        customers = generate_customers(100)
        df = generate_policies(customers["customer_id"].tolist(), sample_size)
        dirty = inject_dirty_data(df, "policies")
        assert (dirty["end_date"] == "1999-12-31").any()

    def test_negative_claim_amounts(self, sample_size):
        customers = generate_customers(100)
        policies = generate_policies(customers["customer_id"].tolist(), 200)
        df = generate_claims(policies["policy_id"].tolist(), sample_size)
        dirty = inject_dirty_data(df, "claims")
        assert (dirty["claim_amount"] < 0).any()

    def test_future_loss_dates(self, sample_size):
        customers = generate_customers(100)
        policies = generate_policies(customers["customer_id"].tolist(), 200)
        df = generate_claims(policies["policy_id"].tolist(), sample_size)
        dirty = inject_dirty_data(df, "claims")
        assert (dirty["loss_date"] == "2099-12-31").any()

    def test_nulls_injected(self, sample_size):
        df = generate_customers(sample_size)
        dirty = inject_dirty_data(df, "customers")
        # At least one non-PK column must have a null
        non_pk = [c for c in dirty.columns if c != "customer_id"]
        assert dirty[non_pk].isnull().any().any()


# ── Upload tests (MinIO mocked) ─────────────────────────────────────────────────

class TestUpload:
    @patch("data_generator.generate._get_s3_client")
    def test_csv_upload_key_and_bucket(self, mock_factory, sample_size):
        mock_client = MagicMock()
        mock_factory.return_value = mock_client

        df = generate_customers(sample_size)
        uri = upload_to_minio(df, "customers", "csv", "2024-01-15")

        call_kw = mock_client.put_object.call_args.kwargs
        assert call_kw["Bucket"] == "raw"
        assert call_kw["Key"] == "customers/dt=2024-01-15/customers.csv"
        assert uri == "s3://raw/customers/dt=2024-01-15/customers.csv"

    @patch("data_generator.generate._get_s3_client")
    def test_json_upload_key(self, mock_factory, sample_size):
        mock_client = MagicMock()
        mock_factory.return_value = mock_client

        customers = generate_customers(100)
        policies = generate_policies(customers["customer_id"].tolist(), 200)
        df = generate_claims(policies["policy_id"].tolist(), sample_size)
        uri = upload_to_minio(df, "claims", "json", "2024-01-15")

        call_kw = mock_client.put_object.call_args.kwargs
        assert call_kw["Key"] == "claims/dt=2024-01-15/claims.json"
        assert uri == "s3://raw/claims/dt=2024-01-15/claims.json"
