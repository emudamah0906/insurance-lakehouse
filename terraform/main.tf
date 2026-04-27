# Snowflake infrastructure for Insurance Claims Lakehouse
# Manages: database, warehouse, schemas, role, and grants
#
# Usage:
#   export TF_VAR_snowflake_account="YQ26829.ca-central-1.aws"
#   export TF_VAR_snowflake_username="MAHESHEMUDAPURAM"
#   export TF_VAR_snowflake_password="<password>"
#   terraform init && terraform plan && terraform apply

terraform {
  required_version = ">= 1.6"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "ACCOUNTADMIN"
}

# ── Warehouse ─────────────────────────────────────────────────────────────────

resource "snowflake_warehouse" "compute_wh" {
  name           = "COMPUTE_WH"
  warehouse_size = "XSMALL"
  auto_suspend   = 60
  auto_resume    = true
  comment        = "Insurance Lakehouse — dbt + ad-hoc queries"
}

# ── Database ──────────────────────────────────────────────────────────────────

resource "snowflake_database" "insurance_dw" {
  name    = "INSURANCE_DW"
  comment = "Insurance Claims Lakehouse — Gold layer"
}

# ── Schemas ───────────────────────────────────────────────────────────────────

resource "snowflake_schema" "staging" {
  database = snowflake_database.insurance_dw.name
  name     = "STAGING"
  comment  = "Silver mirror — loaded by Airflow snowflake_load job"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.insurance_dw.name
  name     = "MARTS"
  comment  = "Gold layer — built by dbt"
}

# ── Transformer role (v0.90 uses snowflake_account_role) ──────────────────────

resource "snowflake_account_role" "transformer" {
  name    = "TRANSFORMER"
  comment = "Used by Airflow + dbt for ETL writes and model builds"
}

# ── Grants (v0.90 uses snowflake_grant_privileges_to_account_role) ────────────

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.compute_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE SCHEMA"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.insurance_dw.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"${snowflake_database.insurance_dw.name}\".\"${snowflake_schema.staging.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_marts" {
  account_role_name = snowflake_account_role.transformer.name
  all_privileges    = true
  on_schema {
    schema_name = "\"${snowflake_database.insurance_dw.name}\".\"${snowflake_schema.marts.name}\""
  }
}

# ── Grant role to user ────────────────────────────────────────────────────────

resource "snowflake_grant_account_role" "transformer_to_user" {
  role_name = snowflake_account_role.transformer.name
  user_name = var.snowflake_username
}
