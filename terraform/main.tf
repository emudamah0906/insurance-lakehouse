# Snowflake infrastructure — provisioned in Phase 5
# terraform init && terraform apply

terraform {
  required_version = ">= 1.6"
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

# provider "snowflake" {
#   account  = var.snowflake_account
#   username = var.snowflake_username
#   password = var.snowflake_password
#   role     = "SYSADMIN"
# }
