output "warehouse_name" {
  description = "Snowflake warehouse name"
  value       = snowflake_warehouse.compute_wh.name
}

output "database_name" {
  description = "Snowflake database name"
  value       = snowflake_database.insurance_dw.name
}

output "staging_schema" {
  description = "Staging schema (Silver mirror)"
  value       = "${snowflake_database.insurance_dw.name}.${snowflake_schema.staging.name}"
}

output "marts_schema" {
  description = "Marts schema (Gold / dbt models)"
  value       = "${snowflake_database.insurance_dw.name}.${snowflake_schema.marts.name}"
}

output "transformer_role" {
  description = "IAM role used by Airflow and dbt"
  value       = snowflake_account_role.transformer.name
}
