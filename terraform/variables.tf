variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.ca-central-1.aws)"
  type        = string
}

variable "snowflake_username" {
  description = "Snowflake login username"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}
