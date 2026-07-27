# terraform/snowflake/main.tf
terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 2.0"
    }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_org_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file("~/.snowflake/snowflake_key.p8")
  role              = "SYSADMIN"
}

# Role/user/grant management is owned by SECURITYADMIN in Snowflake by design,
# separate from SYSADMIN which owns warehouses/databases/schemas. Rather than
# granting CREATE ROLE to SYSADMIN (which weakens that separation), resources
# that create or grant roles use this aliased provider instead.
provider "snowflake" {
  alias             = "securityadmin"
  organization_name = var.snowflake_org_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  authenticator     = "SNOWFLAKE_JWT"
  private_key       = file("~/.snowflake/snowflake_key.p8")
  role              = "SECURITYADMIN"
}