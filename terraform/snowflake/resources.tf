# terraform/snowflake/resources.tf
resource "snowflake_warehouse" "claims_wh" {
  name                = "CLAIMS_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
}

resource "snowflake_database" "claims_platform" {
  name = "CLAIMS_PLATFORM"
}

resource "snowflake_schema" "serving" {
  database = snowflake_database.claims_platform.name
  name     = "SERVING"
}

resource "snowflake_account_role" "claims_analyst" {
  provider = snowflake.securityadmin
  name     = "CLAIMS_ANALYST"
}

resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.claims_analyst.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.claims_wh.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_usage" {
  provider          = snowflake.securityadmin
  account_role_name = snowflake_account_role.claims_analyst.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.serving.fully_qualified_name
  }
}