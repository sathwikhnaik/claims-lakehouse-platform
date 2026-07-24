# terraform/databricks/schemas.tf
resource "databricks_schema" "bronze" {
  catalog_name = "claims_platform"
  name         = "bronze"
  comment      = "Raw, as-ingested claim events. No transformation applied."
}

resource "databricks_schema" "silver" {
  catalog_name = "claims_platform"
  name         = "silver"
  comment      = "Cleaned, deduplicated, type-cast claims data."
}

resource "databricks_schema" "gold" {
  catalog_name = "claims_platform"
  name         = "gold"
  comment      = "Aggregated, business-level tables for serving and BI."
}