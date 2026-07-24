# terraform/databricks/variables.tf
variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, e.g. https://<workspace-id>.cloud.databricks.com. Set via terraform.tfvars (gitignored) or TF_VAR_databricks_host."
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks personal access token used for provider auth. Set via terraform.tfvars (gitignored) or TF_VAR_databricks_token. Never hardcode the real value here."
}