# terraform/databricks/jobs.tf
resource "databricks_job" "lakehouse_transform_job" {
  name = "lakehouse_transform_dbt_run"

  task {
    task_key = "dbt_run_and_test"

    notebook_task {
      notebook_path = "/Workspace/Shared/run_dbt_project"
    }
    # No new_cluster or existing_cluster_id block at all —
    # omitting compute entirely is what tells Databricks to use serverless.
  }

  schedule {
    quartz_cron_expression = "0 0 6 * * ?"  # daily at 6am
    timezone_id            = "America/New_York"
  }
}