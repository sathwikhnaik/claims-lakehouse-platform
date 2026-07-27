# airflow/dags/full_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import os

log = LoggingMixin().log

LAKEHOUSE_DIR = "/opt/airflow/lakehouse_transform"
SERVING_DIR = "/opt/airflow/serving_marts"

def export_gold_to_csv():
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient(
        host=os.environ["DATABRICKS_HOST"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    run = w.jobs.run_now(job_id=int(os.environ["GOLD_EXPORT_JOB_ID"]))
    w.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.response.run_id)
    log.info("Gold export notebook job completed.")

def load_to_snowflake():
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        warehouse="CLAIMS_WH",
        database="CLAIMS_PLATFORM",
        schema="SERVING",
    )
    cs = conn.cursor()
    try:
        cs.execute("PUT file:///opt/airflow/exports/fct_provider_daily_billing.csv @%fct_provider_daily_billing OVERWRITE=TRUE")
        cs.execute("""
            COPY INTO fct_provider_daily_billing
            FROM @%fct_provider_daily_billing/fct_provider_daily_billing.csv.gz
            FILE_FORMAT = (FORMAT_NAME = csv_format)
            FORCE = TRUE
        """)
        log.info("Snowflake load complete.")
    finally:
        cs.close()
        conn.close()

def alert_on_failure(context):
    task_id = context["task_instance"].task_id
    log.error(f"ALERT: Task '{task_id}' failed in full_pipeline_dag")

default_args = {
    "owner": "sathwik",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="full_pipeline",
    default_args=default_args,
    description="End-to-end: Databricks/Delta transforms -> Snowflake load -> serving marts -> dashboard",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["capstone", "final"],
) as dag:

    dbt_run_lakehouse = BashOperator(
        task_id="dbt_run_lakehouse",
        bash_command=f"cd {LAKEHOUSE_DIR} && dbt run --profiles-dir {LAKEHOUSE_DIR}",
    )

    dbt_test_lakehouse = BashOperator(
        task_id="dbt_test_lakehouse",
        bash_command=f"cd {LAKEHOUSE_DIR} && dbt test --profiles-dir {LAKEHOUSE_DIR}",
    )

    export_gold = PythonOperator(
        task_id="export_gold_to_csv",
        python_callable=export_gold_to_csv,
    )

    load_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    dbt_run_serving = BashOperator(
        task_id="dbt_run_serving",
        bash_command=f"cd {SERVING_DIR} && dbt run --profiles-dir {SERVING_DIR}",
    )

    dbt_test_serving = BashOperator(
        task_id="dbt_test_serving",
        bash_command=f"cd {SERVING_DIR} && dbt test --profiles-dir {SERVING_DIR}",
    )

    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command="python3 /opt/airflow/dashboard/build_dashboard.py",
    )

    (
        dbt_run_lakehouse
        >> dbt_test_lakehouse
        >> export_gold
        >> load_snowflake
        >> dbt_run_serving
        >> dbt_test_serving
        >> generate_dashboard
    )