from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

DBT_PROJECT_DIR = "/opt/airflow/lakehouse_transform"
DBT_PROFILES_DIR = "/opt/airflow/lakehouse_transform"  # profiles.yml copied here, see note below

def alert_on_failure(context):
    """Failure callback — replace print with a Slack webhook call or EmailOperator in production."""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    exec_date = context["execution_date"]
    log.error(f"ALERT: Task '{task_id}' in DAG '{dag_id}' failed at {exec_date}")
    # e.g. requests.post(SLACK_WEBHOOK_URL, json={"text": f"{dag_id}.{task_id} failed"})

default_args = {
    "owner": "sathwik",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": alert_on_failure,
    "execution_timeout": timedelta(minutes=20),
}

with DAG(
    dag_id="lakehouse_transform_pipeline",
    default_args=default_args,
    description="Run and test the lakehouse_transform dbt project against Databricks",
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["dbt", "databricks", "claims"],
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run >> dbt_test