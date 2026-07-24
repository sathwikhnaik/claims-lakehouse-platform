"""Claims lakehouse pipeline DAG.

Orchestrates the full flow described in the README:

    generate synthetic claims -> bronze ingest (Unity Catalog) -> dbt run

Runs the bronze ingest job against the ``claims_platform`` Unity Catalog
catalog so the ``claims_platform.bronze.claims_submitted`` table (the dbt
source) is refreshed on every run, instead of requiring a manual copy.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

REPO_ROOT = os.environ.get("CLAIMS_LAKEHOUSE_REPO", "/opt/airflow/repo")

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="claims_lakehouse_pipeline",
    description="Generate claims -> bronze ingest -> dbt run (silver/gold)",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["claims", "lakehouse", "bronze", "dbt"],
) as dag:

    generate_claims = BashOperator(
        task_id="generate_claims",
        bash_command=f"cd {REPO_ROOT} && python data_generator/generate_claims.py",
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest_unity_catalog",
        bash_command=(
            f"cd {REPO_ROOT} && spark-submit spark_jobs/bronze_ingest.py "
            "--sink unity_catalog --catalog claims_platform --schema bronze --table claims_submitted"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {REPO_ROOT} && dbt run",
    )

    generate_claims >> bronze_ingest >> dbt_run
