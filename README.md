# Claims Lakehouse Platform

An end-to-end lakehouse platform for processing insurance claims data,
combining Databricks, Snowflake, Spark, dbt, and Airflow.

## Repository Structure

```
claims-lakehouse-platform/
├── docker-compose.yml        # Local dev stack (Spark, Airflow)
├── terraform/
│   ├── databricks/           # Databricks workspace/infra as code
│   └── snowflake/            # Snowflake infra as code (added Week 11)
├── data_generator/
│   └── generate_claims.py    # Synthetic claims data generator
├── spark_jobs/
│   └── bronze_ingest.py      # Raw -> bronze Delta ingestion job
├── dbt/
│   ├── lakehouse_transform/  # dbt-databricks project (bronze -> silver/gold)
│   └── serving_marts/        # dbt-snowflake project, serving marts (added Week 11)
├── airflow/
│   └── dags/                 # Airflow DAGs orchestrating the pipeline
├── sample_data/               # Exported sample artifacts (added Week 13)
└── README.md
```

## Getting Started

1. Generate sample claims data:

   ```bash
   python data_generator/generate_claims.py --num-records 1000
   ```

2. Start the local dev stack:

   ```bash
   docker-compose up -d
   ```

3. Run the bronze ingestion job:

   ```bash
   # Local dev: writes a Delta table to a local/DBFS path
   spark-submit spark_jobs/bronze_ingest.py

   # Databricks: appends directly into the Unity Catalog table that
   # dbt's sources.yml reads from (claims_platform.bronze.claims_submitted)
   spark-submit spark_jobs/bronze_ingest.py \
     --sink unity_catalog --catalog claims_platform --schema bronze --table claims_submitted
   ```

4. Run dbt transformations:

   ```bash
   cd dbt/lakehouse_transform && dbt run
   ```

The `airflow/dags/claims_lakehouse_pipeline.py` DAG chains all three steps
(generate -> bronze ingest into Unity Catalog -> dbt run) so the pipeline
can be scheduled end-to-end instead of run manually.

## Components

- **terraform/databricks** – Infrastructure as code for the Databricks workspace, clusters, and catalogs.
- **terraform/snowflake** – Infrastructure as code for the Snowflake warehouse used by serving marts.
- **data_generator** – Generates synthetic claims records for testing and demos.
- **spark_jobs** – PySpark jobs that ingest raw data into the bronze layer.
- **dbt/lakehouse_transform** – dbt project (Databricks) transforming bronze data into silver/gold tables.
- **dbt/serving_marts** – dbt project (Snowflake) building serving-layer marts for downstream consumers.
- **airflow/dags** – Airflow DAGs orchestrating the generator, Spark jobs, and dbt runs.
- **sample_data** – Exported sample data artifacts for local development and demos.
