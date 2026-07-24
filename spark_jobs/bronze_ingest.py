# spark_jobs/bronze_ingest.py
#
# NOTE: this builds a *local* SparkSession with Delta Lake support.
# It can write local/DBFS Delta paths or 2-level (db.table) tables in the
# local Hive metastore. It CANNOT reach Databricks Unity Catalog catalogs
# (e.g. `claims_platform`) - that requires running as a Databricks
# notebook/job, since Unity Catalog access isn't available to a plain
# local Spark session. Running against a UC-qualified target_table like
# "claims_platform.bronze.claims_submitted" will still fail locally with
# a "catalog not found" error even with Delta configured correctly.

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
from delta import configure_spark_with_delta_pip
import sys

def main(source_path: str, target_table: str):
    builder = (
        SparkSession.builder
        .appName("bronze_claims_ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    raw_df = spark.read.json(source_path)

    bronze_df = raw_df.withColumn(
        "submitted_date", to_date(col("submitted_at"))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy("submitted_date")
        .saveAsTable(target_table)
    )

    print(f"Ingested {bronze_df.count()} rows into {target_table}")

if __name__ == "__main__":
    source_path = sys.argv[1] if len(sys.argv) > 1 else "sample_data/claims_submitted.jsonl"
    target_table = sys.argv[2] if len(sys.argv) > 2 else "claims_platform.bronze.claims_submitted"
    main(source_path, target_table)