# spark_jobs/bronze_ingest_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StringType, DoubleType

schema = (
    StructType()
    .add("claim_id", StringType())
    .add("patient_id", StringType())
    .add("provider_id", StringType())
    .add("procedure_code", StringType())
    .add("procedure_desc", StringType())
    .add("billed_amount", DoubleType())
    .add("submitted_at", StringType())
)

def main():
    spark = SparkSession.builder.appName("bronze_claims_streaming").getOrCreate()

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:19092")
        .option("subscribe", "claims.submitted")
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        raw_stream
        .select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("submitted_date", to_date(col("submitted_at")))
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "./checkpoints/bronze_claims")
        .partitionBy("submitted_date")
        .trigger(processingTime="30 seconds")
        .start("./delta/bronze/claims_submitted")
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()