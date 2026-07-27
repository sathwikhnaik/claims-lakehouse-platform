# quick_check.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("quick_check").master("local[*]").getOrCreate()

from pyspark.sql.functions import count

df = spark.read.format("delta").load("./delta/bronze/claims_submitted")

dupes = (
    df.groupBy("claim_id")
    .agg(count("*").alias("occurrence_count"))
    .filter("occurrence_count > 1")
    .orderBy("occurrence_count", ascending=False)
)
dupes.show(10)
print("Total duplicated claim_ids:", dupes.count())