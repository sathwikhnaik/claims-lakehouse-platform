# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Delta Ingestion Walkthrough
# MAGIC Loads synthetic claims data into a Bronze Delta table and explores
# MAGIC Delta Lake's transaction log, `DESCRIBE HISTORY`, and time travel.

# COMMAND ----------

# 1. Load synthetic claims JSONL (already uploaded to a Unity Catalog volume) into a Bronze Delta table
df = spark.read.json("/Volumes/workspace/bronze/landing/claims_submitted.jsonl")
df.write.format("delta").mode("overwrite").saveAsTable("workspace.bronze.claims_submitted")
print(f"Loaded {df.count()} rows into workspace.bronze.claims_submitted")

# COMMAND ----------

# 2. Look at what actually got created
display(spark.sql("DESCRIBE HISTORY workspace.bronze.claims_submitted"))
# Notice: version 0, operation = WRITE

# COMMAND ----------

# 3. Make a change - simulate a correction to one claim
first_claim_id = spark.sql("SELECT claim_id FROM workspace.bronze.claims_submitted LIMIT 1").collect()[0][0]

spark.sql(f"""
    UPDATE workspace.bronze.claims_submitted
    SET billed_amount = 275.00
    WHERE claim_id = '{first_claim_id}'
""")
print(f"Updated claim {first_claim_id} -> billed_amount = 275.00")

# COMMAND ----------

# 4. Check history again - you now have version 1
display(spark.sql("DESCRIBE HISTORY workspace.bronze.claims_submitted"))

# COMMAND ----------

# 5. Time travel - read the table as it looked BEFORE your update
old_version = spark.read.format("delta").option("versionAsOf", 0).table("workspace.bronze.claims_submitted")
display(old_version.filter(old_version.claim_id == first_claim_id))

# COMMAND ----------

# 6. Look at the raw files Databricks manages for you (Unity Catalog managed table storage,
#    not the legacy dbfs:/user/hive/warehouse path, since this workspace uses UC)
detail_row = spark.sql("DESCRIBE DETAIL workspace.bronze.claims_submitted").collect()[0]
table_location = detail_row["location"]
print("DESCRIBE DETAIL location field:", repr(table_location))

delta_log_files = []
delta_log_error = None
if table_location:
    try:
        listing = dbutils.fs.ls(table_location.rstrip("/") + "/_delta_log/")
        display(listing)
        delta_log_files = [f.name for f in listing]
    except Exception as e:
        delta_log_error = str(e)
        print(f"dbutils.fs.ls was blocked by Unity Catalog governance: {delta_log_error}")
else:
    delta_log_error = "DESCRIBE DETAIL returned no location for this managed UC table (governance redaction)."
    print(delta_log_error)

# COMMAND ----------

# 7. Summarize results as a machine-readable payload for the driving automation/tooling
import json

history_rows = (
    spark.sql("DESCRIBE HISTORY workspace.bronze.claims_submitted")
    .select("version", "timestamp", "operation")
    .orderBy("version")
    .collect()
)

old_amount = (
    old_version.filter(old_version.claim_id == first_claim_id)
    .collect()[0]["billed_amount"]
)
new_amount = spark.sql(
    f"SELECT billed_amount FROM workspace.bronze.claims_submitted WHERE claim_id = '{first_claim_id}'"
).collect()[0][0]

summary = {
    "row_count": df.count(),
    "updated_claim_id": first_claim_id,
    "old_billed_amount": float(old_amount),
    "new_billed_amount": float(new_amount),
    "history": [
        {"version": r["version"], "operation": r["operation"], "timestamp": str(r["timestamp"])}
        for r in history_rows
    ],
    "table_location": table_location,
    "delta_log_files": delta_log_files,
    "delta_log_error": delta_log_error,
}

print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
