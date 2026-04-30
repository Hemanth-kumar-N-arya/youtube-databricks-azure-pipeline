# COMMAND ----------
dbutils.widgets.text("batch_date", "", "Batch Date")
dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")

batch_date   = dbutils.widgets.get("batch_date")
adls_account = dbutils.widgets.get("adls_account_name")

# Unity Catalog manages ADLS Gen2 access via External Locations
# No Spark OAuth config needed — storage credential handles auth
print('Storage access via Unity Catalog — account: ' + adls_account)


from datetime import datetime, timezone
if not batch_date:
    batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"=== Bronze Ingestion Starting ===")
print(f"Batch date  : {batch_date}")
print(f"ADLS account: {adls_account}")

# COMMAND ----------

spark.conf.set(
spark.conf.set(
spark.conf.set(
spark.conf.set(
spark.conf.set(

print("ADLS Gen2 authentication configured")

# COMMAND ----------
from pyspark.sql import functions as F

RAW_PATH    = f"abfss://raw@{adls_account}.dfs.core.windows.net/youtube/{batch_date}/"
BRONZE_PATH = f"abfss://bronze@{adls_account}.dfs.core.windows.net/youtube_raw/"

print(f"Reading from : {RAW_PATH}")
print(f"Writing to   : {BRONZE_PATH}")

try:
    raw_files = dbutils.fs.ls(RAW_PATH)
    print(f"Found {len(raw_files)} file(s) in landing zone")
except Exception as e:
    raise Exception(f"No raw data found at {RAW_PATH}. Error: {e}")

df_raw = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(RAW_PATH)
)
print(f"Raw record count: {df_raw.count()}")

# COMMAND ----------
df_bronze = (
    df_raw
    .withColumn("_ingested_at",   F.current_timestamp())
    .withColumn("_batch_date",    F.lit(batch_date))
    .withColumn("_source_system", F.lit("youtube_data_api_v3"))
    .withColumn("_layer",         F.lit("bronze"))
)

(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "true")
    .save(BRONZE_PATH)
)
print("Bronze Delta write complete")

# COMMAND ----------
spark.sql("CREATE DATABASE IF NOT EXISTS youtube_pipeline")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS youtube_pipeline.bronze_raw
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

row_count = spark.read.format("delta").load(BRONZE_PATH).count()
assert row_count > 0, "Bronze table is empty — ingestion failed"

print(f"=== Bronze Complete: {row_count} total rows ===")
dbutils.notebook.exit(f"SUCCESS: {row_count} rows in bronze table")