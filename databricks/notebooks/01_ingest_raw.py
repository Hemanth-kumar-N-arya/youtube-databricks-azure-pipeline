# COMMAND ----------
# NOTEBOOK: 01_ingest_raw — Bronze Layer
# PURPOSE:  Read raw JSON files from ADLS Gen2 landing zone
#           and write them as Delta tables into the Bronze container
# LAYER:    Bronze — raw, unmodified, append-only
# COMMAND ----------

# ── 1. Import shared configuration ─────────────────────────────────
# All parameters, secrets, and ADLS setup are centralized in config.py

import sys
sys.path.append("/Workspace/databricks")
from config import init_notebook

batch_date, adls_account, paths = init_notebook()

print(f"=== Bronze Ingestion Starting ===")
print(f"Reading from: {paths['bronze']}")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net",
    sp_client_id
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net",
    sp_client_secret
)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
)

print("ADLS Gen2 authentication configured successfully")

# COMMAND ----------

# ── 3. Define paths ─────────────────────────────────────────────────
# abfss:// is the secure Azure Data Lake Storage protocol
# Structure: abfss://<container>@<account>.dfs.core.windows.net/<path>

RAW_PATH    = f"abfss://raw@{adls_account}.dfs.core.windows.net/youtube/{batch_date}/"
BRONZE_PATH = f"abfss://bronze@{adls_account}.dfs.core.windows.net/youtube_raw/"

print(f"Reading from : {RAW_PATH}")
print(f"Writing to   : {BRONZE_PATH}")

# COMMAND ----------

# ── 4. Read raw JSON from the landing zone ──────────────────────────

from pyspark.sql import functions as F 
from pyspark.sql.types import StructType

# Check if raw data exists for this batch date
try:
    raw_files = dbutils.fs.ls(RAW_PATH)
    print(f"Found {len(raw_files)} file(s) in raw landing zone")
except Exception:
    raise Exception(
        f"No raw data found at {RAW_PATH}. "
        f"Ensure ADF has deposited data for batch_date={batch_date}"
    )

# Read all JSON files in the batch folder
# multiLine=True handles JSON arrays and nested objects
df_raw = (
    spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")          # Don't fail on malformed records
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(RAW_PATH)
)

print(f"Raw record count : {df_raw.count()}")
print(f"Raw schema:")
df_raw.printSchema()

# COMMAND ----------

# ── 5. Add pipeline metadata columns ───────────────────────────────
# Bronze layer adds audit columns so we always know exactly
# when data arrived, from which batch, and from which source


df_bronze = (
    df_raw
    .withColumn("_ingested_at",   F.current_timestamp())
    .withColumn("_batch_date",    F.lit(batch_date))
    .withColumn("_source_system", F.lit("youtube_data_api_v3"))
    .withColumn("_layer",         F.lit("bronze"))
)

# COMMAND ----------

# ── 6. Write to Bronze Delta table ──────────────────────────────────
# append mode — Bronze is always append-only, never overwrite
# partitioned by batch_date for efficient time-range queries

(
    df_bronze
    .write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "true")    # Handle new fields from YouTube API gracefully
    .save(BRONZE_PATH)
)

print(f"Bronze write complete")
print(f"Records written: {df_bronze.count()}")

# COMMAND ----------
# ── 7. Register as Delta table in Hive Metastore ────────────────────
# This allows Databricks SQL to query it by name
# without needing to know the ADLS path

spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS youtube_pipeline
    COMMENT 'YouTube Data Pipeline — all layers'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS youtube_pipeline.bronze_raw
    USING DELTA
    LOCATION '{BRONZE_PATH}'
""")

# Run DESCRIBE to confirm table is registered
display(spark.sql("DESCRIBE EXTENDED youtube_pipeline.bronze_raw"))

# COMMAND ----------

# ── 8. Basic data quality check ─────────────────────────────────────
# In enterprise pipelines, every layer has at least a basic
# row count check before declaring success

row_count = spark.read.format("delta").load(BRONZE_PATH).count()
assert row_count > 0, "Bronze table is empty — ingestion may have failed"

print(f"=== Bronze Layer Complete ===")
print(f"Total rows in bronze table: {row_count}")
dbutils.notebook.exit(f"SUCCESS: {row_count} rows in bronze table")
