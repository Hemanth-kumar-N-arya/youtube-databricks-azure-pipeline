# COMMAND ----------
# NOTEBOOK: 02_transform_silver — Silver Layer
# PURPOSE:  Read from Bronze Delta table, flatten nested JSON,
#           clean data, deduplicate, and write to Silver Delta table
# LAYER:    Silver — clean, structured, deduplicated
# COMMAND ----------

# ── 1. Parameters ───────────────────────────────────────────────────
dbutils.widgets.text("batch_date",       "",                    "Batch Date")
dbutils.widgets.text("adls_account_name","adlsytpipelinedev001","ADLS Account")

batch_date   = dbutils.widgets.get("batch_date")
adls_account = dbutils.widgets.get("adls_account_name")

from datetime import datetime, timezone
if not batch_date:
    batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

print(f"=== Silver Transformation Starting ===")
print(f"Batch date: {batch_date}")

# COMMAND ----------

# COMMAND ----------

# ── 2. Configure ADLS access (same as Bronze) ───────────────────────
sp_client_id     = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")
tenant_id        = dbutils.secrets.get(scope="kv-scope", key="tenant-id")

spark.conf.set(f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net",     sp_client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net", sp_client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print("ADLS Gen2 authentication configured")

# COMMAND ----------

# ── 3. Define paths ─────────────────────────────────────────────────
BRONZE_PATH = f"abfss://bronze@{adls_account}.dfs.core.windows.net/youtube_raw/"
SILVER_PATH = f"abfss://silver@{adls_account}.dfs.core.windows.net/youtube_videos/"

print(f"Reading from : {BRONZE_PATH}")
print(f"Writing to   : {SILVER_PATH}")

# COMMAND ----------

# ── 4. Read only this batch from Bronze ─────────────────────────────
# Partition pruning — only read today's partition, not the whole table
# This is why we partitioned Bronze by _batch_date

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, IntegerType, TimestampType
)

df_bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
    .filter(F.col("_batch_date") == batch_date)
)

print(f"Bronze records for {batch_date}: {df_bronze.count()}")

if df_bronze.count() == 0:
    dbutils.notebook.exit(f"WARNING: No bronze records for {batch_date}")
    
# ── 5. Flatten nested YouTube API JSON structure ─────────────────────
# The YouTube API returns deeply nested JSON like:
#   snippet.title, snippet.channelTitle
#   statistics.viewCount, statistics.likeCount
# We flatten all of these into top-level columns

df_flattened = (
    df_bronze
    # Top-level fields
    .select(
        F.col("id").alias("video_id"),

        # snippet fields — video metadata
        F.col("snippet.publishedAt").alias("published_at"),
        F.col("snippet.channelId").alias("channel_id"),
        F.col("snippet.title").alias("video_title"),
        F.col("snippet.description").alias("description"),
        F.col("snippet.channelTitle").alias("channel_title"),
        F.col("snippet.categoryId").alias("category_id"),
        F.col("snippet.defaultLanguage").alias("default_language"),
        F.col("snippet.defaultAudioLanguage").alias("audio_language"),
        F.col("snippet.liveBroadcastContent").alias("live_broadcast_content"),

        # statistics fields — engagement metrics
        F.col("statistics.viewCount").cast("long").alias("view_count"),
        F.col("statistics.likeCount").cast("long").alias("like_count"),
        F.col("statistics.dislikeCount").cast("long").alias("dislike_count"),
        F.col("statistics.favoriteCount").cast("long").alias("favorite_count"),
        F.col("statistics.commentCount").cast("long").alias("comment_count"),

        # contentDetails fields
        F.col("contentDetails.duration").alias("duration"),
        F.col("contentDetails.definition").alias("definition"),  # hd or sd
        F.col("contentDetails.caption").alias("has_caption"),

        # Pipeline metadata from Bronze
        F.col("_ingested_at"),
        F.col("_batch_date"),
        F.col("_source_system"),
    )
)

print(f"Flattened schema:")
df_flattened.printSchema()

# COMMAND ----------

# ── 6. Clean and enrich ──────────────────────────────────────────────
df_silver = (
    df_flattened

    # Parse published_at string into proper timestamp
    .withColumn("published_at",
        F.to_timestamp("published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

    # Extract date parts for partitioning and filtering
    .withColumn("published_year",  F.year("published_at"))
    .withColumn("published_month", F.month("published_at"))
    .withColumn("published_date",  F.to_date("published_at"))

    # Fill nulls with sensible defaults
    .fillna({
        "view_count":     0,
        "like_count":     0,
        "dislike_count":  0,
        "favorite_count": 0,
        "comment_count":  0,
        "has_caption":    "false",
        "definition":     "unknown",
    })

    # Derive engagement rate — likes per 1000 views
    # Avoid division by zero with greatest()
    .withColumn("engagement_rate",
        F.round(
            F.col("like_count") /
            F.greatest(F.col("view_count"), F.lit(1)) * 1000,
            4
        )
    )

    # Clean title — strip leading/trailing whitespace
    .withColumn("video_title", F.trim("video_title"))
    .withColumn("channel_title", F.trim("channel_title"))

    # Add Silver metadata
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_layer", F.lit("silver"))
)

# ── 7. Deduplicate ───────────────────────────────────────────────────
# YouTube API can return the same video across multiple API calls
# Keep the record with the highest view_count (most recent stats)

from pyspark.sql.window import Window

window_spec = (
    Window
    .partitionBy("video_id", "_batch_date")
    .orderBy(F.col("view_count").desc())
)

df_deduped = (
    df_silver
    .withColumn("_row_num", F.row_number().over(window_spec))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

raw_count   = df_silver.count()
dedup_count = df_deduped.count()
print(f"Before dedup : {raw_count}")
print(f"After dedup  : {dedup_count}")
print(f"Duplicates removed: {raw_count - dedup_count}")

# COMMAND ----------
# ── 8. Write to Silver Delta table ──────────────────────────────────
# Use MERGE (upsert) instead of append
# So re-running the pipeline for the same date doesn't create duplicates

from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
    # Table exists — merge (upsert) new records
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)

    (
        silver_table.alias("existing")
        .merge(
            df_deduped.alias("new"),
            "existing.video_id = new.video_id AND existing._batch_date = new._batch_date"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print("Silver merge (upsert) complete")

else:
    # First run — create the table
    (
        df_deduped
        .write
        .format("delta")
        .mode("overwrite")
        .partitionBy("published_year", "published_month")
        .save(SILVER_PATH)
    )
    print("Silver table created (first run)")

# COMMAND ----------
# ── 9. Register Silver table in metastore ───────────────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS youtube_pipeline.silver_videos
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

# COMMAND ----------

# ── 10. Data quality assertions ──────────────────────────────────────
df_check = spark.read.format("delta").load(SILVER_PATH)
total_rows = df_check.count()

# Assert no null video_ids
null_ids = df_check.filter(F.col("video_id").isNull()).count()
assert null_ids == 0, f"Data quality FAIL: {null_ids} records with null video_id"

# Assert view counts are non-negative
negative_views = df_check.filter(F.col("view_count") < 0).count()
assert negative_views == 0, f"Data quality FAIL: {negative_views} records with negative view_count"

print(f"=== Silver Layer Complete ===")
print(f"Total rows in silver table : {total_rows}")
print(f"Null video_id check        : PASSED")
print(f"Negative view_count check  : PASSED")
dbutils.notebook.exit(f"SUCCESS: {total_rows} rows in silver table")