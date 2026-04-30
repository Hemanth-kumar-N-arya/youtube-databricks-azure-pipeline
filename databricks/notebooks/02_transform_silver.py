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

print(f"=== Silver Transformation Starting ===")
print(f"Batch date: {batch_date}")

# COMMAND ----------

# Unity Catalog handles ADLS authentication automatically
# No Spark OAuth config needed with Premium Databricks + External Locations
print(f"Using ADLS account: {adls_account}")
print("Storage access via Unity Catalog External Location")

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BRONZE_PATH = f"abfss://bronze@{adls_account}.dfs.core.windows.net/youtube_raw/"
SILVER_PATH = f"abfss://silver@{adls_account}.dfs.core.windows.net/youtube_videos/"

df_bronze = (
    spark.read
    .format("delta")
    .load(BRONZE_PATH)
    .filter(F.col("_batch_date") == batch_date)
)

count = df_bronze.count()
print(f"Bronze records for {batch_date}: {count}")

if count == 0:
    dbutils.notebook.exit(f"WARNING: No bronze records for {batch_date}")

# COMMAND ----------
df_silver = (
    df_bronze.select(
        F.col("id").alias("video_id"),
        F.col("snippet.publishedAt").alias("published_at"),
        F.col("snippet.channelId").alias("channel_id"),
        F.col("snippet.title").alias("video_title"),
        F.col("snippet.channelTitle").alias("channel_title"),
        F.col("snippet.categoryId").alias("category_id"),
        F.col("statistics.viewCount").cast("long").alias("view_count"),
        F.col("statistics.likeCount").cast("long").alias("like_count"),
        F.col("statistics.commentCount").cast("long").alias("comment_count"),
        F.col("contentDetails.duration").alias("duration"),
        F.col("contentDetails.definition").alias("definition"),
        F.col("_ingested_at"),
        F.col("_batch_date"),
        F.col("_source_system"),
    )
    .withColumn("published_at",
        F.to_timestamp("published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("published_year",  F.year("published_at"))
    .withColumn("published_month", F.month("published_at"))
    .withColumn("published_date",  F.to_date("published_at"))
    .fillna({
        "view_count":   0,
        "like_count":   0,
        "comment_count":0,
        "definition":   "unknown"
    })
    .withColumn("engagement_rate",
        F.round(
            F.col("like_count") /
            F.greatest(F.col("view_count"), F.lit(1)) * 1000,
        4))
    .withColumn("video_title",   F.trim("video_title"))
    .withColumn("channel_title", F.trim("channel_title"))
    .withColumn("_silver_processed_at", F.current_timestamp())
    .withColumn("_layer", F.lit("silver"))
)

print("Flattening complete")

# COMMAND ----------
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

print(f"After dedup: {df_deduped.count()} records")

# COMMAND ----------
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, SILVER_PATH):
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
    print("Silver merge complete")
else:
    (
        df_deduped.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("published_year", "published_month")
        .save(SILVER_PATH)
    )
    print("Silver table created on first run")

# COMMAND ----------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS youtube_pipeline.silver_videos
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

row_count = spark.read.format("delta").load(SILVER_PATH).count()

null_ids = spark.read.format("delta").load(SILVER_PATH).filter(
    F.col("video_id").isNull()).count()
assert null_ids == 0, f"Data quality FAIL: {null_ids} null video_ids"

print(f"=== Silver Complete: {row_count} rows ===")
dbutils.notebook.exit(f"SUCCESS: {row_count} rows in silver table")