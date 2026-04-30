# COMMAND ----------
dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")
adls_account = dbutils.widgets.get("adls_account_name")

print("=== Gold Aggregation Starting ===")

# COMMAND ----------
# Unity Catalog handles ADLS authentication automatically
# No Spark OAuth config needed with Premium Databricks + External Locations
print(f"Using ADLS account: {adls_account}")
print("Storage access via Unity Catalog External Location")

# COMMAND ----------
from pyspark.sql import functions as F

SILVER_PATH           = f"abfss://silver@{adls_account}.dfs.core.windows.net/youtube_videos/"
GOLD_TOP_CHANNELS     = f"abfss://gold@{adls_account}.dfs.core.windows.net/top_channels/"
GOLD_TRENDING_MONTHLY = f"abfss://gold@{adls_account}.dfs.core.windows.net/trending_by_month/"
GOLD_CATEGORY_STATS   = f"abfss://gold@{adls_account}.dfs.core.windows.net/category_engagement/"
GOLD_DAILY_SUMMARY    = f"abfss://gold@{adls_account}.dfs.core.windows.net/daily_summary/"

df_silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver total rows: {df_silver.count()}")

# COMMAND ----------
(
    df_silver
    .groupBy("channel_id", "channel_title")
    .agg(
        F.count("video_id").alias("total_videos"),
        F.sum("view_count").alias("total_views"),
        F.sum("like_count").alias("total_likes"),
        F.avg("view_count").cast("long").alias("avg_views_per_video"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").save(GOLD_TOP_CHANNELS)
)
print("Gold: top_channels written")

# COMMAND ----------
(
    df_silver
    .groupBy("published_year", "published_month", "category_id")
    .agg(
        F.count("video_id").alias("video_count"),
        F.sum("view_count").alias("total_views"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
    )
    .withColumn("year_month",
        F.concat_ws("-",
            F.col("published_year"),
            F.lpad(F.col("published_month").cast("string"), 2, "0")))
    .withColumn("_gold_updated_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").save(GOLD_TRENDING_MONTHLY)
)
print("Gold: trending_by_month written")

# COMMAND ----------
(
    df_silver
    .groupBy("category_id")
    .agg(
        F.count("video_id").alias("total_videos"),
        F.avg("view_count").alias("avg_views"),
        F.avg("like_count").alias("avg_likes"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.sum("view_count").alias("total_views"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").save(GOLD_CATEGORY_STATS)
)
print("Gold: category_engagement written")

# COMMAND ----------
(
    df_silver
    .groupBy("_batch_date")
    .agg(
        F.count("video_id").alias("videos_processed"),
        F.countDistinct("channel_id").alias("unique_channels"),
        F.sum("view_count").alias("total_views"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
    )
    .withColumn("_gold_updated_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").save(GOLD_DAILY_SUMMARY)
)
print("Gold: daily_summary written")

# COMMAND ----------
for table_name, path in [
    ("gold_top_channels",     GOLD_TOP_CHANNELS),
    ("gold_trending_monthly", GOLD_TRENDING_MONTHLY),
    ("gold_category_stats",   GOLD_CATEGORY_STATS),
    ("gold_daily_summary",    GOLD_DAILY_SUMMARY),
]:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS youtube_pipeline.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"Registered: youtube_pipeline.{table_name}")

print("=== Gold Complete ===")
dbutils.notebook.exit("SUCCESS: All 4 Gold tables written and registered")