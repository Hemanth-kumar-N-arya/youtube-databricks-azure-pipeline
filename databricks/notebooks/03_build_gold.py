# COMMAND ----------
# NOTEBOOK: 03_build_gold — Gold Layer
# PURPOSE:  Read from Silver, build aggregated business-ready tables
# LAYER:    Gold — pre-aggregated, analytics-ready
# COMMAND ----------

dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")
adls_account = dbutils.widgets.get("adls_account_name")

print(f"=== Gold Aggregation Starting ===")

# COMMAND ----------

# ── Configure ADLS access ────────────────────────────────────────────
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

# COMMAND ----------

# ── Define paths ─────────────────────────────────────────────────────
from pyspark.sql import functions as F

SILVER_PATH   = f"abfss://silver@{adls_account}.dfs.core.windows.net/youtube_videos/"
GOLD_BASE     = f"abfss://gold@{adls_account}.dfs.core.windows.net/"

GOLD_TOP_CHANNELS     = f"{GOLD_BASE}top_channels/"
GOLD_TRENDING_MONTHLY = f"{GOLD_BASE}trending_by_month/"
GOLD_CATEGORY_STATS   = f"{GOLD_BASE}category_engagement/"
GOLD_DAILY_SUMMARY    = f"{GOLD_BASE}daily_summary/"

# Read full Silver table — Gold aggregates across all history
df_silver = spark.read.format("delta").load(SILVER_PATH)
print(f"Silver total rows: {df_silver.count()}")

# COMMAND ----------

# ── Gold Table 1: Top Channels ────────────────────────────────────────
# Which channels have the most videos, highest total views,
# and best average engagement

df_top_channels = (
    df_silver
    .groupBy("channel_id", "channel_title")
    .agg(
        F.count("video_id").alias("total_videos"),
        F.sum("view_count").alias("total_views"),
        F.sum("like_count").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("view_count").alias("avg_views_per_video"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("published_date").alias("latest_video_date"),
        F.min("published_date").alias("first_video_date"),
    )
    .withColumn("avg_views_per_video",
        F.round("avg_views_per_video", 0).cast("long"))
    .withColumn("avg_engagement_rate",
        F.round("avg_engagement_rate", 4))
    .orderBy(F.col("total_views").desc())
    .withColumn("_gold_updated_at", F.current_timestamp())
)

(
    df_top_channels.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_TOP_CHANNELS)
)
print(f"Gold: top_channels written — {df_top_channels.count()} channels")

# COMMAND ----------

# ── Gold Table 2: Trending by Month ──────────────────────────────────
# How many videos trended each month, total views by month

df_trending_monthly = (
    df_silver
    .groupBy("published_year", "published_month", "category_id")
    .agg(
        F.count("video_id").alias("video_count"),
        F.sum("view_count").alias("total_views"),
        F.sum("like_count").alias("total_likes"),
        F.avg("view_count").alias("avg_views"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
    )
    .withColumn("year_month",
        F.concat_ws("-",
            F.col("published_year"),
            F.lpad(F.col("published_month").cast("string"), 2, "0")
        )
    )
    .withColumn("avg_views", F.round("avg_views", 0).cast("long"))
    .withColumn("avg_engagement_rate", F.round("avg_engagement_rate", 4))
    .orderBy("published_year", "published_month")
    .withColumn("_gold_updated_at", F.current_timestamp())
)

(
    df_trending_monthly.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_TRENDING_MONTHLY)
)
print(f"Gold: trending_by_month written — {df_trending_monthly.count()} rows")

# COMMAND ----------

# ── Gold Table 3: Category Engagement Stats ───────────────────────────
# Which video categories get the best engagement

df_category_stats = (
    df_silver
    .groupBy("category_id")
    .agg(
        F.count("video_id").alias("total_videos"),
        F.avg("view_count").alias("avg_views"),
        F.avg("like_count").alias("avg_likes"),
        F.avg("comment_count").alias("avg_comments"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.sum("view_count").alias("total_views"),
    )
    .withColumn("avg_views", F.round("avg_views", 0).cast("long"))
    .withColumn("avg_likes", F.round("avg_likes", 0).cast("long"))
    .withColumn("avg_comments", F.round("avg_comments", 0).cast("long"))
    .withColumn("avg_engagement_rate", F.round("avg_engagement_rate", 4))
    .orderBy(F.col("avg_engagement_rate").desc())
    .withColumn("_gold_updated_at", F.current_timestamp())
)

(
    df_category_stats.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_CATEGORY_STATS)
)
print(f"Gold: category_engagement written — {df_category_stats.count()} categories")

# COMMAND ----------

# ── Gold Table 4: Daily Pipeline Summary ─────────────────────────────
# One row per batch_date — high-level summary for monitoring dashboards

df_daily_summary = (
    df_silver
    .groupBy("_batch_date")
    .agg(
        F.count("video_id").alias("videos_processed"),
        F.countDistinct("channel_id").alias("unique_channels"),
        F.countDistinct("category_id").alias("unique_categories"),
        F.sum("view_count").alias("total_views"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("_silver_processed_at").alias("last_processed_at"),
    )
    .withColumn("avg_engagement_rate", F.round("avg_engagement_rate", 4))
    .orderBy(F.col("_batch_date").desc())
    .withColumn("_gold_updated_at", F.current_timestamp())
)

(
    df_daily_summary.write
    .format("delta")
    .mode("overwrite")
    .save(GOLD_DAILY_SUMMARY)
)
print(f"Gold: daily_summary written — {df_daily_summary.count()} rows")

# COMMAND ----------

# ── Register all Gold tables in metastore ───────────────────────────
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

print(f"=== Gold Layer Complete ===")
dbutils.notebook.exit("SUCCESS: All 4 Gold tables written and registered")