# COMMAND ----------
dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")
adls_account = dbutils.widgets.get("adls_account_name")

# Unity Catalog manages ADLS Gen2 access via External Locations
# No Spark OAuth config needed — storage credential handles auth
print('Storage access via Unity Catalog — account: ' + adls_account)


print("=== Delta Optimizations Starting ===")

# COMMAND ----------

# Unity Catalog handles ADLS authentication automatically
# No Spark OAuth config needed with Premium Databricks + External Locations
print(f"Using ADLS account: {adls_account}")
print("Storage access via Unity Catalog External Location")

# COMMAND ----------
spark.sql("""
    OPTIMIZE youtube_pipeline.silver_videos
    ZORDER BY (channel_id, video_id, published_date)
""")
print("Silver OPTIMIZE + ZORDER complete")

# COMMAND ----------
gold_tables = [
    "youtube_pipeline.gold_top_channels",
    "youtube_pipeline.gold_trending_monthly",
    "youtube_pipeline.gold_category_stats",
    "youtube_pipeline.gold_daily_summary",
]

for table in gold_tables:
    spark.sql(f"OPTIMIZE {table}")
    print(f"Optimized: {table}")

# COMMAND ----------
spark.sql("VACUUM youtube_pipeline.silver_videos RETAIN 168 HOURS")
print("Silver VACUUM complete")

for table in gold_tables:
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
    print(f"Vacuumed: {table}")

# COMMAND ----------
print("=== Delta Optimizations Complete ===")
dbutils.notebook.exit("SUCCESS: All tables optimized and vacuumed")