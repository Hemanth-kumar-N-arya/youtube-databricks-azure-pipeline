# COMMAND ----------
# NOTEBOOK: 04_optimize_delta — Delta Lake Optimizations
# PURPOSE:  Run OPTIMIZE, Z-ORDER, and VACUUM on Silver and Gold tables
#           to improve query performance and control storage costs
# RUN:      After Gold notebook completes — once per pipeline run
# COMMAND ----------

dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")
adls_account = dbutils.widgets.get("adls_account_name")

print("=== Delta Optimizations Starting ===")

# COMMAND ----------

# Configure ADLS access
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

# ── OPTIMIZE + Z-ORDER on Silver ─────────────────────────────────────
# OPTIMIZE compacts many small files into fewer large files
# Z-ORDER physically co-locates rows with the same channel_id and video_id
# in the same files — so queries filtering by these columns read far less data

print("Optimizing Silver table...")
spark.sql("""
    OPTIMIZE youtube_pipeline.silver_videos
    ZORDER BY (channel_id, video_id, published_date)
""")
print("Silver OPTIMIZE + ZORDER complete")

# COMMAND ----------

# ── OPTIMIZE Gold tables ─────────────────────────────────────────────
# Gold tables are small so Z-ORDER is less critical
# but OPTIMIZE still helps compact files written by Spark

gold_tables = [
    "youtube_pipeline.gold_top_channels",
    "youtube_pipeline.gold_trending_monthly",
    "youtube_pipeline.gold_category_stats",
    "youtube_pipeline.gold_daily_summary",
]

for table in gold_tables:
    print(f"Optimizing {table}...")
    spark.sql(f"OPTIMIZE {table}")
    print(f"{table} optimized")

# COMMAND ----------

# ── VACUUM Silver and Gold ────────────────────────────────────────────
# VACUUM deletes old Delta file versions beyond the retention window
# Default retention is 7 days — files older than 7 days get deleted
# This controls storage costs on ADLS Gen2

# IMPORTANT: Never set retention below 7 days in production
# as it breaks Delta time travel

print("Running VACUUM on Silver...")
spark.sql("VACUUM youtube_pipeline.silver_videos RETAIN 168 HOURS")
print("Silver VACUUM complete")

for table in gold_tables:
    print(f"Running VACUUM on {table}...")
    spark.sql(f"VACUUM {table} RETAIN 168 HOURS")
    print(f"{table} VACUUM complete")

# COMMAND ----------

# ── Print final table stats ──────────────────────────────────────────
print("\n=== Table Statistics After Optimization ===")
for table in ["youtube_pipeline.silver_videos"] + gold_tables:
    detail = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]
    print(f"{table}:")
    print(f"  Files     : {detail['numFiles']}")
    print(f"  Size bytes: {detail['sizeInBytes']:,}")
    print()

print("=== Delta Optimizations Complete ===")
dbutils.notebook.exit("SUCCESS: All tables optimized and vacuumed")