# COMMAND ----------
dbutils.widgets.text("adls_account_name", "adlsytpipelinedev001", "ADLS Account")
adls_account = dbutils.widgets.get("adls_account_name")

print("=== Delta Optimizations Starting ===")

# COMMAND ----------
sp_client_id     = dbutils.secrets.get(scope="kv-scope", key="sp-client-id")
sp_client_secret = dbutils.secrets.get(scope="kv-scope", key="sp-client-secret")
tenant_id        = dbutils.secrets.get(scope="kv-scope", key="tenant-id")

spark.conf.set(
    f"fs.azure.account.auth.type.{adls_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(
    f"fs.azure.account.oauth.provider.type.{adls_account}.dfs.core.windows.net",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(
    f"fs.azure.account.oauth2.client.id.{adls_account}.dfs.core.windows.net",
    sp_client_id)
spark.conf.set(
    f"fs.azure.account.oauth2.client.secret.{adls_account}.dfs.core.windows.net",
    sp_client_secret)
spark.conf.set(
    f"fs.azure.account.oauth2.client.endpoint.{adls_account}.dfs.core.windows.net",
    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

print("Auth configured")

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