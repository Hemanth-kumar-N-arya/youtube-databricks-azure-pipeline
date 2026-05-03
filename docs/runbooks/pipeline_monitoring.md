# Pipeline Monitoring Runbook

## Overview
This runbook describes how to monitor and troubleshoot the YouTube batch pipeline.

## Daily Health Check
1. Go to ADF Studio → Monitor → Pipeline runs
2. Verify `pl_youtube_batch_ingestion` shows Succeeded for today
3. Check Bronze, Silver, Gold row counts in Databricks SQL

## Alert Response Procedures

### Alert: ADF Pipeline Failed (Severity 1)
1. Go to ADF Studio → Monitor → Pipeline runs
2. Click the failed run → identify which activity failed
3. Click the activity → view error details
4. Common fixes:
   - Databricks cluster terminated → restart cluster
   - YouTube API quota exceeded → wait 24 hours
   - ADLS permission error → check managed identity roles

### Alert: ADF Activity Failed (Severity 1)
1. Open the Databricks run URL from the error details
2. Identify the failed cell in the notebook
3. Check Key Vault secrets are still valid
4. Rerun from ADF Monitor using Rerun button

### Alert: ADLS Capacity (Severity 2)
1. Go to ADLS Gen2 → Storage browser
2. Check which container is largest
3. Run VACUUM in Databricks to clean old Delta versions:
   spark.sql("VACUUM youtube_pipeline.silver_videos RETAIN 168 HOURS")

## Useful Queries in Databricks SQL

### Check row counts across all layers
SELECT 'bronze' as layer, COUNT(*) as rows
FROM youtube_pipeline.bronze_raw
UNION ALL
SELECT 'silver', COUNT(*) FROM youtube_pipeline.silver_videos
UNION ALL
SELECT 'gold_channels', COUNT(*) FROM youtube_pipeline.gold_top_channels;

### Check latest batch processed
SELECT MAX(_batch_date) as latest_batch,
       COUNT(*) as videos_in_latest_batch
FROM youtube_pipeline.silver_videos
WHERE _batch_date = (SELECT MAX(_batch_date) FROM youtube_pipeline.silver_videos);

### Top 10 videos by views
SELECT video_title, channel_title, view_count, engagement_rate
FROM youtube_pipeline.silver_videos
ORDER BY view_count DESC
LIMIT 10;

## Pipeline Schedule
- Runs daily at 06:00 UTC
- Average duration: 15-20 minutes
- Data available by 06:30 UTC daily

## Key Resources
- ADF Studio: https://adf.azure.com
- Databricks: https://adb-7405614625291861.1.azuredatabricks.net
- Azure Monitor: https://portal.azure.com/#blade/Microsoft_Azure_Monitoring
- GitHub Actions: https://github.com/Hemanth-kumar-N-arya/youtube-databricks-azure-pipeline/actions