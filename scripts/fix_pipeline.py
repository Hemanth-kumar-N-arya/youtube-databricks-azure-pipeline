import json

path = r"adf\pipeline\pl_youtube_batch_ingestion.json"

with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

activities = data["properties"]["activities"]

for i, activity in enumerate(activities):
    if activity["name"] == "write_raw_to_adls":
        activities[i] = {
            "name": "write_raw_to_adls",
            "description": "Write raw YouTube API response to ADLS Gen2",
            "type": "WebActivity",
            "dependsOn": [
                {
                    "activity": "fetch_youtube_trending",
                    "dependencyConditions": ["Succeeded"]
                }
            ],
            "typeProperties": {
                "url": {
                    "value": "@concat('https://adlsytpipelinedev001.dfs.core.windows.net/raw/youtube/', pipeline().parameters.batch_date, '/trending_videos_', pipeline().parameters.batch_date, '.json')",
                    "type": "Expression"
                },
                "method": "PUT",
                "headers": {
                    "Content-Type": "application/json",
                    "x-ms-version": "2020-04-08",
                    "x-ms-blob-type": "BlockBlob"
                },
                "body": {
                    "value": "@string(activity('fetch_youtube_trending').output)",
                    "type": "Expression"
                },
                "authentication": {
                    "type": "MSI",
                    "resource": "https://storage.azure.com/"
                }
            }
        }
        print("Fixed write_raw_to_adls activity")
        break

with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print("Pipeline JSON saved successfully")