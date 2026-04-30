import json
import subprocess
import tempfile
import os

SUB_ID   = "84e688d7-af3e-499e-bb47-1f58997ddf23"
RG       = "rg-ytpipeline-dev-eastus"
ADF_NAME = "adf-ytpipeline-dev"

with open(r"adf\pipeline\pl_youtube_batch_ingestion.json", "r", encoding="utf-8") as f:
    full = json.load(f)

props = full.get("properties", full)
body  = {"properties": props}

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False, encoding="utf-8"
) as f:
    json.dump(body, f, ensure_ascii=False)
    tmp = f.name

url = (
    f"https://management.azure.com/subscriptions/{SUB_ID}"
    f"/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{ADF_NAME}"
    f"/pipelines/pl_youtube_batch_ingestion?api-version=2018-06-01"
)

result = subprocess.run(
    ["az", "rest",
     "--method", "PUT",
     "--url", url,
     "--body", f"@{tmp}",
     "--headers", "Content-Type=application/json"],
    capture_output=True, text=True, shell=True
)

os.unlink(tmp)

if result.returncode == 0:
    print("Pipeline deployed successfully")
else:
    print("FAILED:")
    print(result.stderr[:500])