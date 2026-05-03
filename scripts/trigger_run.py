import json
import subprocess
import tempfile
import os
from datetime import datetime, timezone

ADF_NAME = "adf-ytpipeline-dev"
RG       = "rg-ytpipeline-dev-eastus"
SUB_ID   = os.environ.get(
    "AZURE_SUBSCRIPTION_ID",
    "84e688d7-af3e-499e-bb47-1f58997ddf23"
)

batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

params = {
    "batch_date": batch_date,
    "adls_account_name": "adlsytpipelinedev001"
}

url = (
    f"https://management.azure.com/subscriptions/{SUB_ID}"
    f"/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{ADF_NAME}"
    f"/pipelines/pl_youtube_batch_ingestion/createRun"
    f"?api-version=2018-06-01"
)

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False, encoding="utf-8"
) as f:
    json.dump(params, f)
    body_tmp = f.name

# Suppress Azure CLI welcome message
env = os.environ.copy()
env["AZURE_CORE_COLLECT_TELEMETRY"] = "false"
env["AZURE_CORE_SURVEY_MESSAGE"] = "false"

result = subprocess.run(
    ["az", "rest",
     "--method", "POST",
     "--url", url,
     "--body", f"@{body_tmp}",
     "--headers", "Content-Type=application/json",
     "--output", "json"],
    capture_output=True,
    text=True,
    shell=True,
    env=env
)

os.unlink(body_tmp)

print(f"Batch date : {batch_date}")

# Parse only JSON from stdout — ignore any banner text
stdout = result.stdout.strip()
run_id = None

# Find JSON object in output
for line in stdout.split("\n"):
    line = line.strip()
    if line.startswith("{"):
        try:
            response = json.loads(line)
            run_id = response.get("runId")
            break
        except Exception:
            continue

if run_id:
    print(f"Run ID     : {run_id}")
    print(f"Status     : ADF pipeline triggered successfully")
    print(f"Monitor at : https://adf.azure.com")
elif result.returncode == 0:
    print(f"Response   : {stdout[:300]}")
    print("Status     : Request sent — check ADF Monitor for run status")
else:
    print(f"Error      : {result.stderr[:300]}")
    exit(1)