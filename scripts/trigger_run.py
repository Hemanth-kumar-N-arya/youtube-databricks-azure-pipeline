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

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False, encoding="utf-8"
) as f:
    json.dump(params, f)
    tmp = f.name

# Use az rest directly instead of datafactory extension
# This avoids the CLI welcome message issue
url = (
    f"https://management.azure.com/subscriptions/{SUB_ID}"
    f"/resourceGroups/{RG}"
    f"/providers/Microsoft.DataFactory/factories/{ADF_NAME}"
    f"/pipelines/pl_youtube_batch_ingestion/createRun"
    f"?api-version=2018-06-01"
)

body = json.dumps(params)

with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json", delete=False, encoding="utf-8"
) as f:
    json.dump(params, f)
    body_tmp = f.name

result = subprocess.run(
    ["az", "rest",
     "--method", "POST",
     "--url", url,
     "--body", f"@{body_tmp}",
     "--headers", "Content-Type=application/json"],
    capture_output=True, text=True, shell=True
)

os.unlink(tmp)
os.unlink(body_tmp)

print(f"Batch date : {batch_date}")

if result.returncode == 0:
    try:
        response = json.loads(result.stdout)
        run_id = response.get("runId", "unknown")
        print(f"Run ID     : {run_id}")
        print(f"Status     : Pipeline triggered successfully")
    except Exception:
        print(f"Response   : {result.stdout[:200]}")
else:
    print(f"Error      : {result.stderr[:300]}")
    exit(1)