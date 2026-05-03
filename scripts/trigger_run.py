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

# Write params to temp file
with tempfile.NamedTemporaryFile(
    mode="w", suffix=".json",
    delete=False, encoding="utf-8"
) as f:
    json.dump(params, f)
    tmp = f.name

print(f"Batch date : {batch_date}")
print(f"Triggering : {ADF_NAME}/{RG}")

# Use datafactory extension directly — already authenticated via azure/login action
result = subprocess.run(
    ["az", "datafactory", "pipeline", "create-run",
     "--factory-name", ADF_NAME,
     "--resource-group", RG,
     "--name", "pl_youtube_batch_ingestion",
     "--parameters", f"@{tmp}"],
    capture_output=True,
    text=True
)

os.unlink(tmp)

print(f"Return code: {result.returncode}")
print(f"Stdout     : {result.stdout.strip()[:500]}")

if result.stderr:
    print(f"Stderr     : {result.stderr.strip()[:300]}")

if result.returncode == 0:
    try:
        response = json.loads(result.stdout)
        run_id = response.get("runId", "unknown")
        print(f"Run ID     : {run_id}")
        print(f"Status     : SUCCESS — pipeline triggered")
    except Exception:
        print(f"Status     : Request sent")
else:
    print("Status     : FAILED to trigger pipeline")
    exit(1)