import json
import subprocess
import tempfile
import os
from datetime import datetime, timezone

SUB_ID   = "84e688d7-af3e-499e-bb47-1f58997ddf23"
RG       = "rg-ytpipeline-dev-eastus"
ADF_NAME = "adf-ytpipeline-dev"

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

result = subprocess.run(
    ["az", "datafactory", "pipeline", "create-run",
     "--factory-name", ADF_NAME,
     "--resource-group", RG,
     "--name", "pl_youtube_batch_ingestion",
     "--parameters", f"@{tmp}",
     "--query", "runId",
     "--output", "tsv"],
    capture_output=True, text=True, shell=True
)

os.unlink(tmp)

print(f"Batch date : {batch_date}")
print(f"Run ID     : {result.stdout.strip()}")
if result.stderr:
    print(f"Error      : {result.stderr[:300]}")