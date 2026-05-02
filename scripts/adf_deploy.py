import json
import os
import subprocess
import tempfile

ADF_NAME = "adf-ytpipeline-dev"
RG       = "rg-ytpipeline-dev-eastus"
SUB_ID   = os.environ.get("AZURE_SUBSCRIPTION_ID", "84e688d7-af3e-499e-bb47-1f58997ddf23")

def get_subscription_id():
    result = subprocess.run(
        ["az", "account", "show", "--query", "id", "--output", "tsv"],
        capture_output=True, text=True, shell=True
    )
    return result.stdout.strip()

def deploy_resource(resource_type, name, file_path):
    print(f"Deploying {resource_type}: {name}...")

    with open(file_path, "r", encoding="utf-8") as f:
        full_json = json.load(f)

    props  = full_json.get("properties", full_json)
    sub_id = get_subscription_id()

    url_map = {
        "linkedservice": f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/linkedservices/{name}?api-version=2018-06-01",
        "dataset":       f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/datasets/{name}?api-version=2018-06-01",
        "pipeline":      f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/pipelines/{name}?api-version=2018-06-01",
        "trigger":       f"https://management.azure.com/subscriptions/{sub_id}/resourceGroups/{RG}/providers/Microsoft.DataFactory/factories/{ADF_NAME}/triggers/{name}?api-version=2018-06-01",
    }

    url  = url_map[resource_type]
    body = {"properties": props}

    # Write body to a temp file — avoids ALL shell quoting/escaping issues
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        json.dump(body, tmp, ensure_ascii=False)
        tmp_path = tmp.name

    try:
        result = subprocess.run(
            ["az", "rest",
             "--method", "PUT",
             "--url", url,
             "--body", f"@{tmp_path}",
             "--headers", "Content-Type=application/json"],
            capture_output=True, text=True, shell=True
        )

        if result.returncode == 0:
            print(f"  SUCCESS: {name}")
            return True
        else:
            # Show first 400 chars of error
            err = result.stderr or result.stdout
            print(f"  FAILED: {name}")
            print(f"  Error: {err[:400]}")
            return False
    finally:
        os.unlink(tmp_path)  # Always clean up temp file

# ── Deploy in order ──────────────────────────────────────────────────
# ── Deploy in order ──────────────────────────────────────────────────
print("=== Deploying ADF Resources ===\n")

deploy_resource("linkedservice", "ls_keyvault",           "adf/linkedService/ls_keyvault.json")
deploy_resource("linkedservice", "ls_adls_gen2",          "adf/linkedService/ls_adls_gen2.json")
deploy_resource("linkedservice", "ls_databricks",         "adf/linkedService/ls_databricks.json")
deploy_resource("linkedservice", "ls_youtube_api",        "adf/linkedService/ls_youtube_api.json")

deploy_resource("dataset", "ds_youtube_api_response",     "adf/dataset/ds_youtube_api_response.json")
deploy_resource("dataset", "ds_adls_raw_json",            "adf/dataset/ds_adls_raw_json.json")

deploy_resource("pipeline", "pl_youtube_batch_ingestion", "adf/pipeline/pl_youtube_batch_ingestion.json")

deploy_resource("trigger",  "tr_daily_schedule",          "adf/trigger/tr_daily_schedule.json")

print("\n=== Deployment Complete ===")