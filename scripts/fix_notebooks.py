import re
import os

notebooks = [
    r"databricks\notebooks\01_ingest_raw.py",
    r"databricks\notebooks\02_transform_silver.py",
    r"databricks\notebooks\03_build_gold.py",
    r"databricks\notebooks\04_optimize_delta.py",
]

# Lines to remove from all notebooks
remove_patterns = [
    "sp_client_id",
    "sp_client_secret",
    "tenant_id",
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
    "ClientCredsTokenProvider",
    "login.microsoftonline.com",
    "dbutils.secrets.get(scope",
]

for notebook_path in notebooks:
    with open(notebook_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    new_lines = []
    for line in lines:
        should_remove = any(pattern in line for pattern in remove_patterns)
        if not should_remove:
            new_lines.append(line)

    # Add Unity Catalog comment after widgets section
    unity_comment = (
        "\n# Unity Catalog manages ADLS Gen2 access via External Locations\n"
        "# No Spark OAuth config needed — storage credential handles auth\n"
        f"print('Storage access via Unity Catalog — account: ' + adls_account)\n\n"
    )

    # Insert after the widget lines
    insert_idx = 0
    for i, line in enumerate(new_lines):
        if "adls_account_name" in line and "widgets.get" in line:
            insert_idx = i + 1
            break

    new_lines.insert(insert_idx, unity_comment)

    with open(notebook_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    print(f"Fixed: {notebook_path}")

print("\nAll notebooks updated — Spark OAuth removed, Unity Catalog enabled")