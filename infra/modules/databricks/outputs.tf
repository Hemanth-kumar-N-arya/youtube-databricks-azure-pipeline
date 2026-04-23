output "databricks_workspace_id"  { value = azurerm_databricks_workspace.main.id }
output "databricks_workspace_url" { value = "https://${azurerm_databricks_workspace.main.workspace_url}" }
