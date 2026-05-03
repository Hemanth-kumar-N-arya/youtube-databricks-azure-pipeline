output "adls_account_name" {
  description = "ADLS Gen2 storage account name"
  value       = module.adls.storage_account_name
}

output "adls_dfs_endpoint" {
  description = "ADLS Gen2 DFS endpoint"
  value       = module.adls.primary_dfs_endpoint
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = module.databricks.databricks_workspace_url
}

output "adf_name" {
  description = "Azure Data Factory name"
  value       = module.adf.adf_name
}
output "monitor_action_group_id" {
  description = "Azure Monitor action group ID for pipeline alerts"
  value       = module.monitor.action_group_id
}

output "dashboard_id" {
  description = "Azure Portal dashboard ID"
  value       = module.monitor.dashboard_id
}