output "action_group_id" {
  value = azurerm_monitor_action_group.pipeline_alerts.id
}

output "dashboard_id" {
  value = azurerm_portal_dashboard.pipeline_dashboard.id
}