output "cosmos_account_id" {
  value = azurerm_cosmosdb_account.main.id
}

output "cosmos_endpoint" {
  value = azurerm_cosmosdb_account.main.endpoint
}

output "cosmos_primary_key" {
  value     = azurerm_cosmosdb_account.main.primary_key
  sensitive = true
}

output "cosmos_connection_string" {
  value     = azurerm_cosmosdb_account.main.primary_sql_connection_string
  sensitive = true
}