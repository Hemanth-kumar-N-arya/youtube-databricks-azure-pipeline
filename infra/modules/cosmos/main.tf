resource "azurerm_cosmosdb_account" "main" {
  name                = var.cosmos_account_name
  location            = "westus"
  resource_group_name = var.resource_group_name
  offer_type          = "Standard"
  kind                = "GlobalDocumentDB"

  consistency_policy {
    consistency_level = "Session"
  }

  geo_location {
    location          = "westus"
    failover_priority = 0
  }

  capabilities {
    name = "EnableServerless"
  }

  tags = var.tags
}

resource "azurerm_cosmosdb_sql_database" "pipeline_db" {
  name                = "pipeline-metadata"
  resource_group_name = var.resource_group_name
  account_name        = azurerm_cosmosdb_account.main.name
}

resource "azurerm_cosmosdb_sql_container" "ingestion_log" {
  name                  = "ingestion-log"
  resource_group_name   = var.resource_group_name
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.pipeline_db.name
  partition_key_paths   = ["/batch_date"]
  partition_key_version = 2
}

resource "azurerm_cosmosdb_sql_container" "job_runs" {
  name                  = "job-runs"
  resource_group_name   = var.resource_group_name
  account_name          = azurerm_cosmosdb_account.main.name
  database_name         = azurerm_cosmosdb_sql_database.pipeline_db.name
  partition_key_paths   = ["/run_id"]
  partition_key_version = 2
}