resource "azurerm_databricks_workspace" "main" {
  name                        = var.databricks_workspace_name
  resource_group_name         = var.resource_group_name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "rg-${var.databricks_workspace_name}-managed"

  tags = var.tags
}