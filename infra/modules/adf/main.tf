resource "azurerm_data_factory" "main" {
  name                = var.adf_name
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}