data "azurerm_client_config" "current" {}

resource "azurerm_role_assignment" "adf_adls_contributor" {
  scope                = module.adls.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = module.adf.adf_principal_id
}

resource "azurerm_role_assignment" "adf_keyvault_reader" {
  scope                = "/subscriptions/${var.subscription_id}/resourceGroups/${var.resource_group_name}/providers/Microsoft.KeyVault/vaults/${var.key_vault_name}"
  role_definition_name = "Key Vault Secrets User"
  principal_id         = module.adf.adf_principal_id
}