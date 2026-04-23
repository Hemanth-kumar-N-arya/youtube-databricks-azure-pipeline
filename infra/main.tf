data "azurerm_resource_group" "main" {
  name = var.resource_group_name
}

module "adls" {
  source              = "./modules/adls"
  adls_account_name   = var.adls_account_name
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags
}

module "adf" {
  source              = "./modules/adf"
  adf_name            = var.adf_name
  resource_group_name = var.resource_group_name
  location            = var.location
  github_account_name = "YOUR_GITHUB_USERNAME"
  github_repo_name    = "youtube-databricks-azure-pipeline"
  tags                = var.tags
}

module "databricks" {
  source                    = "./modules/databricks"
  databricks_workspace_name = var.databricks_workspace_name
  resource_group_name       = var.resource_group_name
  location                  = var.location
  tags                      = var.tags
}