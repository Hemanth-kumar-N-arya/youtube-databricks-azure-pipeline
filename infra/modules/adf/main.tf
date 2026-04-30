resource "azurerm_data_factory" "main" {
  name                = var.adf_name
  location            = var.location
  resource_group_name = var.resource_group_name

  identity {
    type = "SystemAssigned"
  }

  github_configuration {
    account_name    = var.github_account_name
    branch_name     = "develop"
    git_url         = "https://github.com"
    repository_name = var.github_repo_name
    root_folder     = "/adf"
  }

  tags = var.tags
}