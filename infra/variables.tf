variable "project" {
  description = "Short project name used in all resource names"
  type        = string
  default     = "ytpipeline"
}

variable "environment" {
  description = "Deployment environment — dev, staging, prod"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Name of the existing resource group"
  type        = string
  default     = "rg-ytpipeline-dev-eastus"
}

variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure Active Directory tenant ID"
  type        = string
  sensitive   = true
}

variable "sp_client_id" {
  description = "Service principal client ID"
  type        = string
  sensitive   = true
}

variable "key_vault_name" {
  description = "Name of the existing Key Vault"
  type        = string
  default     = "kv-ytpipeline-dev-001"
}

variable "adls_account_name" {
  description = "ADLS Gen2 storage account name — must be globally unique"
  type        = string
  default     = "adlsytpipelinedev001"
}

variable "databricks_workspace_name" {
  description = "Name of the Databricks workspace"
  type        = string
  default     = "dbw-ytpipeline-dev"
}

variable "adf_name" {
  description = "Azure Data Factory name"
  type        = string
  default     = "adf-ytpipeline-dev"
}


variable "tags" {
  description = "Tags applied to all resources"
  type        = map(string)
  default = {
    Project     = "YouTubePipeline"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}