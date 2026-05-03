# ── Action Group — who gets notified ────────────────────────────────
resource "azurerm_monitor_action_group" "pipeline_alerts" {
  name                = "ag-ytpipeline-${var.environment}"
  resource_group_name = var.resource_group_name
  short_name          = "ytpipeline"

  email_receiver {
    name                    = "pipeline-owner"
    email_address           = var.alert_email
    use_common_alert_schema = true
  }

  tags = var.tags
}

# ── Alert 1: ADF Pipeline Failed ────────────────────────────────────
resource "azurerm_monitor_metric_alert" "adf_pipeline_failed" {
  name                = "alert-adf-pipeline-failed"
  resource_group_name = var.resource_group_name
  scopes              = [var.adf_id]
  description         = "Alert when ADF pipeline run fails"
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

# ── Alert 2: ADF Pipeline Success ───────────────────────────────────
resource "azurerm_monitor_metric_alert" "adf_pipeline_success" {
  name                = "alert-adf-pipeline-success"
  resource_group_name = var.resource_group_name
  scopes              = [var.adf_id]
  description         = "Track successful pipeline runs"
  severity            = 3
  frequency           = "PT5M"
  window_size         = "PT15M"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "PipelineSucceededRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

# ── Alert 3: ADF Activity Failed ────────────────────────────────────
resource "azurerm_monitor_metric_alert" "adf_activity_failed" {
  name                = "alert-adf-activity-failed"
  resource_group_name = var.resource_group_name
  scopes              = [var.adf_id]
  description         = "Alert when any ADF activity fails"
  severity            = 1
  frequency           = "PT5M"
  window_size         = "PT15M"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.DataFactory/factories"
    metric_name      = "ActivityFailedRuns"
    aggregation      = "Total"
    operator         = "GreaterThan"
    threshold        = 0
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

# ── Alert 4: ADLS Storage capacity ──────────────────────────────────
resource "azurerm_monitor_metric_alert" "adls_capacity" {
  name                = "alert-adls-capacity"
  resource_group_name = var.resource_group_name
  scopes              = [var.adls_id]
  description         = "Alert when ADLS storage exceeds 80% of free tier limit"
  severity            = 2
  frequency           = "PT1H"
  window_size         = "PT6H"
  enabled             = true

  criteria {
    metric_namespace = "Microsoft.Storage/storageAccounts"
    metric_name      = "UsedCapacity"
    aggregation      = "Average"
    operator         = "GreaterThan"
    threshold        = 4294967296  # 4GB in bytes
  }

  action {
    action_group_id = azurerm_monitor_action_group.pipeline_alerts.id
  }

  tags = var.tags
}

# ── Dashboard ────────────────────────────────────────────────────────
resource "azurerm_portal_dashboard" "pipeline_dashboard" {
  name                = "dash-ytpipeline-${var.environment}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags

  dashboard_properties = jsonencode({
    lenses = {
      "0" = {
        order = 0
        parts = {
          "0" = {
            position = {
              x        = 0
              y        = 0
              rowSpan  = 4
              colSpan  = 6
            }
            metadata = {
              type = "Extension/HubsExtension/PartType/MarkdownPart"
              settings = {
                content = {
                  settings = {
                    content  = "## YouTube Pipeline Dashboard\n\n**Project:** youtube-databricks-azure-pipeline\n\n**Stack:** ADF → ADLS Gen2 → Databricks → Delta Lake\n\n**Schedule:** Daily at 06:00 UTC\n\n**Layers:** Bronze → Silver → Gold"
                    title    = "Pipeline Overview"
                    subtitle = "Azure + Databricks Data Pipeline"
                  }
                }
              }
            }
          }
        }
      }
    }
    metadata = {
      model = {}
    }
  })
}