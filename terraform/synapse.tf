# Synapse - crear manualmente desde portal.azure.com
# La restriccion SqlServerRegionDoesNotAllowProvisioning
# requiere solicitar quota en support.azure.com


resource "azurerm_synapse_workspace" "main" {
  name                                 = "${var.project_name}-synapse-${var.suffix}"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = var.synapse_sql_password

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.main.id
}

# Permiso para que Synapse acceda al Storage
resource "azurerm_role_assignment" "synapse_storage" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.main.identity[0].principal_id
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.main.name
}