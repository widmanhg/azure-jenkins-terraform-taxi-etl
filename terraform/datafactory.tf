resource "azurerm_data_factory" "main" {
  name                = "${var.project_name}-adf-${var.suffix}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  identity {
    type = "SystemAssigned"
  }
}

# Permiso para que Data Factory acceda al Storage
resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.main.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.main.identity[0].principal_id
}

output "data_factory_name" {
  value = azurerm_data_factory.main.name
}