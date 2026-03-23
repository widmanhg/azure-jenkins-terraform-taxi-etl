resource "azurerm_databricks_workspace" "main" {
  name                = "${var.project_name}-databricks"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "trial" # Gratis 14 dias
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}