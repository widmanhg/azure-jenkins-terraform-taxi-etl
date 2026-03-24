resource "azurerm_mssql_server" "main" {
  name                         = "${var.project_name}-sql-${var.suffix}"
  resource_group_name          = azurerm_resource_group.main.name
  location                     = azurerm_resource_group.main.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = var.sql_admin_password

  tags = {
    project = var.project_name
  }
}

resource "azurerm_mssql_database" "main" {
  name      = "${var.project_name}-db"
  server_id = azurerm_mssql_server.main.id
  sku_name  = "Basic"

  tags = {
    project = var.project_name
  }
}

# Permite acceso desde servicios de Azure (Databricks, ADF)
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.main.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

output "sql_server_fqdn" {
  value = azurerm_mssql_server.main.fully_qualified_domain_name
}

output "sql_database_name" {
  value = azurerm_mssql_database.main.name
}

output "sql_admin_username" {
  value = var.sql_admin_username
}
