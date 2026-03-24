output "storage_connection_string" {
  value     = azurerm_storage_account.main.primary_connection_string
  sensitive = true
}

