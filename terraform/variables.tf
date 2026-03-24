variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "taxi-etl"
}

variable "location" {
  description = "Region de Azure para la mayoria de recursos"
  type        = string
  default     = "East US"
}

variable "sql_location" {
  description = "Region de Azure para SQL Server (East US puede estar restringido)"
  type        = string
  default     = "West US 2"
}

variable "client_id" {
  description = "Azure Service Principal Client ID"
  type        = string
  sensitive   = true
}

variable "client_secret" {
  description = "Azure Service Principal Client Secret"
  type        = string
  sensitive   = true
}

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  type        = string
  sensitive   = true
}

variable "suffix" {
  description = "Sufijo unico para recursos"
  type        = string
}

variable "sql_admin_username" {
  description = "Usuario administrador de Azure SQL"
  type        = string
  default     = "sqladmin"
}

variable "sql_admin_password" {
  description = "Password del administrador de Azure SQL"
  type        = string
  sensitive   = true
}
