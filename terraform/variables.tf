variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "taxi-etl"
}

variable "location" {
  description = "Region de Azure"
  type        = string
  default     = "East US"
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

variable "synapse_sql_password" {
  description = "Password para Synapse SQL admin"
  type        = string
  sensitive   = true
}