variable "resource_group_name" { type = string }
variable "location"            { type = string }
variable "environment"         { type = string }
variable "adf_id"              { type = string }
variable "adls_id"             { type = string }
variable "tags"                { type = map(string) }

variable "alert_email" {
  type        = string
  description = "Email address to receive pipeline alerts"
}