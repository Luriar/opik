variable "name_prefix" {
  description = "Name prefix for storage resources."
  type        = string
}

variable "bucket_name" {
  description = "S3 data lake bucket name."
  type        = string
}

variable "create_bucket" {
  description = "Create and manage the bucket. Set false to reference an existing externally-managed bucket of this name without applying any configuration to it."
  type        = bool
  default     = true
}

variable "enable_s3_lifecycle" {
  description = "Enable S3 lifecycle for noncurrent versions and incomplete multipart uploads."
  type        = bool
}

variable "force_destroy_bucket" {
  description = "Allow Terraform destroy to delete non-empty bucket. Keep false for real data."
  type        = bool
  default     = false
}
