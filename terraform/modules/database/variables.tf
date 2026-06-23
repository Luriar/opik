variable "name_prefix" {
  description = "Name prefix for database resources."
  type        = string
}

variable "private_db_subnet_ids" {
  description = "Private database subnet IDs."
  type        = list(string)
}

variable "rds_security_group_id" {
  description = "RDS security group ID."
  type        = string
}

variable "redis_security_group_id" {
  description = "Redis security group ID."
  type        = string
}

variable "rds_instance_class" {
  description = "RDS instance class."
  type        = string
}

variable "rds_allocated_storage_gb" {
  description = "Initial RDS allocated storage in GiB."
  type        = number
}

variable "rds_max_allocated_storage_gb" {
  description = "Maximum autoscaled RDS storage in GiB."
  type        = number
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ for RDS instances."
  type        = bool
}

variable "rds_deletion_protection" {
  description = "Enable RDS deletion protection."
  type        = bool
}

variable "rds_skip_final_snapshot" {
  description = "Skip final snapshot on RDS destroy."
  type        = bool
}

variable "create_redis" {
  description = "Create ElastiCache Redis."
  type        = bool
}
