variable "name_prefix" {
  description = "Name prefix for load balancer resources."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID."
  type        = string
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for the ALB."
  type        = list(string)
}

variable "alb_security_group_id" {
  description = "ALB security group ID."
  type        = string
}

variable "api_instance_id" {
  description = "API EC2 instance ID."
  type        = string
}

variable "api_port" {
  description = "API instance port."
  type        = number
  default     = 8000
}

variable "health_check_path" {
  description = "Health check path."
  type        = string
  default     = "/health"
}

variable "certificate_arn" {
  description = "Optional ACM certificate ARN for HTTPS."
  type        = string
  default     = null
}
