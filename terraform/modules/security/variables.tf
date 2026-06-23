variable "name_prefix" {
  description = "Name prefix for security resources."
  type        = string
}

variable "vpc_id" {
  description = "VPC ID."
  type        = string
}

variable "alb_ingress_cidrs" {
  description = "CIDRs allowed to access the public ALB or front proxy."
  type        = list(string)
}

variable "admin_airflow_cidrs" {
  description = "Optional CIDRs allowed to reach Airflow UI directly on 8080."
  type        = list(string)
}
