output "alb_security_group_id" {
  description = "ALB security group ID."
  value       = aws_security_group.alb.id
}

output "front_proxy_security_group_id" {
  description = "Front proxy EC2 security group ID."
  value       = aws_security_group.front_proxy.id
}

output "api_security_group_id" {
  description = "API security group ID."
  value       = aws_security_group.api.id
}

output "airflow_security_group_id" {
  description = "Airflow security group ID."
  value       = aws_security_group.airflow.id
}

output "rds_security_group_id" {
  description = "RDS security group ID."
  value       = aws_security_group.rds.id
}

output "redis_security_group_id" {
  description = "Redis security group ID."
  value       = aws_security_group.redis.id
}
