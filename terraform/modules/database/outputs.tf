output "airflow_db_endpoint" {
  description = "Airflow RDS endpoint."
  value       = aws_db_instance.airflow.address
}

output "airflow_db_port" {
  description = "Airflow RDS port."
  value       = aws_db_instance.airflow.port
}

output "airflow_db_name" {
  description = "Airflow database name."
  value       = aws_db_instance.airflow.db_name
}

output "airflow_db_master_user_secret_arn" {
  description = "RDS managed master user secret ARN for the Airflow database."
  value       = aws_db_instance.airflow.master_user_secret[0].secret_arn
}

output "service_db_endpoint" {
  description = "Service RDS endpoint."
  value       = aws_db_instance.service.address
}

output "service_db_port" {
  description = "Service RDS port."
  value       = aws_db_instance.service.port
}

output "service_db_name" {
  description = "Service database name."
  value       = aws_db_instance.service.db_name
}

output "service_db_master_user_secret_arn" {
  description = "RDS managed master user secret ARN for the service database."
  value       = aws_db_instance.service.master_user_secret[0].secret_arn
}

output "redis_primary_endpoint" {
  description = "Redis primary endpoint address. Null when Redis is disabled."
  value       = try(aws_elasticache_replication_group.redis[0].primary_endpoint_address, null)
}

output "redis_port" {
  description = "Redis port. Null when Redis is disabled."
  value       = try(aws_elasticache_replication_group.redis[0].port, null)
}
