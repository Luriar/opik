output "airflow_instance_id" {
  description = "Airflow/batch EC2 instance ID."
  value       = aws_instance.airflow.id
}

output "api_instance_id" {
  description = "API/RAG EC2 instance ID."
  value       = aws_instance.api.id
}

output "airflow_instance_private_ip" {
  description = "Airflow/batch private IP."
  value       = aws_instance.airflow.private_ip
}

output "api_instance_private_ip" {
  description = "API/RAG private IP."
  value       = aws_instance.api.private_ip
}

output "front_proxy_instance_id" {
  description = "Front proxy EC2 instance ID. Null when disabled."
  value       = try(aws_instance.front_proxy[0].id, null)
}

output "front_proxy_public_ip" {
  description = "Front proxy public IP. Null when disabled."
  value       = try(aws_instance.front_proxy[0].public_ip, null)
}

output "airflow_runtime_secret_arn" {
  description = "Secrets Manager container ARN for Airflow runtime secrets."
  value       = data.aws_secretsmanager_secret.airflow_runtime.arn
}

output "api_runtime_secret_arn" {
  description = "Secrets Manager container ARN for API runtime secrets."
  value       = data.aws_secretsmanager_secret.api_runtime.arn
}
