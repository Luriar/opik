output "vpc_id" {
  description = "OPIK VPC ID."
  value       = module.network.vpc_id
}

output "public_subnet_ids" {
  description = "Public subnet IDs for ALB and NAT."
  value       = module.network.public_subnet_ids
}

output "private_app_subnet_ids" {
  description = "Private application subnet IDs."
  value       = module.network.private_app_subnet_ids
}

output "private_db_subnet_ids" {
  description = "Private database subnet IDs."
  value       = module.network.private_db_subnet_ids
}

output "data_bucket_name" {
  description = "S3 data lake bucket name."
  value       = module.storage.bucket_name
}

output "airflow_instance_id" {
  description = "Airflow/batch EC2 instance ID."
  value       = module.compute.airflow_instance_id
}

output "airflow_ui_local_url" {
  description = "Local URL after opening an SSM port-forwarding session."
  value       = "http://127.0.0.1:8080"
}

output "airflow_ui_public_url" {
  description = "Public Airflow UI URL via the front proxy on port 8080. Reachable only from admin_airflow_cidrs. Null when create_front_proxy=false."
  value       = module.compute.front_proxy_public_ip == null ? null : "http://${module.compute.front_proxy_public_ip}:8080"
}

output "airflow_ui_port_forward_command" {
  description = "Command to open a local tunnel to the private Airflow UI."
  value       = "aws ssm start-session --region ${var.aws_region} --target ${module.compute.airflow_instance_id} --document-name AWS-StartPortForwardingSession --parameters '{\"portNumber\":[\"8080\"],\"localPortNumber\":[\"8080\"]}'"
}

output "api_instance_id" {
  description = "FastAPI/RAG EC2 instance ID."
  value       = module.compute.api_instance_id
}

output "front_proxy_public_ip" {
  description = "Public IP for the lightweight front proxy. Null when create_front_proxy=false."
  value       = module.compute.front_proxy_public_ip
}

output "front_proxy_url" {
  description = "HTTP URL for the lightweight front proxy. Null when create_front_proxy=false."
  value       = module.compute.front_proxy_public_ip == null ? null : "http://${module.compute.front_proxy_public_ip}"
}

output "airflow_db_endpoint" {
  description = "Airflow RDS endpoint."
  value       = module.database.airflow_db_endpoint
}

output "service_db_endpoint" {
  description = "Service RDS endpoint."
  value       = module.database.service_db_endpoint
}

output "redis_primary_endpoint" {
  description = "ElastiCache Redis primary endpoint. Null when create_redis=false."
  value       = module.database.redis_primary_endpoint
}

output "airflow_secret_container_arn" {
  description = "Secrets Manager container for Airflow runtime secrets."
  value       = module.compute.airflow_runtime_secret_arn
}

output "api_secret_container_arn" {
  description = "Secrets Manager container for API runtime secrets."
  value       = module.compute.api_runtime_secret_arn
}
