output "bucket_name" {
  description = "S3 data lake bucket name."
  value       = local.bucket_id
}

output "bucket_arn" {
  description = "S3 data lake bucket ARN."
  value       = local.bucket_arn
}
