variable "name_prefix" {
  description = "Name prefix for compute resources."
  type        = string
}

variable "aws_region" {
  description = "AWS region."
  type        = string
}

variable "ami_id" {
  description = "AMI ID for EC2 instances."
  type        = string
}

variable "public_subnet_ids" {
  description = "Public subnet IDs for the front proxy."
  type        = list(string)
}

variable "private_app_subnet_ids" {
  description = "Private application subnet IDs."
  type        = list(string)

  validation {
    condition     = length(var.private_app_subnet_ids) >= 2
    error_message = "At least two private app subnets are required."
  }
}

variable "airflow_security_group_id" {
  description = "Airflow security group ID."
  type        = string
}

variable "front_proxy_security_group_id" {
  description = "Public front proxy security group ID."
  type        = string
}

variable "api_security_group_id" {
  description = "API security group ID."
  type        = string
}

variable "create_front_proxy" {
  description = "Create a lightweight public reverse proxy EC2 instance."
  type        = bool
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow/batch."
  type        = string
}

variable "api_instance_type" {
  description = "EC2 instance type for API/RAG."
  type        = string
}

variable "front_proxy_instance_type" {
  description = "EC2 instance type for the public reverse proxy."
  type        = string
}

variable "ec2_key_name" {
  description = "Optional EC2 key pair name. SSH ingress is not opened."
  type        = string
  default     = null
}

variable "data_bucket_name" {
  description = "S3 data lake bucket name."
  type        = string
}

variable "airflow_db_endpoint" {
  description = "Airflow RDS endpoint."
  type        = string
}

variable "airflow_db_port" {
  description = "Airflow RDS port."
  type        = number
}

variable "airflow_db_name" {
  description = "Airflow RDS database name."
  type        = string
}

variable "service_db_endpoint" {
  description = "Service RDS endpoint."
  type        = string
}

variable "service_db_port" {
  description = "Service RDS port."
  type        = number
}

variable "service_db_name" {
  description = "Service RDS database name."
  type        = string
}

variable "redis_primary_endpoint" {
  description = "Redis primary endpoint."
  type        = string
  default     = null
}

variable "create_redis" {
  description = "Whether Redis is created and should be exposed through SSM Parameter Store."
  type        = bool
}

variable "airflow_db_secret_arn" {
  description = "RDS managed secret ARN for Airflow DB."
  type        = string
}

variable "service_db_secret_arn" {
  description = "RDS managed secret ARN for service DB."
  type        = string
}

variable "repo_url" {
  description = "Optional repository URL to clone during bootstrap."
  type        = string
  default     = null
}

variable "storage_config" {
  description = "Non-secret S3 data-lake layout and storage backend rendered into instance env files."
  type = object({
    storage_backend         = string
    local_storage_base_path = string
    s3_base_prefix          = string
    bronze_prefix           = string
    silver_prefix           = string
    gold_prefix             = string
  })
}

variable "dart_config" {
  description = "Non-secret DART ingestion tuning rendered into the Airflow env file. DART_API_KEYS stays in the runtime secret."
  type = object({
    backfill_months           = number
    backfill_start_date       = string
    backfill_end_date         = string
    incremental_days          = number
    listed_company_schedule   = string
    collect_mode              = string
    daily_limit               = number
    daily_safe_limit          = number
    daily_emergency_limit     = number
    minute_limit              = number
    minute_safe_limit         = number
    quota_request_log_enabled = bool
  })
}

variable "embedding_config" {
  description = "Non-secret embedding model configuration rendered into the Airflow and API env files."
  type = object({
    provider  = string
    model     = string
    version   = string
    dimension = number
  })
}

variable "server_config" {
  description = "Non-secret FastAPI/RAG runtime tuning rendered into the API env file."
  type = object({
    opik_agent_enabled         = bool
    search_top_k               = number
    opik_db_path               = string
    bedrock_model              = string
    safety_model               = string
    intent_model               = string
    report_model               = string
    dart_model                 = string
    dart_summarize_model       = string
    analysis_model             = string
    composer_model             = string
    sentiment_model            = string
    dart_sentiment_batch_size  = number
    dart_sentiment_concurrent  = number
    dart_sentiment_max_retries = number
    dart_sentiment_retry_delay = number
  })
}
