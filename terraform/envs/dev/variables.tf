variable "aws_region" {
  description = "AWS region for the OPIK dev environment."
  type        = string
  default     = "ap-northeast-2"
}

variable "project_name" {
  description = "Project name used for resource naming and tags."
  type        = string
  default     = "opik"
}

variable "environment" {
  description = "Environment name."
  type        = string
  default     = "dev"
}

variable "azs" {
  description = "Availability zones. If empty, the first two available AZs are used."
  type        = list(string)
  default     = []
}

variable "vpc_cidr" {
  description = "CIDR block for the OPIK VPC."
  type        = string
  default     = "10.20.0.0/16"
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets."
  type        = list(string)
  default     = ["10.20.0.0/24", "10.20.1.0/24"]
}

variable "private_app_subnet_cidrs" {
  description = "CIDR blocks for private application subnets."
  type        = list(string)
  default     = ["10.20.10.0/24", "10.20.11.0/24"]
}

variable "private_db_subnet_cidrs" {
  description = "CIDR blocks for private database subnets."
  type        = list(string)
  default     = ["10.20.20.0/24", "10.20.21.0/24"]
}

variable "single_nat_gateway" {
  description = "Use one NAT Gateway for dev cost control. Set false for AZ-local NAT gateways."
  type        = bool
  default     = true
}

variable "data_bucket_name" {
  description = "S3 data lake bucket name. If null, a deterministic account/region based name is used."
  type        = string
  default     = null
}

variable "manage_data_bucket" {
  description = "Create and manage the data bucket with Terraform. Set false to point at an existing externally-managed bucket (e.g. s3-opik-bucket) without Terraform owning it."
  type        = bool
  default     = true
}

variable "enable_s3_lifecycle" {
  description = "Enable noncurrent version expiration for the S3 data bucket."
  type        = bool
  default     = true
}

variable "create_redis" {
  description = "Create ElastiCache Redis for Airflow Celery broker."
  type        = bool
  default     = false
}

variable "create_front_proxy" {
  description = "Create a public EC2 reverse proxy when ALB permissions are unavailable."
  type        = bool
  default     = true
}

variable "rds_instance_class" {
  description = "RDS instance class for both Airflow and service PostgreSQL databases."
  type        = string
  default     = "db.t4g.micro"
}

variable "rds_allocated_storage_gb" {
  description = "Initial RDS allocated storage in GiB."
  type        = number
  default     = 20
}

variable "rds_max_allocated_storage_gb" {
  description = "Maximum RDS autoscaled storage in GiB."
  type        = number
  default     = 100
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ RDS. Recommended for production, disabled by default for dev cost."
  type        = bool
  default     = false
}

variable "rds_deletion_protection" {
  description = "Enable RDS deletion protection."
  type        = bool
  default     = false
}

variable "rds_skip_final_snapshot" {
  description = "Skip final snapshot when destroying dev RDS instances."
  type        = bool
  default     = true
}

variable "airflow_instance_type" {
  description = "EC2 instance type for Airflow, embedding, and batch jobs."
  type        = string
  default     = "r6g.large"
}

variable "api_instance_type" {
  description = "EC2 instance type for FastAPI, FAISS, RAG, and agents."
  type        = string
  default     = "r6g.large"
}

variable "front_proxy_instance_type" {
  description = "EC2 instance type for the lightweight public reverse proxy."
  type        = string
  default     = "t4g.nano"
}

variable "ec2_key_name" {
  description = "Optional EC2 key pair name. SSH ingress is not opened by this Terraform."
  type        = string
  default     = null
}

variable "repo_url" {
  description = "Optional Git repository URL for bootstrap clone. Leave null when deployment is handled separately."
  type        = string
  default     = null
}

variable "admin_airflow_cidrs" {
  description = "Optional CIDRs allowed to reach Airflow UI on 8080. Prefer SSM port forwarding and keep this empty."
  type        = list(string)
  default     = []
}

variable "alb_ingress_cidrs" {
  description = "CIDRs allowed to access the public ALB or front proxy."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "certificate_arn" {
  description = "Optional ACM certificate ARN. If set, ALB serves HTTPS and redirects HTTP to HTTPS."
  type        = string
  default     = null
}

variable "api_health_check_path" {
  description = "FastAPI health check path for the ALB target group."
  type        = string
  default     = "/health"
}

# ---------------------------------------------------------------------------
# Application runtime configuration (non-secret).
#
# These mirror the non-secret, non-Airflow keys from the repository .env so the
# data-lake layout, DART ingestion tuning, and embedding model can be chosen at
# deploy time. They are rendered into the EC2 user-data env files.
#
# SECRETS DO NOT GO HERE. AWS credentials are provided by the EC2 IAM role, and
# DART_API_KEYS / Bedrock keys / Airflow keys live in the Secrets Manager
# runtime container (see DEPLOYMENT.md). Airflow-internal settings (executor,
# UID, fernet/secret keys, DB URLs, timezone) are handled by the bootstrap.
# ---------------------------------------------------------------------------

variable "storage_config" {
  description = "S3 data-lake layout and storage backend. No AWS credentials here; the EC2 IAM role provides them. S3 bucket itself is set via data_bucket_name."
  type = object({
    storage_backend         = string # auto | s3 | local
    local_storage_base_path = string
    s3_base_prefix          = string # optional global prefix, "" for bucket root
    bronze_prefix           = string
    silver_prefix           = string
    gold_prefix             = string
  })
  default = {
    storage_backend         = "auto"
    local_storage_base_path = "/opt/airflow/data"
    s3_base_prefix          = ""
    bronze_prefix           = "bronze/dart"
    silver_prefix           = "silver/dart"
    gold_prefix             = "gold/dart"
  }
}

variable "dart_config" {
  description = "DART ingestion tuning (non-secret). DART_API_KEYS is a SECRET and is NOT set here; store it in the Airflow runtime secret container."
  type = object({
    backfill_months           = number
    backfill_start_date       = string # YYYY-MM-DD
    backfill_end_date         = string # YYYY-MM-DD
    incremental_days          = number
    listed_company_schedule   = string # cron, e.g. "1 9 * * 1-5"
    collect_mode              = string # structured | document | both
    daily_limit               = number
    daily_safe_limit          = number
    daily_emergency_limit     = number
    minute_limit              = number
    minute_safe_limit         = number
    quota_request_log_enabled = bool
  })
  default = {
    backfill_months           = 12
    backfill_start_date       = "2025-06-13"
    backfill_end_date         = "2026-06-13"
    incremental_days          = 3
    listed_company_schedule   = "1 9 * * 1-5"
    collect_mode              = "both"
    daily_limit               = 40000
    daily_safe_limit          = 36000
    daily_emergency_limit     = 39000
    minute_limit              = 1000
    minute_safe_limit         = 900
    quota_request_log_enabled = true
  }
}

variable "embedding_config" {
  description = "Gold-layer embedding model configuration (non-secret). Used by both the Airflow embedding job and the API/RAG query encoder; keep them aligned."
  type = object({
    provider  = string
    model     = string
    version   = string
    dimension = number
  })
  default = {
    provider  = "e5"
    model     = "intfloat/multilingual-e5-small"
    version   = "v1"
    dimension = 384
  }
}

variable "server_config" {
  description = "FastAPI/RAG runtime tuning rendered into the API instance env file. No secrets here."
  type = object({
    opik_agent_enabled          = bool
    search_top_k                = number
    opik_db_path                = string
    bedrock_model               = string
    safety_model                = string
    intent_model                = string
    report_model                = string
    dart_model                  = string
    dart_summarize_model        = string
    analysis_model              = string
    composer_model              = string
    sentiment_model             = string
    dart_sentiment_batch_size   = number
    dart_sentiment_concurrent   = number
    dart_sentiment_max_retries  = number
    dart_sentiment_retry_delay  = number
  })
  default = {
    opik_agent_enabled          = false
    search_top_k                = 10
    opik_db_path                = "/data/opik/opik.db"
    bedrock_model               = "apac.anthropic.claude-3-haiku-20240307-v1:0"
    safety_model                = "apac.anthropic.claude-3-haiku-20240307-v1:0"
    intent_model                = "apac.anthropic.claude-3-haiku-20240307-v1:0"
    report_model                = "apac.anthropic.claude-3-haiku-20240307-v1:0"
    dart_model                  = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
    dart_summarize_model        = "global.anthropic.claude-sonnet-4-6"
    analysis_model              = "global.anthropic.claude-opus-4-8"
    composer_model              = "global.anthropic.claude-sonnet-4-6"
    sentiment_model             = "apac.anthropic.claude-3-haiku-20240307-v1:0"
    dart_sentiment_batch_size   = 25
    dart_sentiment_concurrent   = 20
    dart_sentiment_max_retries  = 2
    dart_sentiment_retry_delay  = 1.5
  }
}
