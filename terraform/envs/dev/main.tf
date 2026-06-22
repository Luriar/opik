data "aws_caller_identity" "current" {}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ami" "amazon_linux_2023_arm64" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023*-kernel-6.1-arm64"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  name_prefix = "${var.project_name}-${var.environment}"
  azs         = length(var.azs) > 0 ? var.azs : slice(data.aws_availability_zones.available.names, 0, 2)

  data_bucket_name = coalesce(
    var.data_bucket_name,
    "${var.project_name}-${var.environment}-${data.aws_caller_identity.current.account_id}-${var.aws_region}-data"
  )

  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Owner       = "opik"
  }
}

module "network" {
  source = "../../modules/network"

  name_prefix              = local.name_prefix
  vpc_cidr                 = var.vpc_cidr
  azs                      = local.azs
  public_subnet_cidrs      = var.public_subnet_cidrs
  private_app_subnet_cidrs = var.private_app_subnet_cidrs
  private_db_subnet_cidrs  = var.private_db_subnet_cidrs
  single_nat_gateway       = var.single_nat_gateway
}

module "security" {
  source = "../../modules/security"

  name_prefix         = local.name_prefix
  vpc_id              = module.network.vpc_id
  alb_ingress_cidrs   = var.alb_ingress_cidrs
  admin_airflow_cidrs = var.admin_airflow_cidrs
}

module "storage" {
  source = "../../modules/storage"

  name_prefix          = local.name_prefix
  bucket_name          = local.data_bucket_name
  create_bucket        = var.manage_data_bucket
  enable_s3_lifecycle  = var.enable_s3_lifecycle
  force_destroy_bucket = false
}

module "database" {
  source = "../../modules/database"

  name_prefix                  = local.name_prefix
  private_db_subnet_ids        = module.network.private_db_subnet_ids
  rds_security_group_id        = module.security.rds_security_group_id
  redis_security_group_id      = module.security.redis_security_group_id
  rds_instance_class           = var.rds_instance_class
  rds_allocated_storage_gb     = var.rds_allocated_storage_gb
  rds_max_allocated_storage_gb = var.rds_max_allocated_storage_gb
  rds_multi_az                 = var.rds_multi_az
  rds_deletion_protection      = var.rds_deletion_protection
  rds_skip_final_snapshot      = var.rds_skip_final_snapshot
  create_redis                 = var.create_redis
}

module "compute" {
  source = "../../modules/compute"

  name_prefix                   = local.name_prefix
  aws_region                    = var.aws_region
  ami_id                        = data.aws_ami.amazon_linux_2023_arm64.id
  public_subnet_ids             = module.network.public_subnet_ids
  private_app_subnet_ids        = module.network.private_app_subnet_ids
  front_proxy_security_group_id = module.security.front_proxy_security_group_id
  airflow_security_group_id     = module.security.airflow_security_group_id
  api_security_group_id         = module.security.api_security_group_id
  create_front_proxy            = var.create_front_proxy
  airflow_instance_type         = var.airflow_instance_type
  api_instance_type             = var.api_instance_type
  front_proxy_instance_type     = var.front_proxy_instance_type
  ec2_key_name                  = var.ec2_key_name
  data_bucket_name              = module.storage.bucket_name
  airflow_db_endpoint           = module.database.airflow_db_endpoint
  airflow_db_port               = module.database.airflow_db_port
  airflow_db_name               = module.database.airflow_db_name
  service_db_endpoint           = module.database.service_db_endpoint
  service_db_port               = module.database.service_db_port
  service_db_name               = module.database.service_db_name
  redis_primary_endpoint        = module.database.redis_primary_endpoint
  create_redis                  = var.create_redis
  airflow_db_secret_arn         = module.database.airflow_db_master_user_secret_arn
  service_db_secret_arn         = module.database.service_db_master_user_secret_arn
  repo_url                      = var.repo_url
  storage_config                = var.storage_config
  dart_config                   = var.dart_config
  embedding_config              = var.embedding_config
  server_config                 = var.server_config
}
