data "aws_partition" "current" {}

locals {
  ssm_prefix = "/opik/${var.name_prefix}"
  repo_url   = var.repo_url == null ? "" : var.repo_url

  data_bucket_arn         = "arn:${data.aws_partition.current.partition}:s3:::${var.data_bucket_name}"
  data_bucket_objects_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.data_bucket_name}/*"
}

# Runtime secret containers are NOT managed by Terraform. They are referenced as
# data sources so that `terraform destroy` never deletes them and a redeploy
# (same account) reuses them without name conflicts. The empty containers must
# exist before apply; see DEPLOYMENT.md for the one-time create-secret commands.
data "aws_secretsmanager_secret" "airflow_runtime" {
  name = "${var.name_prefix}/airflow/runtime"
}

data "aws_secretsmanager_secret" "api_runtime" {
  name = "${var.name_prefix}/api/runtime"
}

resource "aws_ssm_parameter" "data_bucket" {
  name  = "${local.ssm_prefix}/s3_bucket"
  type  = "String"
  value = var.data_bucket_name
}

resource "aws_ssm_parameter" "airflow_db_endpoint" {
  name  = "${local.ssm_prefix}/airflow_db_endpoint"
  type  = "String"
  value = var.airflow_db_endpoint
}

resource "aws_ssm_parameter" "service_db_endpoint" {
  name  = "${local.ssm_prefix}/service_db_endpoint"
  type  = "String"
  value = var.service_db_endpoint
}

resource "aws_ssm_parameter" "redis_endpoint" {
  count = var.create_redis ? 1 : 0

  name  = "${local.ssm_prefix}/redis_endpoint"
  type  = "String"
  value = var.redis_primary_endpoint
}

data "aws_iam_policy_document" "ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "${var.name_prefix}-airflow-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Name = "${var.name_prefix}-airflow-role"
  }
}

resource "aws_iam_role" "api" {
  name               = "${var.name_prefix}-api-role"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json

  tags = {
    Name = "${var.name_prefix}-api-role"
  }
}

resource "aws_iam_role_policy_attachment" "airflow_ssm" {
  role       = aws_iam_role.airflow.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_role_policy_attachment" "api_ssm" {
  role       = aws_iam_role.api.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "airflow" {
  statement {
    sid = "S3DataLakeReadWrite"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:DeleteObject",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      local.data_bucket_arn,
      local.data_bucket_objects_arn
    ]
  }

  statement {
    sid = "BedrockInvoke"
    actions = [
      "bedrock:InvokeModel",
      "bedrock:InvokeModelWithResponseStream"
    ]
    resources = ["*"]
  }

  statement {
    sid = "ReadRuntimeConfig"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:ssm:${var.aws_region}:*:parameter${local.ssm_prefix}/*"]
  }

  statement {
    sid = "ReadSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      var.airflow_db_secret_arn,
      var.service_db_secret_arn,
      data.aws_secretsmanager_secret.airflow_runtime.arn
    ]
  }

  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

data "aws_iam_policy_document" "api" {
  statement {
    sid = "S3DataLakeReadMostly"
    actions = [
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:PutObject"
    ]
    resources = [
      local.data_bucket_arn,
      local.data_bucket_objects_arn
    ]
  }

  statement {
    sid = "BedrockInvoke"
    actions = [
      "bedrock:InvokeModel",
      "bedrock:InvokeModelWithResponseStream"
    ]
    resources = ["*"]
  }

  statement {
    sid = "ReadRuntimeConfig"
    actions = [
      "ssm:GetParameter",
      "ssm:GetParameters",
      "ssm:GetParametersByPath"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:ssm:${var.aws_region}:*:parameter${local.ssm_prefix}/*"]
  }

  statement {
    sid = "ReadSecrets"
    actions = [
      "secretsmanager:DescribeSecret",
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      var.service_db_secret_arn,
      data.aws_secretsmanager_secret.api_runtime.arn
    ]
  }

  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "airflow" {
  name   = "${var.name_prefix}-airflow-policy"
  policy = data.aws_iam_policy_document.airflow.json
}

resource "aws_iam_policy" "api" {
  name   = "${var.name_prefix}-api-policy"
  policy = data.aws_iam_policy_document.api.json
}

resource "aws_iam_role_policy_attachment" "airflow_custom" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.airflow.arn
}

resource "aws_iam_role_policy_attachment" "api_custom" {
  role       = aws_iam_role.api.name
  policy_arn = aws_iam_policy.api.arn
}

resource "aws_iam_instance_profile" "airflow" {
  name = "${var.name_prefix}-airflow-profile"
  role = aws_iam_role.airflow.name
}

resource "aws_iam_instance_profile" "api" {
  name = "${var.name_prefix}-api-profile"
  role = aws_iam_role.api.name
}

resource "aws_instance" "airflow" {
  ami                         = var.ami_id
  instance_type               = var.airflow_instance_type
  subnet_id                   = var.private_app_subnet_ids[0]
  vpc_security_group_ids      = [var.airflow_security_group_id]
  associate_public_ip_address = false
  iam_instance_profile        = aws_iam_instance_profile.airflow.name
  key_name                    = var.ec2_key_name
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/user_data/airflow.sh.tftpl", {
    aws_region             = var.aws_region
    data_bucket_name       = var.data_bucket_name
    airflow_db_endpoint    = var.airflow_db_endpoint
    airflow_db_port        = var.airflow_db_port
    airflow_db_name        = var.airflow_db_name
    service_db_endpoint    = var.service_db_endpoint
    service_db_port        = var.service_db_port
    service_db_name        = var.service_db_name
    redis_primary_endpoint = var.redis_primary_endpoint == null ? "" : var.redis_primary_endpoint
    airflow_secret_arn     = data.aws_secretsmanager_secret.airflow_runtime.arn
    airflow_db_secret_arn  = var.airflow_db_secret_arn
    service_db_secret_arn  = var.service_db_secret_arn
    repo_url               = local.repo_url
    storage_config         = var.storage_config
    dart_config            = var.dart_config
    embedding_config       = var.embedding_config
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size           = 80
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name = "${var.name_prefix}-airflow-batch"
    Role = "airflow-batch"
  }
}

resource "aws_instance" "api" {
  ami                         = var.ami_id
  instance_type               = var.api_instance_type
  subnet_id                   = var.private_app_subnet_ids[1]
  vpc_security_group_ids      = [var.api_security_group_id]
  associate_public_ip_address = false
  iam_instance_profile        = aws_iam_instance_profile.api.name
  key_name                    = var.ec2_key_name
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/user_data/api.sh.tftpl", {
    aws_region          = var.aws_region
    data_bucket_name    = var.data_bucket_name
    service_db_endpoint = var.service_db_endpoint
    api_secret_arn      = data.aws_secretsmanager_secret.api_runtime.arn
    repo_url            = local.repo_url
    storage_config      = var.storage_config
    embedding_config    = var.embedding_config
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size           = 80
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name = "${var.name_prefix}-api-rag"
    Role = "api-rag"
  }
}

resource "aws_instance" "front_proxy" {
  count = var.create_front_proxy ? 1 : 0

  ami                         = var.ami_id
  instance_type               = var.front_proxy_instance_type
  subnet_id                   = var.public_subnet_ids[0]
  vpc_security_group_ids      = [var.front_proxy_security_group_id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.api.name
  key_name                    = var.ec2_key_name
  user_data_replace_on_change = true

  user_data = templatefile("${path.module}/user_data/front_proxy.sh.tftpl", {
    api_private_ip     = aws_instance.api.private_ip
    airflow_private_ip = aws_instance.airflow.private_ip
  })

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  root_block_device {
    volume_size           = 20
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = {
    Name = "${var.name_prefix}-front-proxy"
    Role = "front-proxy"
  }
}
