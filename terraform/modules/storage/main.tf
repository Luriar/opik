# When create_bucket = true, Terraform creates and manages the data-lake bucket.
# When create_bucket = false, Terraform only references an existing externally
# managed bucket (e.g. a pre-existing data bucket with real data) and applies no
# configuration to it. This keeps Terraform from owning/destroying shared data.

resource "aws_s3_bucket" "data" {
  count = var.create_bucket ? 1 : 0

  bucket        = var.bucket_name
  force_destroy = var.force_destroy_bucket

  # Data must survive `terraform destroy` even when Terraform manages the bucket.
  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name = "${var.name_prefix}-data"
    Role = "data-lake"
  }
}

data "aws_s3_bucket" "existing" {
  count = var.create_bucket ? 0 : 1

  bucket = var.bucket_name
}

locals {
  bucket_id  = var.create_bucket ? aws_s3_bucket.data[0].id : data.aws_s3_bucket.existing[0].id
  bucket_arn = var.create_bucket ? aws_s3_bucket.data[0].arn : data.aws_s3_bucket.existing[0].arn
}

resource "aws_s3_bucket_public_access_block" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_versioning" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data" {
  count = var.create_bucket ? 1 : 0

  bucket = aws_s3_bucket.data[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data" {
  count = var.create_bucket && var.enable_s3_lifecycle ? 1 : 0

  bucket = aws_s3_bucket.data[0].id

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-old-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}
