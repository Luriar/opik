resource "aws_db_subnet_group" "this" {
  name       = "${var.name_prefix}-db-subnets"
  subnet_ids = var.private_db_subnet_ids

  tags = {
    Name = "${var.name_prefix}-db-subnets"
  }
}

resource "aws_db_instance" "airflow" {
  identifier = "${var.name_prefix}-airflow-db"

  engine         = "postgres"
  instance_class = var.rds_instance_class
  db_name        = "airflow"
  username       = "airflowadmin"

  allocated_storage     = var.rds_allocated_storage_gb
  max_allocated_storage = var.rds_max_allocated_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.rds_security_group_id]
  publicly_accessible    = false
  multi_az               = var.rds_multi_az

  manage_master_user_password = true

  backup_retention_period   = 7
  copy_tags_to_snapshot     = true
  deletion_protection       = var.rds_deletion_protection
  skip_final_snapshot       = var.rds_skip_final_snapshot
  final_snapshot_identifier = var.rds_skip_final_snapshot ? null : "${var.name_prefix}-airflow-db-final"

  auto_minor_version_upgrade = true

  tags = {
    Name = "${var.name_prefix}-airflow-db"
    Role = "airflow-metadata"
  }
}

resource "aws_db_instance" "service" {
  identifier = "${var.name_prefix}-service-db"

  engine         = "postgres"
  instance_class = var.rds_instance_class
  db_name        = "opik_service"
  username       = "serviceadmin"

  allocated_storage     = var.rds_allocated_storage_gb
  max_allocated_storage = var.rds_max_allocated_storage_gb
  storage_type          = "gp3"
  storage_encrypted     = true

  db_subnet_group_name   = aws_db_subnet_group.this.name
  vpc_security_group_ids = [var.rds_security_group_id]
  publicly_accessible    = false
  multi_az               = var.rds_multi_az

  manage_master_user_password = true

  backup_retention_period   = 7
  copy_tags_to_snapshot     = true
  deletion_protection       = var.rds_deletion_protection
  skip_final_snapshot       = var.rds_skip_final_snapshot
  final_snapshot_identifier = var.rds_skip_final_snapshot ? null : "${var.name_prefix}-service-db-final"

  auto_minor_version_upgrade = true

  tags = {
    Name = "${var.name_prefix}-service-db"
    Role = "service-serving"
  }
}

resource "aws_elasticache_subnet_group" "redis" {
  count = var.create_redis ? 1 : 0

  name       = "${var.name_prefix}-redis-subnets"
  subnet_ids = var.private_db_subnet_ids

  tags = {
    Name = "${var.name_prefix}-redis-subnets"
  }
}

resource "aws_elasticache_replication_group" "redis" {
  count = var.create_redis ? 1 : 0

  replication_group_id       = "${var.name_prefix}-redis"
  description                = "OPIK Airflow Celery Redis"
  engine                     = "redis"
  node_type                  = "cache.t4g.micro"
  num_cache_clusters         = 1
  port                       = 6379
  automatic_failover_enabled = false
  multi_az_enabled           = false

  subnet_group_name  = aws_elasticache_subnet_group.redis[0].name
  security_group_ids = [var.redis_security_group_id]

  at_rest_encryption_enabled = true
  transit_encryption_enabled = true

  tags = {
    Name = "${var.name_prefix}-redis"
    Role = "airflow-celery-broker"
  }
}
