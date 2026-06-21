resource "aws_security_group" "alb" {
  name        = "${var.name_prefix}-alb-sg"
  description = "Public ALB security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-alb-sg"
  }
}

resource "aws_security_group" "front_proxy" {
  name        = "${var.name_prefix}-front-proxy-sg"
  description = "Public front proxy EC2 security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-front-proxy-sg"
  }
}

resource "aws_security_group" "api" {
  name        = "${var.name_prefix}-api-sg"
  description = "Private FastAPI/RAG server security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-api-sg"
  }
}

resource "aws_security_group" "airflow" {
  name        = "${var.name_prefix}-airflow-sg"
  description = "Private Airflow/batch server security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-airflow-sg"
  }
}

resource "aws_security_group" "rds" {
  name        = "${var.name_prefix}-rds-sg"
  description = "Private PostgreSQL RDS security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-rds-sg"
  }
}

resource "aws_security_group" "redis" {
  name        = "${var.name_prefix}-redis-sg"
  description = "Private Redis security group"
  vpc_id      = var.vpc_id

  tags = {
    Name = "${var.name_prefix}-redis-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  for_each = toset(var.alb_ingress_cidrs)

  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.value
  from_port         = 80
  ip_protocol       = "tcp"
  to_port           = 80
  description       = "HTTP ingress to public ALB"
}

resource "aws_vpc_security_group_ingress_rule" "alb_https" {
  for_each = toset(var.alb_ingress_cidrs)

  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = each.value
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
  description       = "HTTPS ingress to public ALB"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_api" {
  security_group_id            = aws_security_group.alb.id
  referenced_security_group_id = aws_security_group.api.id
  from_port                    = 8000
  ip_protocol                  = "tcp"
  to_port                      = 8000
  description                  = "ALB to private API"
}

resource "aws_vpc_security_group_ingress_rule" "front_proxy_http" {
  for_each = toset(var.alb_ingress_cidrs)

  security_group_id = aws_security_group.front_proxy.id
  cidr_ipv4         = each.value
  from_port         = 80
  ip_protocol       = "tcp"
  to_port           = 80
  description       = "HTTP ingress to public front proxy"
}

resource "aws_vpc_security_group_ingress_rule" "front_proxy_https" {
  for_each = toset(var.alb_ingress_cidrs)

  security_group_id = aws_security_group.front_proxy.id
  cidr_ipv4         = each.value
  from_port         = 443
  ip_protocol       = "tcp"
  to_port           = 443
  description       = "HTTPS ingress to public front proxy"
}

resource "aws_vpc_security_group_ingress_rule" "front_proxy_airflow_ui" {
  for_each = toset(var.admin_airflow_cidrs)

  security_group_id = aws_security_group.front_proxy.id
  cidr_ipv4         = each.value
  from_port         = 8080
  ip_protocol       = "tcp"
  to_port           = 8080
  description       = "Airflow UI via front proxy, restricted to admin CIDRs"
}

resource "aws_vpc_security_group_egress_rule" "front_proxy_all" {
  security_group_id = aws_security_group.front_proxy.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Front proxy outbound for SSM, package updates, and API proxying"
}

resource "aws_vpc_security_group_ingress_rule" "api_from_alb" {
  security_group_id            = aws_security_group.api.id
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = 8000
  ip_protocol                  = "tcp"
  to_port                      = 8000
  description                  = "Private API from ALB"
}

resource "aws_vpc_security_group_ingress_rule" "api_from_front_proxy" {
  security_group_id            = aws_security_group.api.id
  referenced_security_group_id = aws_security_group.front_proxy.id
  from_port                    = 8000
  ip_protocol                  = "tcp"
  to_port                      = 8000
  description                  = "Private API from front proxy"
}

resource "aws_vpc_security_group_egress_rule" "api_all" {
  security_group_id = aws_security_group.api.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "API outbound through NAT and VPC endpoints"
}

resource "aws_vpc_security_group_ingress_rule" "airflow_admin_ui" {
  for_each = toset(var.admin_airflow_cidrs)

  security_group_id = aws_security_group.airflow.id
  cidr_ipv4         = each.value
  from_port         = 8080
  ip_protocol       = "tcp"
  to_port           = 8080
  description       = "Optional direct Airflow UI admin access"
}

resource "aws_vpc_security_group_ingress_rule" "airflow_from_front_proxy" {
  security_group_id            = aws_security_group.airflow.id
  referenced_security_group_id = aws_security_group.front_proxy.id
  from_port                    = 8080
  ip_protocol                  = "tcp"
  to_port                      = 8080
  description                  = "Airflow UI from front proxy"
}

resource "aws_vpc_security_group_egress_rule" "airflow_all" {
  security_group_id = aws_security_group.airflow.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
  description       = "Airflow outbound through NAT and VPC endpoints"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_airflow" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.airflow.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
  description                  = "PostgreSQL from Airflow"
}

resource "aws_vpc_security_group_ingress_rule" "rds_from_api" {
  security_group_id            = aws_security_group.rds.id
  referenced_security_group_id = aws_security_group.api.id
  from_port                    = 5432
  ip_protocol                  = "tcp"
  to_port                      = 5432
  description                  = "PostgreSQL from API"
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_airflow" {
  security_group_id            = aws_security_group.redis.id
  referenced_security_group_id = aws_security_group.airflow.id
  from_port                    = 6379
  ip_protocol                  = "tcp"
  to_port                      = 6379
  description                  = "Redis from Airflow"
}
