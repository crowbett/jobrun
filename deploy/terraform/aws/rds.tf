# Postgres 17 to match compose.yaml and CI. Reachable only from the EKS node security group
# (VPC CNI pods use the node ENI security groups), only on database subnets with no
# internet route, TLS forced, master password generated and rotated by RDS in Secrets Manager.
resource "aws_security_group" "db" {
  name_prefix = "${var.name}-db-"
  description = "Postgres for jobrun: 5432 from EKS nodes only"
  vpc_id      = module.vpc.vpc_id

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_vpc_security_group_ingress_rule" "db_from_nodes" {
  security_group_id            = aws_security_group.db.id
  description                  = "Postgres from EKS nodes and pods"
  ip_protocol                  = "tcp"
  from_port                    = 5432
  to_port                      = 5432
  referenced_security_group_id = module.eks.node_security_group_id
}

module "db" {
  source  = "terraform-aws-modules/rds/aws"
  version = "~> 7.2"

  identifier = var.name

  engine               = "postgres"
  engine_version       = "17"
  family               = "postgres17"
  major_engine_version = "17"
  instance_class       = var.db_instance_class

  allocated_storage     = var.db_allocated_storage
  max_allocated_storage = var.db_max_allocated_storage
  storage_type          = "gp3"
  storage_encrypted     = true

  db_name  = "jobrun"
  username = "jobrun"
  port     = 5432

  # RDS generates the password and keeps it in a Secrets Manager secret it rotates;
  # it never appears in Terraform state or variables.
  manage_master_user_password = true

  multi_az               = var.db_multi_az
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [aws_security_group.db.id]
  publicly_accessible    = false

  backup_retention_period = var.db_backup_retention_days
  deletion_protection     = var.db_deletion_protection
  skip_final_snapshot     = !var.db_deletion_protection
  copy_tags_to_snapshot   = true

  performance_insights_enabled = true
  monitoring_interval          = 0

  create_db_parameter_group = true
  parameters = [
    # DATABASE_URL must then use sslmode=require (or verify-full with the RDS CA bundle).
    { name = "rds.force_ssl", value = "1" },
    # Surface lock waits longer than deadlock_timeout (claims use SKIP LOCKED, so these are anomalies).
    { name = "log_lock_waits", value = "1" },
  ]
}
