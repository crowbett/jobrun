output "region" {
  description = "AWS region."
  value       = var.region
}

output "cluster_name" {
  description = "EKS cluster name (aws eks update-kubeconfig --name <this>)."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API endpoint."
  value       = module.eks.cluster_endpoint
}

output "vpc_id" {
  description = "VPC id."
  value       = module.vpc.vpc_id
}

output "db_endpoint" {
  description = "RDS endpoint host:port for DATABASE_URL."
  value       = module.db.db_instance_endpoint
}

output "db_master_user_secret_arn" {
  description = "Secrets Manager ARN of the RDS-managed master credentials (username/password JSON)."
  value       = module.db.db_instance_master_user_secret_arn
}

output "ecr_repository_url" {
  description = "Push the jobrun image here; set image.repository to it."
  value       = aws_ecr_repository.jobrun.repository_url
}

output "jobrun_irsa_role_arn" {
  description = "Set as serviceAccount.annotations.eks.amazonaws.com/role-arn in the Helm values."
  value       = module.jobrun_irsa.arn
}

output "helm_values_hint" {
  description = "Values to layer on top of the chart defaults for this environment."
  value       = <<-EOT
    image:
      repository: ${aws_ecr_repository.jobrun.repository_url}
    database:
      existingSecret: jobrun-db   # rendered by External Secrets from ${module.db.db_instance_master_user_secret_arn}
    serviceAccount:
      annotations:
        eks.amazonaws.com/role-arn: ${module.jobrun_irsa.arn}
    networkPolicy:
      egress:
        postgresTo:
          - ipBlock:
              cidr: ${var.vpc_cidr}
  EOT
}
