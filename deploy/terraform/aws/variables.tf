variable "region" {
  description = "AWS region. The README cost table is priced for us-east-1."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for every resource (VPC, EKS cluster, RDS instance, IAM role)."
  type        = string
  default     = "jobrun"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name))
    error_message = "name must be 2-31 chars of lowercase letters, digits and dashes, starting with a letter."
  }
}

variable "environment" {
  description = "Environment tag (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "VPC CIDR. Split into /20 private, /24 public and /24 database subnets across azs_count AZs."
  type        = string
  default     = "10.40.0.0/16"
}

variable "azs_count" {
  description = "Number of availability zones (EKS needs at least 2, RDS subnet groups too)."
  type        = number
  default     = 2

  validation {
    condition     = var.azs_count >= 2 && var.azs_count <= 3
    error_message = "azs_count must be 2 or 3."
  }
}

variable "single_nat_gateway" {
  description = "One NAT gateway for the whole VPC (cheap, one AZ is a single point of failure for egress) instead of one per AZ."
  type        = bool
  default     = true
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version. The Helm chart needs >= 1.30."
  type        = string
  default     = "1.34"
}

variable "cluster_endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public EKS API endpoint. Narrow this to your office/VPN egress IPs."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "node_instance_types" {
  description = "Managed node group instance types. The jobrun image is built for linux/amd64; switch to Graviton (t4g/m7g) only after publishing an arm64 image."
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_min_size" {
  description = "Managed node group minimum size."
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Managed node group maximum size."
  type        = number
  default     = 4
}

variable "node_desired_size" {
  description = "Managed node group desired size at creation (ignored afterwards so a cluster autoscaler can own it)."
  type        = number
  default     = 2
}

variable "db_instance_class" {
  description = "RDS instance class. db.t4g.small gives max_connections around 190 (LEAST(DBInstanceClassMemory/9531392, 5000)); budget server.pgMaxConns x HPA maxReplicas below that."
  type        = string
  default     = "db.t4g.small"
}

variable "db_allocated_storage" {
  description = "Initial gp3 storage in GiB."
  type        = number
  default     = 20
}

variable "db_max_allocated_storage" {
  description = "Storage autoscaling ceiling in GiB (0 disables autoscaling)."
  type        = number
  default     = 100
}

variable "db_multi_az" {
  description = "Multi-AZ standby for RDS. Doubles the instance cost; turn on for prod."
  type        = bool
  default     = false
}

variable "db_deletion_protection" {
  description = "RDS deletion protection. Keep true outside throwaway environments."
  type        = bool
  default     = true
}

variable "db_backup_retention_days" {
  description = "Automated backup retention in days."
  type        = number
  default     = 7
}

variable "jobrun_namespace" {
  description = "Kubernetes namespace the Helm release is installed into."
  type        = string
  default     = "jobrun"
}

variable "jobrun_service_account" {
  description = "Kubernetes ServiceAccount name the chart creates (the chart's fullname; 'jobrun' for a release named jobrun)."
  type        = string
  default     = "jobrun"
}

variable "tags" {
  description = "Extra tags for every resource."
  type        = map(string)
  default     = {}
}
