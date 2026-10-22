module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 21.26"

  name               = var.name
  kubernetes_version = var.kubernetes_version

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.cluster_endpoint_public_access_cidrs
  endpoint_private_access      = true

  # The identity running `terraform apply` gets cluster-admin through an access entry.
  enable_cluster_creator_admin_permissions = true

  # IRSA: the module creates the cluster IAM OIDC provider that irsa.tf trusts.
  enable_irsa = true

  # Control-plane logs go to CloudWatch (billed per GB ingested); api logs are the noisy ones.
  enabled_log_types = ["audit", "authenticator"]

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni = {
      before_compute = true
      # The chart ships NetworkPolicies; the VPC CNI only enforces them with this on.
      configuration_values = jsonencode({
        enableNetworkPolicy = "true"
      })
    }
  }

  eks_managed_node_groups = {
    default = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = var.node_instance_types

      min_size     = var.node_min_size
      max_size     = var.node_max_size
      desired_size = var.node_desired_size

      # IMDSv2 only, one hop: pods cannot borrow the node role through the metadata
      # service, so IRSA is the only way a pod gets AWS credentials.
      metadata_options = {
        http_endpoint               = "enabled"
        http_tokens                 = "required"
        http_put_response_hop_limit = 1
      }
    }
  }
}
