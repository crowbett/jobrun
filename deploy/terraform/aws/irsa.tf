# Least-privilege IAM role for the jobrun ServiceAccount (IRSA).
#
# jobrun itself calls no AWS API: it only needs DATABASE_URL. The one AWS permission in the
# path is reading the RDS-managed master secret, done with this role by External Secrets
# Operator (SecretStore auth.jwt.serviceAccountRef -> the jobrun ServiceAccount) or by the
# Secrets Store CSI driver AWS provider; both assume the pod ServiceAccount role. Scope: that
# one secret ARN, read-only. RDS encrypts it with the AWS-managed aws/secretsmanager key, so
# no kms:Decrypt grant is needed; add one scoped to the key if you switch to a CMK.
data "aws_iam_policy_document" "jobrun" {
  statement {
    sid       = "ReadRdsMasterSecret"
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [module.db.db_instance_master_user_secret_arn]
  }
}

resource "aws_iam_policy" "jobrun" {
  name_prefix = "${var.name}-app-"
  description = "jobrun pods: read the RDS master user secret, nothing else"
  policy      = data.aws_iam_policy_document.jobrun.json
}

module "jobrun_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts"
  version = "~> 6.8"

  name            = "${var.name}-app"
  use_name_prefix = true

  oidc_providers = {
    this = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["${var.jobrun_namespace}:${var.jobrun_service_account}"]
    }
  }

  policies = {
    jobrun = aws_iam_policy.jobrun.arn
  }
}
