# jobrun on AWS: EKS + RDS Postgres + VPC (Terraform)

> **Status: validated, never applied.** `terraform fmt -check`, `terraform init -backend=false`,
> `terraform validate` and `tflint` (terraform + aws rulesets) pass locally and in CI. No `plan`
> against a real account, no `apply`, no AWS resources exist. Treat this as a reviewed starting
> point, not a proven environment.

## What it builds

| File | Resources |
| --- | --- |
| `main.tf` | VPC (`terraform-aws-modules/vpc/aws ~> 6.7`): 2-3 AZs, private /20 (nodes, pods), public /24 (NAT, load balancers), database /24 (no internet route), one NAT gateway by default |
| `eks.tf` | EKS (`terraform-aws-modules/eks/aws ~> 21.26`): managed node group (AL2023, t3.medium x2), IRSA OIDC provider, coredns/kube-proxy/vpc-cni add-ons with **VPC CNI NetworkPolicy enforcement on** (the chart ships NetworkPolicies), IMDSv2 with hop limit 1 so pods cannot use the node role, creator gets cluster-admin via an access entry |
| `rds.tf` | RDS PostgreSQL 17 (`terraform-aws-modules/rds/aws ~> 7.2`): gp3, encrypted, private, `rds.force_ssl=1`, password generated and rotated by RDS in Secrets Manager (never in state), 7-day backups, deletion protection, SG allowing 5432 from the EKS node SG only |
| `irsa.tf` | Least-privilege IRSA role (`terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts ~> 6.8`) trusted only by `system:serviceaccount:jobrun:jobrun`, allowed only `secretsmanager:GetSecretValue`/`DescribeSecret` on the one RDS master secret |
| `ecr.tf` | Private ECR repo for the image: immutable tags, scan on push, keep last 30 |
| `backend.tf` | S3 remote state with native lockfile, **commented out** |

### Why the IRSA role is so small

jobrun calls no AWS API; it needs a `DATABASE_URL` and nothing else. The only AWS call in its
path is reading the RDS-managed credentials secret, which External Secrets Operator
(`SecretStore` with `auth.jwt.serviceAccountRef: {name: jobrun}`) or the Secrets Store CSI
driver's AWS provider make *as the jobrun ServiceAccount*. So the role grants read on exactly
that secret ARN. The node role cannot be borrowed (IMDSv2, hop limit 1). Production should
also create a dedicated non-superuser Postgres role for jobrun instead of using the master user.

## Use (when you decide to spend money)

```bash
cd deploy/terraform/aws
cp terraform.tfvars.example terraform.tfvars        # set your egress CIDR for the API endpoint
terraform init                                      # after uncommenting backend.tf, or local state
terraform plan -out tfplan                          # never run here: review it yourself
terraform apply tfplan
aws eks update-kubeconfig --name "$(terraform output -raw cluster_name)" --region "$(terraform output -raw region)"
terraform output -raw helm_values_hint > ../../helm/values-aws.yaml   # then add the ExternalSecret
helm install jobrun ../../helm/jobrun -n jobrun --create-namespace -f ../../helm/values-aws.yaml
```

`DATABASE_URL` must use `sslmode=require` (or `verify-full` with the RDS CA bundle) because of
`rds.force_ssl`. Connection budget: `db.t4g.small` allows ~190 connections; jobrund uses
`WORKER_CONCURRENCY + 8` per replica (24 by default), so `server.autoscaling.maxReplicas` x 24
must stay under that with headroom for migrations and admin sessions.

## Validate locally (what was actually run)

```bash
terraform fmt -check -recursive      # exit 0
terraform init -backend=false        # downloads the four modules + hashicorp/aws 6.67.0
terraform validate                   # Success! The configuration is valid.
tflint --init && tflint              # exit 0, no findings (terraform recommended + aws 0.40.0)
```

## Rough monthly cost (us-east-1, on-demand, default variables)

Not a quote. Unit prices are AWS list prices as published on the pages cited below (last
known values, **not re-checked on the day this was written**); 730 hours per month; no free
tier, Savings Plans or Reserved Instances. Use the [AWS Pricing Calculator](https://calculator.aws/)
before applying.

| Item | Unit price | Quantity (defaults) | ~USD / month |
| --- | --- | --- | ---: |
| EKS control plane, standard support | $0.10 / cluster-hour [1] | 1 cluster | 73.00 |
| EC2 t3.medium nodes | $0.0416 / hour [2] | 2 | 60.74 |
| EBS gp3 node volumes | $0.08 / GB-month [3] | 2 x 20 GB | 3.20 |
| NAT gateway (hourly) | $0.045 / hour [4] | 1 (`single_nat_gateway = true`) | 32.85 |
| NAT gateway data processed | $0.045 / GB [4] | assume 50 GB (image pulls, OTLP, updates) | 2.25 |
| Public IPv4 (NAT Elastic IP) | $0.005 / hour [4] | 1 | 3.65 |
| RDS PostgreSQL db.t4g.small, Single-AZ | $0.032 / hour [5] | 1 | 23.36 |
| RDS gp3 storage | $0.115 / GB-month [5] | 20 GB | 2.30 |
| Secrets Manager (RDS master secret) | $0.40 / secret-month [6] | 1 | 0.40 |
| KMS key (EKS secrets envelope encryption, module default) | $1.00 / key-month [7] | 1 | 1.00 |
| CloudWatch Logs (EKS audit + authenticator) | $0.50 / GB ingested [8] | assume 3 GB | 1.50 |
| ECR storage | $0.10 / GB-month [9] | < 1 GB | 0.10 |
| **Total, dev defaults** | | | **~ $204 / month (~ $0.28 / hour)** |

Production-shaped variant (`db_multi_az = true`, `single_nat_gateway = false` with 3 AZs,
3 nodes): RDS instance and storage double (+$25.7), two more NAT gateways and EIPs (+$73.0),
one more node and volume (+$32.0): **~ $335 / month** before traffic. Not included anywhere:
inter-AZ data transfer ($0.01/GB each way [10]), load balancers (the chart creates none),
backup storage beyond the free allocation equal to DB size, EKS extended-support pricing
($0.60/cluster-hour [1]) if `kubernetes_version` falls out of standard support.

Sources:
[1] https://aws.amazon.com/eks/pricing/ ·
[2] https://aws.amazon.com/ec2/pricing/on-demand/ ·
[3] https://aws.amazon.com/ebs/pricing/ ·
[4] https://aws.amazon.com/vpc/pricing/ (NAT gateway, public IPv4) ·
[5] https://aws.amazon.com/rds/postgresql/pricing/ ·
[6] https://aws.amazon.com/secrets-manager/pricing/ ·
[7] https://aws.amazon.com/kms/pricing/ ·
[8] https://aws.amazon.com/cloudwatch/pricing/ ·
[9] https://aws.amazon.com/ecr/pricing/ ·
[10] https://aws.amazon.com/ec2/pricing/on-demand/#Data_Transfer_within_the_same_AWS_Region
