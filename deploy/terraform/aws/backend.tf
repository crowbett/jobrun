# Remote state. Left commented on purpose: nothing in this directory has ever been applied,
# and `terraform init -backend=false` (what CI runs) does not need it. To use it, create the
# bucket out of band, uncomment, and run `terraform init -migrate-state`.
#
# terraform {
#   backend "s3" {
#     bucket       = "<account>-tfstate"
#     key          = "jobrun/aws/terraform.tfstate"
#     region       = "us-east-1"
#     encrypt      = true
#     use_lockfile = true # S3-native state locking (Terraform >= 1.10); no DynamoDB table
#   }
# }
