# Remote state backend using S3 + DynamoDB for locking.
#
# Bootstrap (run once manually before `terraform init`):
#   aws s3api create-bucket --bucket last-mile-fulfillment-tf-state --region us-east-2 \
#       --create-bucket-configuration LocationConstraint=us-east-2
#   aws dynamodb create-table --table-name terraform-locks --region us-east-2 \
#       --attribute-definitions AttributeName=LockID,AttributeType=S \
#       --key-schema AttributeName=LockID,KeyType=HASH \
#       --billing-mode PAY_PER_REQUEST

# Uncomment the S3 backend after creating the state bucket (see bootstrap commands above).
# Until then, Terraform uses local state.
#
# terraform {
#   backend "s3" {
#     bucket         = "last-mile-fulfillment-tf-state"
#     key            = "terraform.tfstate"
#     region         = "us-east-2"
#     dynamodb_table = "terraform-locks"
#     encrypt        = true
#   }
# }
