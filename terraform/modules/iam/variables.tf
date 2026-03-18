variable "lambda_function_name" {
  description = "Lambda function name, used as prefix for IAM resources"
  type        = string
}

variable "s3_bucket_name" {
  description = "S3 bucket name for scoped IAM policy"
  type        = string
}
