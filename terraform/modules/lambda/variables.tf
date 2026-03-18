variable "function_name" {
  description = "Lambda function name"
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN for Lambda execution"
  type        = string
}

variable "runtime" {
  description = "Lambda runtime"
  type        = string
}

variable "timeout" {
  description = "Lambda timeout in seconds"
  type        = number
}

variable "memory_size" {
  description = "Lambda memory in MB"
  type        = number
}

variable "s3_bucket_name" {
  description = "S3 bucket name passed as environment variable"
  type        = string
}

variable "aws_region" {
  description = "AWS region passed as environment variable"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}
