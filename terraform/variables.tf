variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "environment" {
  description = "Deployment environment (dev or prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be dev or prod."
  }
}

variable "s3_bucket_name" {
  description = "S3 bucket for fulfillment data"
  type        = string
  default     = "last-mile-fulfillment-platform"
}

variable "lambda_function_name" {
  description = "Name of the data generator Lambda function"
  type        = string
  default     = "fulfillment-data-generator"
}

variable "lambda_memory_size" {
  description = "Lambda memory in MB"
  type        = number
  default     = 3008
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 900
}

variable "lambda_runtime" {
  description = "Lambda runtime"
  type        = string
  default     = "python3.12"
}

variable "daily_schedule_enabled" {
  description = "Enable the daily EventBridge rule"
  type        = bool
  default     = true
}

variable "weekly_schedule_enabled" {
  description = "Enable the weekly EventBridge rule"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Default tags applied to all resources"
  type        = map(string)
  default = {
    Project     = "last-mile-fulfillment"
    ManagedBy   = "terraform"
  }
}
