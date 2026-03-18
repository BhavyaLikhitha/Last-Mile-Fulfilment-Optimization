variable "lambda_function_name" {
  description = "Name of the Lambda function to trigger"
  type        = string
}

variable "lambda_function_arn" {
  description = "ARN of the Lambda function to trigger"
  type        = string
}

variable "daily_schedule_enabled" {
  description = "Enable the daily schedule"
  type        = bool
}

variable "weekly_schedule_enabled" {
  description = "Enable the weekly schedule"
  type        = bool
}
