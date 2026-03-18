output "lambda_function_arn" {
  description = "ARN of the data generator Lambda"
  value       = module.lambda.function_arn
}

output "s3_bucket_name" {
  description = "Name of the fulfillment S3 bucket"
  value       = module.s3.bucket_name
}

output "iam_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = module.iam.role_arn
}

output "daily_rule_arn" {
  description = "ARN of the daily EventBridge rule"
  value       = module.eventbridge.daily_rule_arn
}

output "weekly_rule_arn" {
  description = "ARN of the weekly EventBridge rule"
  value       = module.eventbridge.weekly_rule_arn
}
