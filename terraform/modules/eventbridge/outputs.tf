output "daily_rule_arn" {
  description = "ARN of the daily EventBridge rule"
  value       = aws_cloudwatch_event_rule.daily.arn
}

output "weekly_rule_arn" {
  description = "ARN of the weekly EventBridge rule"
  value       = aws_cloudwatch_event_rule.weekly.arn
}
