# Daily rule: runs at 2am UTC every day
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "${var.lambda_function_name}-daily"
  description         = "Trigger Lambda daily at 2am UTC for 1-day data generation"
  schedule_expression = "cron(0 2 * * ? *)"
  state               = var.daily_schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "daily" {
  rule = aws_cloudwatch_event_rule.daily.name
  arn  = var.lambda_function_arn

  input = jsonencode({
    mode = "daily"
  })
}

resource "aws_lambda_permission" "daily" {
  statement_id  = "AllowEventBridgeDaily"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}

# Weekly rule: runs at 3am UTC every Monday
resource "aws_cloudwatch_event_rule" "weekly" {
  name                = "${var.lambda_function_name}-weekly"
  description         = "Trigger Lambda weekly on Monday at 3am UTC for 7-day data generation"
  schedule_expression = "cron(0 3 ? * MON *)"
  state               = var.weekly_schedule_enabled ? "ENABLED" : "DISABLED"
}

resource "aws_cloudwatch_event_target" "weekly" {
  rule = aws_cloudwatch_event_rule.weekly.name
  arn  = var.lambda_function_arn

  input = jsonencode({
    mode = "weekly"
  })
}

resource "aws_lambda_permission" "weekly" {
  statement_id  = "AllowEventBridgeWeekly"
  action        = "lambda:InvokeFunction"
  function_name = var.lambda_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weekly.arn
}
