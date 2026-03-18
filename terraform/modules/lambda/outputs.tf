output "function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.data_generator.arn
}

output "function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.data_generator.function_name
}

output "invoke_arn" {
  description = "Invoke ARN of the Lambda function"
  value       = aws_lambda_function.data_generator.invoke_arn
}
