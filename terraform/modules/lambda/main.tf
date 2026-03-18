data "archive_file" "placeholder" {
  type        = "zip"
  source_file = "${path.module}/../../placeholder.py"
  output_path = "${path.module}/../../placeholder.zip"
}

resource "aws_lambda_function" "data_generator" {
  function_name = var.function_name
  role          = var.role_arn
  handler       = "lambda_handler.lambda_handler"
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  filename         = data.archive_file.placeholder.output_path
  source_code_hash = data.archive_file.placeholder.output_base64sha256

  environment {
    variables = {
      S3_BUCKET_NAME   = var.s3_bucket_name
      AWS_DEFAULT_REGION = var.aws_region
    }
  }

  lifecycle {
    ignore_changes = [
      filename,
      source_code_hash,
    ]
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.environment == "prod" ? 30 : 14
}
