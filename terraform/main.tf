module "iam" {
  source = "./modules/iam"

  lambda_function_name = var.lambda_function_name
  s3_bucket_name       = var.s3_bucket_name
}

module "s3" {
  source = "./modules/s3"

  bucket_name = var.s3_bucket_name
  environment = var.environment
}

module "lambda" {
  source = "./modules/lambda"

  function_name  = var.lambda_function_name
  role_arn       = module.iam.role_arn
  runtime        = var.lambda_runtime
  timeout        = var.lambda_timeout
  memory_size    = var.lambda_memory_size
  s3_bucket_name = module.s3.bucket_name
  aws_region     = var.aws_region
  environment    = var.environment
}

module "eventbridge" {
  source = "./modules/eventbridge"

  lambda_function_name   = module.lambda.function_name
  lambda_function_arn    = module.lambda.function_arn
  daily_schedule_enabled = var.daily_schedule_enabled
  weekly_schedule_enabled = var.weekly_schedule_enabled
}
