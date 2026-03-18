output "bucket_name" {
  description = "Name of the fulfillment S3 bucket"
  value       = aws_s3_bucket.fulfillment.bucket
}

output "bucket_arn" {
  description = "ARN of the fulfillment S3 bucket"
  value       = aws_s3_bucket.fulfillment.arn
}
