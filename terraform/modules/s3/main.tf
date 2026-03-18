resource "aws_s3_bucket" "fulfillment" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_versioning" "fulfillment" {
  bucket = aws_s3_bucket.fulfillment.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "fulfillment" {
  bucket = aws_s3_bucket.fulfillment.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "fulfillment" {
  bucket = aws_s3_bucket.fulfillment.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "fulfillment" {
  count  = var.environment == "dev" ? 1 : 0
  bucket = aws_s3_bucket.fulfillment.id

  rule {
    id     = "expire-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    expiration {
      days = 90
    }
  }
}
