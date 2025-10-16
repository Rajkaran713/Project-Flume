# S3 Data Lake Module
# Reusable module for creating data lake buckets with best practices

resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
  
  tags = merge(
    var.tags,
    {
      Name        = "${var.project_name}-data-lake"
      Description = "Multi-layer data lake (Bronze/Silver/Gold)"
    }
  )
}

# Enable versioning
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

# Server-side encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  bucket = aws_s3_bucket.data_lake.id
  
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = var.encryption_algorithm
    }
  }
}

# Lifecycle rules for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
  count  = var.enable_lifecycle_rules ? 1 : 0
  bucket = aws_s3_bucket.data_lake.id
  
  # Archive Bronze layer after 30 days
  rule {
    id     = "archive-bronze-layer"
    status = "Enabled"
    
    filter {
      prefix = var.bronze_prefix
    }
    
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }
    
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
  
  # Delete old Silver files after retention period
  rule {
    id     = "expire-old-silver"
    status = "Enabled"
    
    filter {
      prefix = var.silver_prefix
    }
    
    expiration {
      days = var.silver_retention_days
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "public_access_block" {
  bucket = aws_s3_bucket.data_lake.id
  
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket notification for Lambda triggers
resource "aws_s3_bucket_notification" "lambda_triggers" {
  bucket = aws_s3_bucket.data_lake.id
  
  dynamic "lambda_function" {
    for_each = var.lambda_triggers
    
    content {
      lambda_function_arn = lambda_function.value.function_arn
      events              = lambda_function.value.events
      filter_prefix       = lambda_function.value.prefix
      filter_suffix       = lambda_function.value.suffix
    }
  }
  
  depends_on = [var.lambda_permissions]
}