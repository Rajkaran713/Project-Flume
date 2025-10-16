# Root module - orchestrates all resources using custom modules

# Use custom S3 module
module "data_lake" {
  source = "./modules/s3-data-lake"
  
  bucket_name              = var.s3_bucket_name
  project_name            = var.project_name
  enable_versioning       = true
  enable_lifecycle_rules  = true
  silver_retention_days   = 180
  
  # Lambda triggers configuration
  lambda_triggers = [
    {
      function_arn = aws_lambda_function.transformation_pipeline.arn
      events       = ["s3:ObjectCreated:*"]
      prefix       = "swob_raw/"
      suffix       = ".json"
    },
    {
      function_arn = aws_lambda_function.transformation_pipeline.arn
      events       = ["s3:ObjectCreated:*"]
      prefix       = "hydrometric_raw/"
      suffix       = ".json"
    },
    {
      function_arn = aws_lambda_function.transformation_pipeline.arn
      events       = ["s3:ObjectCreated:*"]
      prefix       = "climate_hourly_raw/"
      suffix       = ".json"
    }
  ]
  
  # Pass Lambda permissions for dependency
  lambda_permissions = [aws_lambda_permission.allow_s3_invoke]
  
  tags = {
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}