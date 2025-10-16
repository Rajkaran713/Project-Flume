# S3 Outputs (from custom module)
output "s3_bucket_name" {
  description = "Name of the S3 data lake bucket"
  value       = module.data_lake.bucket_id
}

output "s3_bucket_arn" {
  description = "ARN of the S3 data lake bucket"
  value       = module.data_lake.bucket_arn
}

# Lambda Outputs
output "lambda_function_name" {
  description = "Name of the Lambda transformation function"
  value       = aws_lambda_function.transformation_pipeline.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda transformation function"
  value       = aws_lambda_function.transformation_pipeline.arn
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_execution_role.arn
}

# EC2 Outputs
output "ec2_instance_id" {
  description = "ID of the EC2 data producer instance"
  value       = var.enable_ec2 ? aws_instance.data_producer[0].id : null
}

output "ec2_public_ip" {
  description = "Public IP of the EC2 instance"
  value       = var.enable_ec2 ? aws_instance.data_producer[0].public_ip : null
}

# CloudWatch Outputs
output "cloudwatch_log_group" {
  description = "CloudWatch log group for Lambda"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}
