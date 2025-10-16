# Lambda Layer for AWS SDK Pandas
data "aws_lambda_layer_version" "pandas" {
  layer_name = "AWSSDKPandas-Python312"
}

# Lambda function for data transformation
resource "aws_lambda_function" "transformation_pipeline" {
  # Note: You need to create lambda-code.zip with your lambda_transform.py
  filename         = "${path.module}/../flume-etl/lambda-code.zip"
  function_name    = "${var.project_name}-transformation-pipeline"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "lambda_transform.lambda_handler"
  source_code_hash = fileexists("${path.module}/../flume-etl/lambda-code.zip") ? filebase64sha256("${path.module}/../flume-etl/lambda-code.zip") : null
  runtime         = var.lambda_runtime
  timeout         = var.lambda_timeout
  memory_size     = var.lambda_memory_size
  
  layers = [data.aws_lambda_layer_version.pandas.arn]
  
  environment {
    variables = {
      S3_BUCKET = var.s3_bucket_name
      LOG_LEVEL = "INFO"
    }
  }
  
  tags = {
    Name = "${var.project_name}-transformation"
  }
}

# Lambda permission for S3 to invoke
resource "aws_lambda_permission" "allow_s3_invoke" {
  statement_id  = "AllowExecutionFromS3"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformation_pipeline.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = module.data_lake.bucket_arn
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.transformation_pipeline.function_name}"
  retention_in_days = 7
  
  tags = {
    Name = "${var.project_name}-lambda-logs"
  }
}