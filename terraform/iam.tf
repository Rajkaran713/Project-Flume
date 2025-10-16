# Lambda execution role
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-lambda-execution-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Name = "${var.project_name}-lambda-role"
  }
}

# Lambda policy for S3 access
resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "${var.project_name}-lambda-s3-policy"
  role = aws_iam_role.lambda_execution_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          module.data_lake.bucket_arn,
          "${module.data_lake.bucket_arn}/*"
        ]
      }
    ]
  })
}

# Attach AWS managed policy for CloudWatch Logs
resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# EC2 instance role
resource "aws_iam_role" "ec2_data_producer_role" {
  count = var.enable_ec2 ? 1 : 0
  name  = "${var.project_name}-ec2-producer-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

# EC2 policy for S3 write access
resource "aws_iam_role_policy" "ec2_s3_write_policy" {
  count = var.enable_ec2 ? 1 : 0
  name  = "${var.project_name}-ec2-s3-write-policy"
  role  = aws_iam_role.ec2_data_producer_role[0].id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          module.data_lake.bucket_arn,
          "${module.data_lake.bucket_arn}/*"
        ]
      }
    ]
  })
}

# EC2 instance profile
resource "aws_iam_instance_profile" "ec2_producer_profile" {
  count = var.enable_ec2 ? 1 : 0
  name  = "${var.project_name}-ec2-producer-profile"
  role  = aws_iam_role.ec2_data_producer_role[0].name
}