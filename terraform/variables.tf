variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "project-flume"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data lake"
  type        = string
  default     = "project-nimbus-raw-data-lake-12345-raj"
}

variable "lambda_memory_size" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 1024
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_runtime" {
  description = "Lambda runtime version"
  type        = string
  default     = "python3.12"
}

variable "ec2_instance_type" {
  description = "EC2 instance type for data ingestion"
  type        = string
  default     = "t2.micro"
}

variable "ec2_key_name" {
  description = "EC2 SSH key pair name"
  type        = string
  default     = "flume-producer-key"
}

variable "enable_ec2" {
  description = "Whether to provision EC2 instance"
  type        = bool
  default     = false
}