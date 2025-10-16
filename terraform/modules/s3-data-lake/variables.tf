variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
}

variable "enable_versioning" {
  description = "Enable S3 bucket versioning"
  type        = bool
  default     = true
}

variable "encryption_algorithm" {
  description = "Server-side encryption algorithm"
  type        = string
  default     = "AES256"
}

variable "enable_lifecycle_rules" {
  description = "Enable lifecycle rules for cost optimization"
  type        = bool
  default     = true
}

variable "bronze_prefix" {
  description = "Prefix for Bronze layer data"
  type        = string
  default     = "swob_raw/"
}

variable "silver_prefix" {
  description = "Prefix for Silver layer data"
  type        = string
  default     = "swob_silver/"
}

variable "silver_retention_days" {
  description = "Number of days to retain Silver layer data"
  type        = number
  default     = 180
}

variable "lambda_triggers" {
  description = "Lambda function triggers configuration"
  type = list(object({
    function_arn = string
    events       = list(string)
    prefix       = string
    suffix       = string
  }))
  default = []
}

variable "lambda_permissions" {
  description = "List of Lambda permission resources (for dependency)"
  type        = any
  default     = []
}

variable "tags" {
  description = "Additional tags for the bucket"
  type        = map(string)
  default     = {}
}