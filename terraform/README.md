# Project Flume - Terraform Infrastructure as Code

This directory contains Infrastructure as Code (IaC) for provisioning AWS resources using Terraform with **modular architecture**.

## 📋 Architecture

### Module Structure

- **Custom Module**: `s3-data-lake` - Reusable S3 bucket with data lake best practices
- **Root Module**: Orchestrates all resources (Lambda, IAM, EC2)

### Resources Managed

- ✅ **S3 Data Lake** (via custom module)
  - Versioning enabled
  - Server-side encryption
  - Lifecycle policies for cost optimization
  - Public access blocked
  - Event notifications for Lambda
- ✅ **AWS Lambda** - Serverless transformation pipeline
- ✅ **IAM Roles & Policies** - Least-privilege access
- ✅ **CloudWatch Logs** - Lambda execution monitoring
- ✅ **EC2 Instance** (optional) - Data ingestion server

## 🏗️ Directory Structure
```
terraform/
├── modules/
│   └── s3-data-lake/          # Custom reusable S3 module
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
├── main.tf                    # Root module using custom S3 module
├── provider.tf                # AWS provider configuration
├── variables.tf               # Input variables
├── outputs.tf                 # Output values
├── lambda.tf                  # Lambda function
├── iam.tf                     # IAM roles
├── ec2.tf                     # EC2 instance (optional)
└── README.md                  # This file
```

## 🚀 Quick Start

### Prerequisites

- [Terraform >= 1.0](https://www.terraform.io/downloads)
- AWS CLI configured with credentials
- (Optional) Lambda deployment package: `lambda-code.zip`

### Deployment
```bash
# Navigate to terraform directory
cd terraform/

# Initialize Terraform (downloads providers, initializes modules)
terraform init

# Validate configuration
terraform validate

# Preview changes
terraform plan

# Apply infrastructure
terraform apply

# With custom variables
terraform apply -var="environment=prod" -var="enable_ec2=true"
```

### Destroy Resources
```bash
terraform destroy
```

## 📝 Configuration

### Key Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `aws_region` | AWS region | `us-east-1` |
| `environment` | Environment name | `dev` |
| `project_name` | Project identifier | `project-flume` |
| `s3_bucket_name` | Data lake bucket | `project-nimbus-raw-data-lake-12345-raj` |
| `lambda_memory_size` | Lambda memory (MB) | `1024` |
| `lambda_timeout` | Lambda timeout (sec) | `300` |
| `enable_ec2` | Provision EC2 | `false` |

### Custom Configuration

Create `terraform.tfvars`:
```hcl
aws_region      = "us-east-1"
environment     = "production"
s3_bucket_name  = "your-custom-bucket"
enable_ec2      = true
ec2_key_name    = "your-key-pair"
```

## 🔧 Module Usage

### S3 Data Lake Module

The custom `s3-data-lake` module is reusable and configurable:
```hcl
module "data_lake" {
  source = "./modules/s3-data-lake"
  
  bucket_name              = "my-data-lake"
  project_name            = "my-project"
  enable_versioning       = true
  enable_lifecycle_rules  = true
  silver_retention_days   = 90
  
  lambda_triggers = [
    {
      function_arn = aws_lambda_function.processor.arn
      events       = ["s3:ObjectCreated:*"]
      prefix       = "raw/"
      suffix       = ".json"
    }
  ]
}
```

**Module Features:**
- Parameterized bucket configuration
- Optional versioning and lifecycle rules
- Configurable retention periods
- Dynamic Lambda trigger support
- Built-in security best practices

## 📊 Cost Estimate

| Resource | Configuration | Monthly Cost |
|----------|---------------|-------------|
| S3 (50GB) | Standard + IA + Glacier | ~$1.15 |
| Lambda | 1GB RAM, 5K invocations | ~$2.00 |
| EC2 t2.micro | (optional) | ~$8.50 |
| CloudWatch Logs | 7-day retention | ~$0.50 |
| **Total** | | **~$4.15 - $12.15** |

## 🔒 Security Features

- ✅ S3 bucket encryption (AES256)
- ✅ Public access completely blocked
- ✅ Least-privilege IAM policies
- ✅ VPC security groups for EC2
- ✅ CloudWatch logging enabled
- ✅ Versioning for data recovery

## 📈 Best Practices Implemented

### Infrastructure
- **Modular design** for reusability
- **Variables** for configuration flexibility
- **Outputs** for resource reference
- **Dynamic blocks** for scalability
- **Conditional resources** (e.g., EC2)

### Cost Optimization
- Lifecycle policies (STANDARD → IA → GLACIER)
- Configurable retention periods
- Right-sized Lambda memory

### Security
- Encryption at rest
- Public access blocked
- Least-privilege policies
- Tagged resources for governance

## 🎯 Interview Talking Points

**"Did you use Terraform modules?"**
> "Yes, I created a custom S3 module (`s3-data-lake`) that encapsulates data lake best practices including versioning, lifecycle policies, encryption, and Lambda triggers. This makes the bucket configuration reusable across different projects. The root module then orchestrates this with Lambda, IAM, and EC2 resources."

**"How did you structure your Terraform code?"**
> "I used a modular approach with a custom S3 module for reusability, separate files for logical grouping (lambda.tf, iam.tf, ec2.tf), variables for configuration flexibility, and outputs for resource referencing. This makes the code maintainable and follows Terraform best practices."

**"How do you handle different environments?"**
> "I use variables with defaults for dev and can override via terraform.tfvars or -var flags for production. The S3 module accepts environment tags and the code supports conditional resource creation (like EC2) based on variables."

## ⚠️ Current State

**Note**: The existing Project Flume infrastructure was provisioned manually for hands-on learning. This Terraform code provides an IaC alternative demonstrating:

- Infrastructure versioning
- Reproducible deployments
- Team collaboration capability
- Disaster recovery readiness

To use this Terraform code:
1. Destroy manually created resources
2. Update `terraform.tfvars` with your configuration
3. Run `terraform apply`

## 📚 Resources

- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Terraform Modules](https://www.terraform.io/language/modules)
- [AWS Lambda Terraform](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/lambda_function)
- [S3 Best Practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/security-best-practices.html)

## 🤝 Contributing

To modify the infrastructure:

1. Update relevant `.tf` files
2. Test with `terraform plan`
3. Apply with `terraform apply`
4. Commit changes to version control

---

**Built with Terraform for reproducible, version-controlled infrastructure** 🚀