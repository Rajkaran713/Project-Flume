# Security group for EC2 instance
resource "aws_security_group" "ec2_producer_sg" {
  count       = var.enable_ec2 ? 1 : 0
  name        = "${var.project_name}-ec2-producer-sg"
  description = "Security group for data producer EC2 instance"
  
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Restrict to your IP in production
    description = "SSH access"
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Allow all outbound"
  }
  
  tags = {
    Name = "${var.project_name}-ec2-sg"
  }
}

# EC2 instance for data ingestion
resource "aws_instance" "data_producer" {
  count                  = var.enable_ec2 ? 1 : 0
  ami                    = data.aws_ami.amazon_linux_2.id
  instance_type          = var.ec2_instance_type
  key_name              = var.ec2_key_name
  iam_instance_profile  = aws_iam_instance_profile.ec2_producer_profile[0].name
  vpc_security_group_ids = [aws_security_group.ec2_producer_sg[0].id]
  
  user_data = <<-EOF
              #!/bin/bash
              yum update -y
              yum install -y docker
              systemctl start docker
              systemctl enable docker
              usermod -a -G docker ec2-user
              
              # Install AWS CLI
              curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
              unzip awscliv2.zip
              sudo ./aws/install
              
              # Setup cron for data ingestion
              mkdir -p /home/ec2-user/flume-scripts
              echo "*/5 * * * * /home/ec2-user/flume-scripts/run_producer.sh >> /home/ec2-user/flume-scripts/cron.log 2>&1" | crontab -u ec2-user -
              EOF
  
  tags = {
    Name = "${var.project_name}-data-producer"
  }
}

# Latest Amazon Linux 2 AMI
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]
  
  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }
  
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}