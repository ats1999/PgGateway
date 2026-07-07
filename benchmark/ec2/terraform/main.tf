terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Ubuntu 22.04 AMI, published by Canonical to SSM
data "aws_ssm_parameter" "ubuntu_ami" {
  name = "/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id"
}

resource "aws_security_group" "bench" {
  name        = "pg-bench-${var.run_id}"
  description = "PgGateway benchmark ${var.run_id}"

  ingress {
    description = "SSH from anywhere (key-auth only)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "All traffic within the security group (inter-instance)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "pg-bench-${var.run_id}"
    project = "pggateway-bench"
  }
}

resource "aws_instance" "bench" {
  for_each = toset(["client", "pooler", "postgres"])

  ami                    = data.aws_ssm_parameter.ubuntu_ami.value
  instance_type          = var.instance_type
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.bench.id]

  tags = {
    Name          = "pg-bench-${var.run_id}-${each.key}"
    benchmark-run = var.run_id
    project       = "pggateway-bench"
    role          = each.key
  }
}
