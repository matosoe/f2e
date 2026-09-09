terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.62"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # LocalStack: static credentials and skip AWS-only validation steps.
  access_key                  = local.is_localstack ? "test" : null
  secret_key                  = local.is_localstack ? "test" : null
  skip_credentials_validation = local.is_localstack
  skip_requesting_account_id  = local.is_localstack
  skip_metadata_api_check     = local.is_localstack
  s3_use_path_style           = local.is_localstack

  dynamic "endpoints" {
    for_each = local.is_localstack ? [var.localstack_endpoint] : []
    content {
      s3         = endpoints.value
      sqs        = endpoints.value
      lambda     = endpoints.value
      iam        = endpoints.value
      logs       = endpoints.value
      dynamodb   = endpoints.value
      cloudwatch = endpoints.value
      ssm        = endpoints.value
    }
  }


  # Allows the management client to use AWS's public dual-stack SSM endpoint
  # when local DNS redirects the legacy hostname to an unreachable private IP.
  dynamic "endpoints" {
    for_each = !local.is_localstack && var.ssm_endpoint != "" ? [var.ssm_endpoint] : []
    content {
      ssm = endpoints.value
    }
  }
}
