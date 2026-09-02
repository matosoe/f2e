resource "aws_dynamodb_table" "job_ledger" {
  name         = "${var.resource_prefix}-${var.environment}-job-ledger"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"
  range_key    = "sk"

  attribute {
    name = "pk"
    type = "S"
  }

  attribute {
    name = "sk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = !local.is_localstack
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn != "" ? var.kms_key_arn : null
  }

  ttl {
    attribute_name = "expiresAt"
    enabled        = true
  }

  tags = local.tags
}
