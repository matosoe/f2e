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

  # status-time-index (T14): enables operational queries by status and time range
  # without a full Scan. Only JOB aggregate items carry a statusIndex attribute
  # (set on every state transition). Non-terminal jobs with an old updatedAt are
  # candidates for the stuck-job reconciler.
  attribute {
    name = "statusIndex"
    type = "S"
  }
  attribute {
    name = "updatedAt"
    type = "S"
  }

  global_secondary_index {
    name            = "status-time-index"
    hash_key        = "statusIndex"
    range_key       = "updatedAt"
    projection_type = "INCLUDE"
    non_key_attributes = [
      "jobId", "fileId", "receiptId", "status", "createdAt", "environment"
    ]
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
