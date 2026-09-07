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

  # intentPending is set to "1" on COMPLETION_INTENT items that have not yet
  # been delivered. The publisher clears it after SQS confirms the send.
  # Using a sparse GSI on this attribute means only pending intents appear
  # in the index, enabling efficient recovery without a full Scan.
  attribute {
    name = "intentPending"
    type = "S"
  }

  global_secondary_index {
    name            = "pending-intents-index"
    hash_key        = "intentPending"
    range_key       = "sk"
    projection_type = "ALL"
  }

  # Waiting admissions are queued when a prefix has exhausted maxActiveJobs.
  # The single marker keeps the scheduler query small; queuedAt provides a
  # stable FIFO order and the Organizer releases at most one item per prefix.
  attribute {
    name = "waitingAdmission"
    type = "S"
  }
  attribute {
    name = "queuedAt"
    type = "S"
  }

  global_secondary_index {
    name            = "waiting-admissions-index"
    hash_key        = "waitingAdmission"
    range_key       = "queuedAt"
    projection_type = "ALL"
  }

  # DynamoDB Streams feeds the completion-publisher Lambda (T13). NEW_AND_OLD_IMAGES
  # is required so the publisher can read the intent payload even when the item
  # already existed before the Streams window.
  stream_enabled   = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

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
