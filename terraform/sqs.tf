# ── Dead-letter queues ────────────────────────────────────────────────────────

resource "aws_sqs_queue" "file_intake_dlq" {
  name                       = "${var.resource_prefix}-${var.environment}-file-intake-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_dlq_retention_seconds
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags                       = local.tags
}

resource "aws_sqs_queue" "chunk_jobs_dlq" {
  name                       = "${var.resource_prefix}-${var.environment}-chunk-jobs-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_dlq_retention_seconds
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags                       = local.tags
}

# ── Main queues ───────────────────────────────────────────────────────────────

resource "aws_sqs_queue" "file_intake" {
  name                       = "${var.resource_prefix}-${var.environment}-file-intake"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_retention_seconds
  max_message_size           = 262144
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.file_intake_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}

resource "aws_sqs_queue" "chunk_jobs" {
  name                       = "${var.resource_prefix}-${var.environment}-chunk-jobs"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_retention_seconds
  max_message_size           = 262144
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.chunk_jobs_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}

resource "aws_sqs_queue" "output_events" {
  name                       = "${var.resource_prefix}-${var.environment}-output-events"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_retention_seconds
  max_message_size           = var.f2e_max_event_bytes
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags                       = local.tags
}

# ── Queue policy: allow S3 to publish S3-event notifications ──────────────────

data "aws_iam_policy_document" "file_intake_policy" {
  statement {
    sid    = "AllowS3SendMessage"
    effect = "Allow"
    principals {
      # LocalStack does not enforce the principal restriction, so use wildcard there
      # to avoid dependency on full IAM evaluation.
      type        = local.is_localstack ? "*" : "Service"
      identifiers = local.is_localstack ? ["*"] : ["s3.amazonaws.com"]
    }
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.file_intake.arn]

    dynamic "condition" {
      for_each = local.is_localstack ? [] : [1]
      content {
        test     = "ArnLike"
        variable = "aws:SourceArn"
        values   = [aws_s3_bucket.input.arn]
      }
    }
    dynamic "condition" {
      for_each = local.is_localstack ? [] : [1]
      content {
        test     = "StringEquals"
        variable = "aws:SourceAccount"
        values   = [var.aws_account_id]
      }
    }
  }
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.file_intake.arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "file_intake" {
  queue_url = aws_sqs_queue.file_intake.id
  policy    = data.aws_iam_policy_document.file_intake_policy.json
}

locals {
  tls_only_queues = {
    chunk_jobs            = aws_sqs_queue.chunk_jobs
    output_events         = aws_sqs_queue.output_events
    file_intake_dlq       = aws_sqs_queue.file_intake_dlq
    chunk_jobs_dlq        = aws_sqs_queue.chunk_jobs_dlq
    completion_events     = aws_sqs_queue.completion_events
    completion_events_dlq = aws_sqs_queue.completion_events_dlq
  }
}

data "aws_iam_policy_document" "queue_tls" {
  for_each = local.tls_only_queues
  statement {
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [each.value.arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_sqs_queue_policy" "tls" {
  for_each  = local.tls_only_queues
  queue_url = each.value.id
  policy    = data.aws_iam_policy_document.queue_tls[each.key].json
}

resource "aws_sqs_queue_redrive_allow_policy" "file_intake" {
  queue_url            = aws_sqs_queue.file_intake_dlq.id
  redrive_allow_policy = jsonencode({ redrivePermission = "byQueue", sourceQueueArns = [aws_sqs_queue.file_intake.arn] })
}
resource "aws_sqs_queue_redrive_allow_policy" "chunk_jobs" {
  queue_url            = aws_sqs_queue.chunk_jobs_dlq.id
  redrive_allow_policy = jsonencode({ redrivePermission = "byQueue", sourceQueueArns = [aws_sqs_queue.chunk_jobs.arn] })
}

# ── Completion events queue (T13) ─────────────────────────────────────────────
#
# The completion-publisher Lambda writes a single terminal-state event per job
# to this queue. Consumers subscribe here for job-completion notifications; they
# are separate from the record-envelope output_events queue.

resource "aws_sqs_queue" "completion_events_dlq" {
  name                       = "${var.resource_prefix}-${var.environment}-completion-events-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_dlq_retention_seconds
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags                       = local.tags
}

resource "aws_sqs_queue" "completion_events" {
  name                       = "${var.resource_prefix}-${var.environment}-completion-events"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_retention_seconds
  max_message_size           = 262144
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.completion_events_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "completion_events" {
  queue_url            = aws_sqs_queue.completion_events_dlq.id
  redrive_allow_policy = jsonencode({ redrivePermission = "byQueue", sourceQueueArns = [aws_sqs_queue.completion_events.arn] })
}
