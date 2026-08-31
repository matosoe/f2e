# ── Dead-letter queues ────────────────────────────────────────────────────────

resource "aws_sqs_queue" "file_intake_dlq" {
  name                       = "file-intake-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  tags                       = local.tags
}

resource "aws_sqs_queue" "chunk_jobs_dlq" {
  name                       = "chunk-jobs-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  tags                       = local.tags
}

resource "aws_sqs_queue" "output_events_dlq" {
  name                       = "output-events-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  tags                       = local.tags
}

# ── Main queues ───────────────────────────────────────────────────────────────

resource "aws_sqs_queue" "file_intake" {
  name                       = "file-intake"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.file_intake_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}

resource "aws_sqs_queue" "chunk_jobs" {
  name                       = "chunk-jobs"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.chunk_jobs_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
}

resource "aws_sqs_queue" "output_events" {
  name                       = "output-events"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.output_events_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = local.tags
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
  }
}

resource "aws_sqs_queue_policy" "file_intake" {
  queue_url = aws_sqs_queue.file_intake.id
  policy    = data.aws_iam_policy_document.file_intake_policy.json
}
