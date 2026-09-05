# ── Module: f2e-prefix / main.tf ─────────────────────────────────────────────
#
# Per-prefix resources: output queue (+DLQ), Worker Lambda, IAM role/policy,
# event-source mapping, and optional output-queue resource policy for
# AllowedSourceARNs (T20).
#
# The shared Organizer Lambda propagates this module's output_queue_url via the
# SSM PrefixConfiguration.outputQueueURL field (set outside this module).

locals {
  name_prefix = "${var.resource_prefix}-${var.environment}-${var.prefix_id}"
}

# ── Output queue ──────────────────────────────────────────────────────────────

resource "aws_sqs_queue" "output_dlq" {
  name                       = "${local.name_prefix}-output-dlq"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_dlq_retention_seconds
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags                       = var.tags
}

resource "aws_sqs_queue" "output" {
  name                       = "${local.name_prefix}-output"
  visibility_timeout_seconds = var.sqs_visibility_timeout
  receive_wait_time_seconds  = 1
  message_retention_seconds  = var.sqs_retention_seconds
  max_message_size           = var.max_event_bytes
  sqs_managed_sse_enabled    = var.kms_key_arn == "" ? true : null
  kms_master_key_id          = var.kms_key_arn != "" ? var.kms_key_arn : null
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.output_dlq.arn
    maxReceiveCount     = var.sqs_max_receive_count
  })
  tags = var.tags
}

resource "aws_sqs_queue_redrive_allow_policy" "output" {
  queue_url            = aws_sqs_queue.output_dlq.id
  redrive_allow_policy = jsonencode({ redrivePermission = "byQueue", sourceQueueArns = [aws_sqs_queue.output.arn] })
}

# TLS-only policy on the output queue.
data "aws_iam_policy_document" "output_tls" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["sqs:*"]
    resources = [aws_sqs_queue.output.arn]
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

data "aws_iam_policy_document" "output_allowed_senders" {
  count = length(var.allowed_source_arns) > 0 ? 1 : 0
  statement {
    sid    = "AllowAuthorisedSenders"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.output.arn]
    principals {
      type        = "AWS"
      identifiers = var.allowed_source_arns
    }
  }
  statement {
    sid    = "DenyOtherSenders"
    effect = "Deny"
    actions = [
      "sqs:SendMessage",
    ]
    resources = [aws_sqs_queue.output.arn]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "ArnNotLike"
      variable = "aws:PrincipalArn"
      values   = var.allowed_source_arns
    }
  }
}

data "aws_iam_policy_document" "output_policy" {
  source_policy_documents = concat(
    [data.aws_iam_policy_document.output_tls.json],
    length(var.allowed_source_arns) > 0 ? [data.aws_iam_policy_document.output_allowed_senders[0].json] : []
  )
}

resource "aws_sqs_queue_policy" "output" {
  queue_url = aws_sqs_queue.output.id
  policy    = data.aws_iam_policy_document.output_policy.json
}

resource "aws_sqs_queue_policy" "output_dlq_tls" {
  queue_url = aws_sqs_queue.output_dlq.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Action    = "sqs:*"
      Resource  = aws_sqs_queue.output_dlq.arn
      Principal = { AWS = "*" }
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

# ── Worker IAM ────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "worker" {
  name               = "${local.name_prefix}-worker-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = var.tags
}

data "aws_iam_policy_document" "worker" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${var.log_group_arn}:*"]
  }
  statement {
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
  # Consume from the shared chunk-jobs queue (scoped to this prefix's jobs
  # via message-level prefixId; IAM isolation requires a per-prefix queue —
  # see T21 ADR note on ledger isolation).
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [var.chunk_queue_arn]
  }
  # Publish to THIS prefix's dedicated output queue only.
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.output.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${var.input_bucket_arn}/*"]
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:TransactWriteItems", "dynamodb:UpdateItem"]
    resources = [var.ledger_table_arn]
  }
  dynamic "statement" {
    for_each = var.kms_key_arn == "" ? [] : [1]
    content {
      actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
      resources = [var.kms_key_arn]
      condition {
        test     = "StringLike"
        variable = "kms:ViaService"
        values   = ["s3.${var.aws_region}.amazonaws.com", "sqs.${var.aws_region}.amazonaws.com", "dynamodb.${var.aws_region}.amazonaws.com"]
      }
    }
  }
}

resource "aws_iam_role_policy" "worker" {
  name   = "${local.name_prefix}-worker"
  role   = aws_iam_role.worker.name
  policy = data.aws_iam_policy_document.worker.json
}

# ── Worker Lambda ─────────────────────────────────────────────────────────────

resource "aws_lambda_function" "worker" {
  function_name    = "${local.name_prefix}-worker"
  role             = aws_iam_role.worker.arn
  filename         = var.worker_zip
  source_code_hash = filebase64sha256(var.worker_zip)
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  memory_size      = var.worker_memory_mb
  timeout          = var.worker_timeout_seconds
  tags             = var.tags

  # T20: inject per-prefix output queue URL so the Worker knows where to send.
  environment {
    variables = merge(var.worker_env, {
      F2E_OUTPUT_QUEUE_URL = aws_sqs_queue.output.url
    })
  }

  # reserved_concurrent_executions = -1 means no reservation (account pool).
  # 0 = throttled. >0 = reserved (isolated capacity, T21 requirement).
  reserved_concurrent_executions = var.worker_reserved_concurrency
}

# Event-source mapping: shared chunk-jobs queue → this prefix's Worker.
resource "aws_lambda_event_source_mapping" "worker" {
  event_source_arn                   = var.chunk_queue_arn
  function_name                      = aws_lambda_function.worker.arn
  batch_size                         = var.sqs_batch_size
  maximum_batching_window_in_seconds = 0
  function_response_types            = ["ReportBatchItemFailures"]

  dynamic "scaling_config" {
    for_each = var.worker_maximum_concurrency != null ? [1] : []
    content {
      maximum_concurrency = var.worker_maximum_concurrency
    }
  }
}
