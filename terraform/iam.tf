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

resource "aws_iam_role" "organizer" {
  name               = "${var.resource_prefix}-${var.environment}-organizer-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}
resource "aws_iam_role" "worker" {
  name               = "${var.resource_prefix}-${var.environment}-worker-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}
data "aws_iam_policy_document" "organizer" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.organizer.arn}:*"]
  }
  statement {
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.file_intake.arn]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [for m in module.prefix : m.chunk_queue_arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.input.arn}/*"]
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:DeleteItem", "dynamodb:Query", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.job_ledger.arn, "${aws_dynamodb_table.job_ledger.arn}/index/waiting-admissions-index"]
  }
  statement {
    actions = ["ssm:GetParameter", "ssm:GetParametersByPath"]
    resources = [
      "arn:aws:ssm:${var.aws_region}:${var.aws_account_id}:parameter${local.global_limits_parameter}",
      "arn:aws:ssm:${var.aws_region}:${var.aws_account_id}:parameter${local.file_config_path}/${var.f2e_input_bucket}/*"
    ]
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
data "aws_iam_policy_document" "worker" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.worker.arn}:*"]
  }
  statement {
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"]
    resources = [aws_sqs_queue.chunk_jobs.arn]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.output_events.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.input.arn}/*"]
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:TransactWriteItems", "dynamodb:UpdateItem"]
    resources = [aws_dynamodb_table.job_ledger.arn]
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
resource "aws_iam_role_policy" "organizer" {
  name   = "${var.resource_prefix}-${var.environment}-organizer"
  role   = aws_iam_role.organizer.name
  policy = data.aws_iam_policy_document.organizer.json
}
resource "aws_iam_role_policy" "worker" {
  name   = "${var.resource_prefix}-${var.environment}-worker"
  role   = aws_iam_role.worker.name
  policy = data.aws_iam_policy_document.worker.json
}

# ── Completion-publisher IAM (T13) ────────────────────────────────────────────

resource "aws_iam_role" "completion_publisher" {
  name               = "${var.resource_prefix}-${var.environment}-completion-publisher-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}

data "aws_iam_policy_document" "completion_publisher" {
  statement {
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.completion_publisher.arn}:*"]
  }
  statement {
    actions   = ["xray:PutTraceSegments", "xray:PutTelemetryRecords"]
    resources = ["*"]
  }
  # DynamoDB Streams read access on the ledger table.
  statement {
    actions = [
      "dynamodb:GetRecords",
      "dynamodb:GetShardIterator",
      "dynamodb:DescribeStream",
      "dynamodb:ListStreams",
    ]
    resources = ["${aws_dynamodb_table.job_ledger.arn}/stream/*"]
  }
  # Query the pending-intents-index GSI for the recovery path.
  statement {
    actions   = ["dynamodb:Query"]
    resources = ["${aws_dynamodb_table.job_ledger.arn}/index/pending-intents-index"]
  }
  # MarkIntentDelivered: UpdateItem on the ledger table.
  statement {
    actions   = ["dynamodb:UpdateItem", "dynamodb:GetItem"]
    resources = [aws_dynamodb_table.job_ledger.arn]
  }
  # Send completion events to the dedicated queue.
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.completion_events.arn]
  }
  # Write failed-batch records to the DLQ (on_failure destination).
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.completion_events_dlq.arn]
  }
  dynamic "statement" {
    for_each = var.kms_key_arn == "" ? [] : [1]
    content {
      actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
      resources = [var.kms_key_arn]
      condition {
        test     = "StringLike"
        variable = "kms:ViaService"
        values   = ["sqs.${var.aws_region}.amazonaws.com", "dynamodb.${var.aws_region}.amazonaws.com"]
      }
    }
  }
}

resource "aws_iam_role_policy" "completion_publisher" {
  name   = "${var.resource_prefix}-${var.environment}-completion-publisher"
  role   = aws_iam_role.completion_publisher.name
  policy = data.aws_iam_policy_document.completion_publisher.json
}
