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
    resources = [aws_sqs_queue.chunk_jobs.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.input.arn}/*"]
  }
  statement {
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:Query", "dynamodb:UpdateItem"]
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
