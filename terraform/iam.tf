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

resource "aws_iam_role" "lambda" {
  name               = "f2e-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.tags
}

# AWS-managed basic execution policy (CloudWatch Logs).
# Skipped for LocalStack since managed policies are not always present in Community edition.
resource "aws_iam_role_policy_attachment" "basic_execution" {
  count      = local.is_localstack ? 0 : 1
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_permissions" {
  statement {
    sid    = "SQSConsume"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [
      aws_sqs_queue.file_intake.arn,
      aws_sqs_queue.chunk_jobs.arn,
    ]
  }

  statement {
    sid    = "SQSPublish"
    effect = "Allow"
    actions = [
      "sqs:SendMessage",
      "sqs:GetQueueUrl",
    ]
    resources = [
      aws_sqs_queue.chunk_jobs.arn,
      aws_sqs_queue.output_events.arn,
    ]
  }

  statement {
    sid    = "S3Read"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:HeadObject",
    ]
    resources = ["${aws_s3_bucket.input.arn}/*"]
  }
}

resource "aws_iam_role_policy" "lambda_permissions" {
  name   = "f2e-lambda-permissions"
  role   = aws_iam_role.lambda.name
  policy = data.aws_iam_policy_document.lambda_permissions.json
}
