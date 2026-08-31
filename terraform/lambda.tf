resource "aws_lambda_function" "organizer" {
  function_name    = "f2e-organizer"
  filename         = local.organizer_zip
  source_code_hash = filebase64sha256(local.organizer_zip)
  handler          = "bootstrap"
  runtime          = var.lambda_runtime
  role             = aws_iam_role.lambda.arn
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_mb

  environment {
    variables = local.lambda_env
  }

  tags = local.tags
}

resource "aws_lambda_function" "worker" {
  function_name    = "f2e-worker"
  filename         = local.worker_zip
  source_code_hash = filebase64sha256(local.worker_zip)
  handler          = "bootstrap"
  runtime          = var.lambda_runtime
  role             = aws_iam_role.lambda.arn
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_mb

  environment {
    variables = local.lambda_env
  }

  tags = local.tags
}

# ── Event source mappings ─────────────────────────────────────────────────────

resource "aws_lambda_event_source_mapping" "organizer_intake" {
  event_source_arn        = aws_sqs_queue.file_intake.arn
  function_name           = aws_lambda_function.organizer.arn
  batch_size              = var.organizer_batch_size
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_lambda_event_source_mapping" "worker_chunk" {
  event_source_arn        = aws_sqs_queue.chunk_jobs.arn
  function_name           = aws_lambda_function.worker.arn
  batch_size              = var.worker_batch_size
  function_response_types = ["ReportBatchItemFailures"]
}
