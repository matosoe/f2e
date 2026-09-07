resource "aws_cloudwatch_log_group" "organizer" {
  name              = "/aws/lambda/${var.resource_prefix}-${var.environment}-organizer"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags              = local.tags
}

resource "aws_cloudwatch_log_group" "worker" {
  name              = "/aws/lambda/${var.resource_prefix}-${var.environment}-worker"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags              = local.tags
}

resource "aws_lambda_function" "organizer" {
  function_name                  = "${var.resource_prefix}-${var.environment}-organizer"
  filename                       = local.organizer_zip
  source_code_hash               = filebase64sha256(local.organizer_zip)
  handler                        = "bootstrap"
  runtime                        = var.lambda_runtime
  role                           = aws_iam_role.organizer.arn
  timeout                        = var.lambda_timeout
  memory_size                    = var.lambda_memory_mb
  architectures                  = [var.lambda_architecture]
  reserved_concurrent_executions = var.organizer_reserved_concurrency > 0 ? var.organizer_reserved_concurrency : -1
  publish                        = true

  ephemeral_storage { size = var.lambda_ephemeral_storage_mb }
  logging_config { log_format = "JSON" }
  tracing_config { mode = local.is_localstack ? "PassThrough" : "Active" }

  environment {
    variables = local.lambda_env
  }

  tags       = local.tags
  depends_on = [aws_cloudwatch_log_group.organizer, aws_ssm_parameter.global_limits]
}

resource "aws_lambda_function" "worker" {
  function_name                  = "${var.resource_prefix}-${var.environment}-worker"
  filename                       = local.worker_zip
  source_code_hash               = filebase64sha256(local.worker_zip)
  handler                        = "bootstrap"
  runtime                        = var.lambda_runtime
  role                           = aws_iam_role.worker.arn
  timeout                        = var.lambda_timeout
  memory_size                    = var.lambda_memory_mb
  architectures                  = [var.lambda_architecture]
  reserved_concurrent_executions = var.worker_reserved_concurrency > 0 ? var.worker_reserved_concurrency : -1
  publish                        = true

  ephemeral_storage { size = var.lambda_ephemeral_storage_mb }
  logging_config { log_format = "JSON" }
  tracing_config { mode = local.is_localstack ? "PassThrough" : "Active" }

  environment {
    variables = local.lambda_env
  }

  tags       = local.tags
  depends_on = [aws_cloudwatch_log_group.worker]
}

# ── Completion-publisher Lambda (T13) ─────────────────────────────────────────
#
# Triggered by DynamoDB Streams on the job-ledger table. For every new
# COMPLETION_INTENT# item with intentPending="1" it publishes the event to the
# completion_events queue and marks the intent as delivered.

resource "aws_cloudwatch_log_group" "completion_publisher" {
  name              = "/aws/lambda/${var.resource_prefix}-${var.environment}-completion-publisher"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn != "" ? var.kms_key_arn : null
  tags              = local.tags
}

resource "aws_lambda_function" "completion_publisher" {
  function_name    = "${var.resource_prefix}-${var.environment}-completion-publisher"
  filename         = local.completion_publisher_zip
  source_code_hash = filebase64sha256(local.completion_publisher_zip)
  handler          = "bootstrap"
  runtime          = var.lambda_runtime
  role             = aws_iam_role.completion_publisher.arn
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_mb
  architectures    = [var.lambda_architecture]
  publish          = true

  ephemeral_storage { size = var.lambda_ephemeral_storage_mb }
  logging_config { log_format = "JSON" }
  tracing_config { mode = local.is_localstack ? "PassThrough" : "Active" }

  environment {
    variables = merge(local.lambda_env, {
      F2E_COMPLETION_QUEUE_URL = aws_sqs_queue.completion_events.url
    })
  }

  tags       = local.tags
  depends_on = [aws_cloudwatch_log_group.completion_publisher]
}

resource "aws_lambda_alias" "completion_publisher_live" {
  name             = "live"
  function_name    = aws_lambda_function.completion_publisher.function_name
  function_version = aws_lambda_function.completion_publisher.version
}

# DynamoDB Streams event source: only COMPLETION_INTENT inserts are relevant;
# the Lambda filters internally. bisect_batch_on_function_error retries only
# the failing record on failure rather than the entire batch.
resource "aws_lambda_event_source_mapping" "completion_publisher_streams" {
  event_source_arn               = aws_dynamodb_table.job_ledger.stream_arn
  function_name                  = aws_lambda_alias.completion_publisher_live.arn
  starting_position              = "LATEST"
  batch_size                     = 10
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 3

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.completion_events_dlq.arn
    }
  }
}

resource "aws_lambda_alias" "organizer_live" {
  name             = "live"
  function_name    = aws_lambda_function.organizer.function_name
  function_version = aws_lambda_function.organizer.version
}

resource "aws_lambda_alias" "worker_live" {
  name             = "live"
  function_name    = aws_lambda_function.worker.function_name
  function_version = aws_lambda_function.worker.version
}

# ── Event source mappings ─────────────────────────────────────────────────────

resource "aws_lambda_event_source_mapping" "organizer_intake" {
  event_source_arn        = aws_sqs_queue.file_intake.arn
  function_name           = aws_lambda_alias.organizer_live.arn
  batch_size              = var.organizer_batch_size
  function_response_types = ["ReportBatchItemFailures"]
}

resource "aws_lambda_event_source_mapping" "worker_chunk" {
  event_source_arn        = aws_sqs_queue.chunk_jobs.arn
  function_name           = aws_lambda_alias.worker_live.arn
  batch_size              = var.worker_batch_size
  function_response_types = ["ReportBatchItemFailures"]
  scaling_config { maximum_concurrency = var.worker_maximum_concurrency }
}
