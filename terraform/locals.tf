locals {
  is_localstack = var.localstack_endpoint != ""

  organizer_zip = "${path.module}/${var.lambda_zip_dir}/organizer.zip"
  worker_zip    = "${path.module}/${var.lambda_zip_dir}/worker.zip"

  # Base env vars shared by both Lambda functions.
  lambda_env_base = {
    F2E_ENVIRONMENT             = var.environment
    F2E_INPUT_BUCKET            = var.f2e_input_bucket
    F2E_RECORD_LENGTH           = tostring(var.f2e_record_length)
    F2E_RECORDS_PER_CHUNK       = tostring(var.f2e_records_per_chunk)
    F2E_BATCH_SIZE              = tostring(var.f2e_batch_size)
    F2E_MAX_EVENT_BYTES         = tostring(var.f2e_max_event_bytes)
    F2E_MAX_FILE_BYTES          = tostring(var.f2e_max_file_bytes)
    F2E_MAX_CHUNK_BYTES         = tostring(var.f2e_max_chunk_bytes)
    F2E_MAX_RECEIVE_COUNT       = tostring(var.sqs_max_receive_count)
    F2E_JSON_ARRAY_SEARCH_BYTES = tostring(var.f2e_json_array_search_bytes)
    F2E_INTAKE_QUEUE_URL        = aws_sqs_queue.file_intake.url
    F2E_CHUNK_QUEUE_URL         = aws_sqs_queue.chunk_jobs.url
    F2E_OUTPUT_QUEUE_URL        = aws_sqs_queue.output_events.url
    F2E_LEDGER_TABLE            = aws_dynamodb_table.job_ledger.name
    F2E_LEDGER_RETENTION_DAYS   = tostring(var.ledger_retention_days)
  }

  # Add AWS_ENDPOINT_URL only when a Lambda-internal endpoint is provided (LocalStack).
  lambda_env = var.lambda_aws_endpoint_url != "" ? merge(local.lambda_env_base, {
    AWS_ENDPOINT_URL = var.lambda_aws_endpoint_url
  }) : local.lambda_env_base

  tags = {
    Project     = "f2e"
    Environment = var.environment
  }
}
