locals {
  is_localstack = var.localstack_endpoint != ""

  organizer_zip = "${path.module}/${var.lambda_zip_dir}/organizer.zip"
  worker_zip    = "${path.module}/${var.lambda_zip_dir}/worker.zip"

  # Base env vars shared by both Lambda functions.
  lambda_env_base = {
    AWS_REGION             = var.aws_region
    F2E_INPUT_BUCKET       = var.f2e_input_bucket
    F2E_RECORD_LENGTH      = tostring(var.f2e_record_length)
    F2E_RECORDS_PER_CHUNK  = tostring(var.f2e_records_per_chunk)
    F2E_BATCH_SIZE         = tostring(var.f2e_batch_size)
    F2E_WORKER_CONCURRENCY = tostring(var.f2e_worker_concurrency)
    F2E_INTAKE_QUEUE_URL   = aws_sqs_queue.file_intake.url
    F2E_CHUNK_QUEUE_URL    = aws_sqs_queue.chunk_jobs.url
    F2E_OUTPUT_QUEUE_URL   = aws_sqs_queue.output_events.url
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
