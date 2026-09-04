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
    F2E_FILE_CONFIG_PATH        = local.file_config_path
    F2E_GLOBAL_LIMITS_PARAMETER = local.global_limits_parameter
  }

  # Add AWS_ENDPOINT_URL only when a Lambda-internal endpoint is provided (LocalStack).
  lambda_env = var.lambda_aws_endpoint_url != "" ? merge(local.lambda_env_base, {
    AWS_ENDPOINT_URL = var.lambda_aws_endpoint_url
  }) : local.lambda_env_base

  tags = {
    Project     = "f2e"
    Environment = var.environment
  }

  file_configuration_base = {
    bucket               = var.f2e_input_bucket
    recordLengthBytes    = var.f2e_record_length
    recordsPerChunk      = var.f2e_records_per_chunk
    batchSize            = var.f2e_batch_size
    maxEventBytes        = var.f2e_max_event_bytes
    maxFileBytes         = var.f2e_max_file_bytes
    maxChunkBytes        = var.f2e_max_chunk_bytes
    jsonArraySearchBytes = var.f2e_json_array_search_bytes
    maxRecordLengthBytes = 0
    eventSchemaId        = "f2e-record"
    eventSchemaVersion   = "1"
    eventFormat          = "json"
    options              = { bypassJsonValidation = false }
  }

  file_configurations = {
    "example-fixed-width" = merge(local.file_configuration_base, { prefix = "example-fixed-width/", dataType = "fixed-width" })
    "example-text"        = merge(local.file_configuration_base, { prefix = "example-text/", dataType = "text", maxRecordLengthBytes = 65536 })
    "example-jsonl"       = merge(local.file_configuration_base, { prefix = "example-jsonl/", dataType = "jsonl", maxRecordLengthBytes = 65536 })
    "example-ndjson"      = merge(local.file_configuration_base, { prefix = "example-ndjson/", dataType = "ndjson", maxRecordLengthBytes = 65536 })
    "example-csv"         = merge(local.file_configuration_base, { prefix = "example-csv/", dataType = "csv", maxFileBytes = 67108864, maxChunkBytes = 67108864 })
    "example-binary"      = merge(local.file_configuration_base, { prefix = "example-binary/", dataType = "binary", maxFileBytes = 193536, maxChunkBytes = 193536 })
    "example-json" = merge(local.file_configuration_base, {
      prefix          = "example-json/", dataType = "json",
      jsonArrayLayout = { arrayPath = "", maxBytesPerElement = 65536 }
    })
    "example-multi-line" = merge(local.file_configuration_base, {
      prefix          = "example-multi-line/", dataType = "multi-line",
      multiLineLayout = { breakPosition = 0, breakMarker = "1", acceptedPrefixes = [], lineSeparator = "\u001c", maxBytesPerRecord = 65536 }
    })
  }

  global_limits = {
    maxFileBytes            = var.f2e_max_file_bytes
    maxChunkBytes           = var.f2e_max_chunk_bytes
    maxEventBytes           = var.f2e_max_event_bytes
    maxBatchSize            = 10
    maxJsonArraySearchBytes = 16777216
    inputTypes = {
      "fixed-width" = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
      text          = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
      jsonl         = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
      ndjson        = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
      csv           = { maxFileBytes = 67108864, maxRecordBytes = 258048 }
      json          = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
      binary        = { maxFileBytes = 193536, maxRecordBytes = 193536 }
      "multi-line"  = { maxFileBytes = 10737418240, maxRecordBytes = 258048 }
    }
  }
}
