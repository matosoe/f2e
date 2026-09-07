# ── Per-prefix Worker modules (T21) ─────────────────────────────────────────
#
# For every entry in locals.file_configurations we instantiate one
# f2e-prefix module that creates:
#   - A dedicated SQS output queue (+DLQ)
#   - A dedicated Worker Lambda with its IAM role and event-source mapping
#
# The shared resources (Organizer, S3 bucket, chunk-jobs queue, DynamoDB
# ledger, completion-publisher) remain in the root module.
#
# Isolation levels (T21 acceptance criteria):
# - SQS: each prefix has its own output queue; Workers from prefix A cannot
#   publish to prefix B's queue because IAM policies are scoped to the per-
#   prefix queue ARN.
# - Lambda concurrency: setting reserved_concurrency > 0 per prefix removes
#   that many units from the account pool, giving prefix B a hard guarantee
#   that prefix A workloads cannot starve it (and vice versa).
# - Ledger isolation: the ledger table is shared; IAM isolation at item level
#   requires attribute-based access control (ABAC) or per-prefix tables.
#   Using prefixId in the item partition key alone is NOT sufficient for IAM
#   isolation. Current approach: per-prefix IAM role scoped to the table but
#   not to individual items.
#
# Migration note:
# Existing Terraform state uses the root-module aws_lambda_function.worker and
# aws_sqs_queue.output_events resources. Before apply, run:
#
#   terraform state mv aws_lambda_function.worker \
#     module.prefix["example-text"].aws_lambda_function.worker
#   terraform state mv aws_sqs_queue.output_events \
#     module.prefix["example-text"].aws_sqs_queue.output
#
# for each existing prefix, review the generated plan, and confirm no
# destroy/replace of queues with live traffic before applying.

locals {
  # Merge per-prefix overrides from var.prefix_worker_config.
  # For each file_configuration key we build the effective Worker config.
  prefix_worker_effective = {
    for k, cfg in local.file_configurations : k => {
      reserved_concurrency = try(var.prefix_worker_config[k].reserved_concurrency, -1)
      maximum_concurrency  = try(var.prefix_worker_config[k].maximum_concurrency, null)
      memory_mb            = try(var.prefix_worker_config[k].memory_mb, 1024)
    }
  }

  # Base Worker env vars shared across all per-prefix Workers.
  # Per-module: F2E_OUTPUT_QUEUE_URL is injected by the module itself.
  worker_env_base = {
    F2E_ENVIRONMENT             = var.environment
    F2E_INPUT_BUCKET            = var.f2e_input_bucket
    F2E_RECORDS_PER_CHUNK       = tostring(var.f2e_records_per_chunk)
    F2E_BATCH_SIZE              = tostring(var.f2e_batch_size)
    F2E_MAX_EVENT_BYTES         = tostring(var.f2e_max_event_bytes)
    F2E_MAX_FILE_BYTES          = tostring(var.f2e_max_file_bytes)
    F2E_MAX_CHUNK_BYTES         = tostring(var.f2e_max_chunk_bytes)
    F2E_MAX_RECEIVE_COUNT       = tostring(var.sqs_max_receive_count)
    F2E_JSON_ARRAY_SEARCH_BYTES = tostring(var.f2e_json_array_search_bytes)
    F2E_INTAKE_QUEUE_URL        = aws_sqs_queue.file_intake.url
    F2E_CHUNK_QUEUE_URL         = aws_sqs_queue.chunk_jobs.url
    F2E_LEDGER_TABLE            = aws_dynamodb_table.job_ledger.name
    F2E_LEDGER_RETENTION_DAYS   = tostring(var.ledger_retention_days)
    F2E_FILE_CONFIG_PATH        = local.file_config_path
    F2E_GLOBAL_LIMITS_PARAMETER = local.global_limits_parameter
  }

  worker_env_with_endpoint = var.lambda_aws_endpoint_url != "" ? merge(local.worker_env_base, {
    AWS_ENDPOINT_URL = var.lambda_aws_endpoint_url
  }) : local.worker_env_base
}

module "prefix" {
  for_each = local.file_configurations
  source   = "./modules/f2e-prefix"

  prefix_id       = each.key
  resource_prefix = var.resource_prefix
  environment     = var.environment
  aws_region      = var.aws_region
  aws_account_id  = var.aws_account_id
  tags            = local.tags
  kms_key_arn     = var.kms_key_arn

  # Shared infrastructure references.
  input_bucket_arn = aws_s3_bucket.input.arn
  chunk_queue_arn  = aws_sqs_queue.chunk_jobs.arn
  ledger_table_arn = aws_dynamodb_table.job_ledger.arn
  # Each prefix shares the shared-Worker log group for now; operators may
  # split log groups once Terraform migration is complete.
  log_group_arn = aws_cloudwatch_log_group.worker.arn

  # Worker binary: shared ZIP, per-prefix configuration via env vars + SSM.
  worker_zip = local.worker_zip
  worker_env = local.worker_env_with_endpoint

  # Per-prefix concurrency and sizing.
  worker_reserved_concurrency = local.prefix_worker_effective[each.key].reserved_concurrency
  worker_maximum_concurrency  = local.prefix_worker_effective[each.key].maximum_concurrency
  worker_memory_mb            = local.prefix_worker_effective[each.key].memory_mb
  worker_timeout_seconds      = var.lambda_timeout

  sqs_batch_size            = var.worker_batch_size
  sqs_visibility_timeout    = var.sqs_visibility_timeout
  sqs_retention_seconds     = var.sqs_retention_seconds
  sqs_dlq_retention_seconds = var.sqs_dlq_retention_seconds
  sqs_max_receive_count     = var.sqs_max_receive_count
  max_event_bytes           = var.f2e_max_event_bytes

  # AllowedSourceARNs are sourced from the SSM PrefixConfiguration (T20).
  # They are not available in Terraform locals at plan time; set them here
  # if you want Terraform to also generate queue resource policies.
  allowed_source_arns = []
}
