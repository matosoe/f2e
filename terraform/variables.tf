# ── Deployment target ────────────────────────────────────────────────────────

variable "localstack_endpoint" {
  type        = string
  default     = ""
  description = "LocalStack endpoint reachable from the Terraform host (e.g. http://localhost:4566). Leave empty for real AWS."
}

variable "lambda_aws_endpoint_url" {
  type        = string
  default     = ""
  description = "AWS_ENDPOINT_URL injected into Lambda functions. For LocalStack use the container-network hostname (http://localstack:4566), not localhost."
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type        = string
  default     = "local"
  description = "Deployment environment label used in resource tags."
  validation {
    condition     = contains(["local", "development", "staging", "production"], var.environment)
    error_message = "environment must be local, development, staging or production."
  }
}

variable "aws_account_id" {
  type        = string
  default     = "000000000000"
  description = "AWS account allowed to publish S3 notifications."
  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must contain exactly 12 digits."
  }
}

variable "resource_prefix" {
  type    = string
  default = "f2e"
  validation {
    condition     = can(regex("^[a-z0-9-]{2,32}$", var.resource_prefix))
    error_message = "resource_prefix must contain 2-32 lowercase letters, digits or hyphens."
  }
}

variable "kms_key_arn" {
  type        = string
  default     = ""
  description = "Corporate KMS key ARN. Required by production policy; empty uses the AWS-managed service key."
  validation {
    condition     = var.environment != "production" || can(regex("^arn:aws[a-z-]*:kms:[a-z0-9-]+:[0-9]{12}:key/[A-Za-z0-9-]+$", var.kms_key_arn))
    error_message = "Production requires a valid customer-managed KMS key ARN."
  }
}

# ── Lambda build artefacts ────────────────────────────────────────────────────

variable "lambda_zip_dir" {
  type        = string
  default     = "../.build"
  description = "Path (relative to the terraform/ directory) that contains organizer.zip and worker.zip."
}

variable "lambda_runtime" {
  type        = string
  default     = "provided.al2023"
  description = "Lambda runtime identifier. Use provided.al2 for older LocalStack versions."
}

variable "lambda_timeout" {
  type    = number
  default = 60
}

variable "lambda_memory_mb" {
  type    = number
  default = 256
}

variable "organizer_reserved_concurrency" {
  type    = number
  default = 2
  validation {
    condition     = var.organizer_reserved_concurrency >= 1
    error_message = "Organizer reserved concurrency must be at least 1."
  }
}

variable "worker_reserved_concurrency" {
  type    = number
  default = 10
  validation {
    condition     = var.worker_reserved_concurrency >= var.worker_maximum_concurrency
    error_message = "Worker reserved concurrency must be at least the event-source maximum concurrency."
  }
}

# ── F2E processing parameters ─────────────────────────────────────────────────

variable "f2e_input_bucket" {
  type    = string
  default = "f2e-input"
}

variable "f2e_record_length" {
  type    = number
  default = 100
}

variable "f2e_records_per_chunk" {
  type    = number
  default = 1000
}

variable "f2e_batch_size" {
  type    = number
  default = 10
}

# ── SQS tuning ────────────────────────────────────────────────────────────────

variable "sqs_visibility_timeout" {
  type    = number
  default = 90
  validation {
    condition     = var.sqs_visibility_timeout >= var.lambda_timeout * 6 && var.sqs_visibility_timeout <= 43200
    error_message = "SQS visibility timeout must be at least six times the Lambda timeout and no more than 12 hours."
  }
}

variable "s3_notification_prefix" {
  type        = string
  default     = ""
  description = "Optional approved intake key prefix."
}

variable "s3_notification_suffix" {
  type        = string
  default     = ""
  description = "Optional approved intake key suffix."
}

variable "sqs_max_receive_count" {
  type    = number
  default = 3
}

variable "f2e_max_event_bytes" {
  type    = number
  default = 262144
  validation {
    condition     = var.f2e_max_event_bytes >= 1024 && var.f2e_max_event_bytes <= 262144
    error_message = "Event size must be between 1 KiB and the SQS 256 KiB limit."
  }
}

variable "f2e_max_file_bytes" {
  type    = number
  default = 10737418240
  validation {
    condition     = var.f2e_max_file_bytes >= var.f2e_max_chunk_bytes
    error_message = "Maximum file size must be at least the maximum chunk size."
  }
}

variable "f2e_max_chunk_bytes" {
  type    = number
  default = 67108864
  validation {
    condition     = var.f2e_max_chunk_bytes >= 1024
    error_message = "Maximum chunk size must be at least 1 KiB."
  }
}

variable "f2e_json_array_search_bytes" {
  type    = number
  default = 1048576
  validation {
    condition     = var.f2e_json_array_search_bytes >= 1024 && var.f2e_json_array_search_bytes <= 16777216
    error_message = "JSON array path search must be between 1 KiB and 16 MiB."
  }
}

variable "enable_preview_formats" {
  type    = bool
  default = false
}

variable "enable_experimental_formats" {
  type    = bool
  default = false
  validation {
    condition     = var.environment != "production" || !var.enable_experimental_formats
    error_message = "Experimental formats cannot be enabled in production."
  }
}

variable "lambda_architecture" {
  type    = string
  default = "x86_64"
  validation {
    condition     = contains(["x86_64", "arm64"], var.lambda_architecture)
    error_message = "lambda_architecture must be x86_64 or arm64."
  }
}

variable "lambda_ephemeral_storage_mb" {
  type    = number
  default = 512
  validation {
    condition     = var.lambda_ephemeral_storage_mb >= 512 && var.lambda_ephemeral_storage_mb <= 10240
    error_message = "Lambda ephemeral storage must be between 512 and 10240 MB."
  }
}

variable "worker_maximum_concurrency" {
  type    = number
  default = 4
  validation {
    condition     = var.worker_maximum_concurrency >= 2 && var.worker_maximum_concurrency <= 1000
    error_message = "SQS event-source maximum concurrency must be between 2 and 1000."
  }
}

variable "sqs_retention_seconds" {
  type    = number
  default = 345600
}

variable "sqs_dlq_retention_seconds" {
  type    = number
  default = 1209600
  validation {
    condition     = var.sqs_dlq_retention_seconds > var.sqs_retention_seconds
    error_message = "DLQ retention must be greater than main queue retention."
  }
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "ledger_retention_days" {
  type    = number
  default = 90
  validation {
    condition     = var.ledger_retention_days >= 1
    error_message = "Ledger retention must be at least one day."
  }
}

variable "backlog_age_alarm_seconds" {
  type    = number
  default = 300
}

variable "organizer_batch_size" {
  type    = number
  default = 1
}

variable "worker_batch_size" {
  type    = number
  default = 1
  validation {
    condition     = var.worker_batch_size == 1
    error_message = "Worker batch size must be 1 so every chunk has an independent Lambda timeout and retry lifecycle."
  }
}
