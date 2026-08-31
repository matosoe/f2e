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

variable "f2e_worker_concurrency" {
  type    = number
  default = 4
}

# ── SQS tuning ────────────────────────────────────────────────────────────────

variable "sqs_visibility_timeout" {
  type    = number
  default = 90
}

variable "sqs_max_receive_count" {
  type    = number
  default = 3
}

variable "organizer_batch_size" {
  type    = number
  default = 1
}

variable "worker_batch_size" {
  type    = number
  default = 5
}
