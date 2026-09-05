# ── Module: f2e-prefix ────────────────────────────────────────────────────────
#
# Creates the per-prefix SQS output queue, dedicated Worker Lambda with its
# IAM role, and the event-source mapping that binds the shared chunk-jobs queue
# to this Worker. The Organizer Lambda is shared and reads the prefix's SSM
# configuration, which carries this queue's URL via outputQueueURL (T20).
#
# Usage (root module):
#
#   module "prefix_example_text" {
#     source = "./modules/f2e-prefix"
#     ...
#   }
#
# The root module continues to own: S3 bucket, DynamoDB ledger, shared queues
# (file-intake, chunk-jobs, completion-events), Organizer Lambda, and global
# CloudWatch/monitoring resources.

variable "prefix_id" {
  description = "Canonical identifier for this prefix (a-z A-Z 0-9 - _). Used in resource names."
  type        = string
  validation {
    condition     = can(regex("^[a-zA-Z0-9_-]+$", var.prefix_id))
    error_message = "prefix_id must contain only a-z, A-Z, 0-9, hyphens or underscores."
  }
}

variable "resource_prefix" {
  description = "String prepended to all resource names (e.g. 'f2e')."
  type        = string
}

variable "environment" {
  description = "Deployment environment name (e.g. 'prod', 'staging')."
  type        = string
}

variable "aws_region" {
  description = "AWS region where resources are deployed."
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID used in IAM resource ARNs."
  type        = string
}

variable "tags" {
  description = "Tags applied to all resources created by this module."
  type        = map(string)
  default     = {}
}

variable "kms_key_arn" {
  description = "KMS key ARN for SQS SSE. Leave empty to use SQS-managed keys."
  type        = string
  default     = ""
}

# ── Shared infrastructure ARNs/URLs ──────────────────────────────────────────

variable "input_bucket_arn" {
  description = "ARN of the shared S3 input bucket."
  type        = string
}

variable "chunk_queue_arn" {
  description = "ARN of the shared chunk-jobs SQS queue."
  type        = string
}

variable "ledger_table_arn" {
  description = "ARN of the shared DynamoDB job-ledger table."
  type        = string
}

variable "log_group_arn" {
  description = "ARN of the CloudWatch log group for this Worker Lambda."
  type        = string
}

# ── Worker Lambda configuration ───────────────────────────────────────────────

variable "worker_zip" {
  description = "Local path to the Worker Lambda ZIP (shared binary, per-prefix configuration)."
  type        = string
}

variable "worker_env" {
  description = "Environment variables injected into the Worker Lambda."
  type        = map(string)
  default     = {}
}

variable "worker_memory_mb" {
  description = "Memory in MiB for the Worker Lambda."
  type        = number
  default     = 1024
}

variable "worker_timeout_seconds" {
  description = "Timeout in seconds for the Worker Lambda."
  type        = number
  default     = 900
}

variable "worker_reserved_concurrency" {
  description = <<EOF
Reserved concurrency for this prefix's Worker Lambda.
-1 = unreserved (shares account-level pool, default).
 0 = throttled (Lambda will not execute; use for maintenance windows).
>0 = exact reserved units (removes that many from the account pool).
Choosing >0 provides hard isolation: prefix B Workers cannot consume
reserved slots allocated to prefix A.
EOF
  type        = number
  default     = -1
  validation {
    condition     = var.worker_reserved_concurrency >= -1
    error_message = "worker_reserved_concurrency must be -1 (unreserved), 0 (throttled), or a positive integer."
  }
}

variable "worker_maximum_concurrency" {
  description = <<EOF
Maximum concurrent Lambda instances driven from the event-source mapping
(i.e., maximum SQS pollers). Null means no limit (AWS default is 1000).
Configuring this independently of reserved_concurrency lets you cap SQS
drain speed without fully reserving capacity from the account pool.
EOF
  type        = number
  default     = null
}

variable "sqs_batch_size" {
  description = "SQS event-source mapping batch size for the Worker."
  type        = number
  default     = 10
}

variable "sqs_visibility_timeout" {
  description = "SQS visibility timeout (seconds) for the output queue."
  type        = number
  default     = 900
}

variable "sqs_retention_seconds" {
  description = "SQS message retention period (seconds) for the output queue."
  type        = number
  default     = 345600
}

variable "sqs_dlq_retention_seconds" {
  description = "SQS message retention period (seconds) for the output DLQ."
  type        = number
  default     = 1209600
}

variable "sqs_max_receive_count" {
  description = "Maximum number of receive attempts before a message is moved to the DLQ."
  type        = number
  default     = 3
}

variable "max_event_bytes" {
  description = "Maximum SQS message size (bytes) for the output queue."
  type        = number
  default     = 262144
}

variable "allowed_source_arns" {
  description = <<EOF
List of IAM principal ARNs (role, user, or service) that may send messages to
the output queue. These ARNs are used to generate a resource policy on the SQS
queue, preventing other identities from writing to a prefix's output queue.
An empty list produces no resource policy (queue is unrestricted by ARN).
EOF
  type        = list(string)
  default     = []
}
