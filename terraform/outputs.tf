output "input_bucket" {
  value = aws_s3_bucket.input.id
}

output "aws_region" {
  value = var.aws_region
}

output "environment" {
  value = var.environment
}

output "s3_notification_prefix" {
  value = var.s3_notification_prefix
}

output "s3_configured_prefixes" {
  value = sort([for configuration in local.file_configurations : configuration.prefix])
}

output "ssm_file_config_path" {
  value = local.file_config_path
}

output "ssm_global_limits_parameter" {
  value = local.global_limits_parameter
}

output "file_intake_queue_url" {
  value = aws_sqs_queue.file_intake.url
}

output "file_intake_queue_name" {
  value = aws_sqs_queue.file_intake.name
}

output "chunk_jobs_queue_url" {
  value = aws_sqs_queue.chunk_jobs.url
}

output "output_events_queue_url" {
  value = aws_sqs_queue.output_events.url
}

output "output_events_queue_name" {
  value = aws_sqs_queue.output_events.name
}

output "file_intake_dlq_name" {
  value = aws_sqs_queue.file_intake_dlq.name
}

output "chunk_jobs_dlq_name" {
  value = aws_sqs_queue.chunk_jobs_dlq.name
}

output "ledger_table_name" {
  value = aws_dynamodb_table.job_ledger.name
}

output "organizer_function_arn" {
  value = aws_lambda_function.organizer.arn
}

output "worker_function_arn" {
  value = aws_lambda_function.worker.arn
}

output "organizer_role_arn" {
  value = aws_iam_role.organizer.arn
}

output "worker_role_arn" {
  value = aws_iam_role.worker.arn
}
