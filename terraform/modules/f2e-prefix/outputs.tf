output "output_queue_url" {
  description = "URL of the per-prefix SQS output queue. Set this as PrefixConfiguration.outputQueueURL in SSM."
  value       = aws_sqs_queue.output.url
}

output "output_queue_arn" {
  description = "ARN of the per-prefix SQS output queue."
  value       = aws_sqs_queue.output.arn
}

output "output_dlq_url" {
  description = "URL of the per-prefix output dead-letter queue."
  value       = aws_sqs_queue.output_dlq.url
}

output "output_dlq_arn" {
  description = "ARN of the per-prefix output dead-letter queue."
  value       = aws_sqs_queue.output_dlq.arn
}

output "worker_function_arn" {
  description = "ARN of the per-prefix Worker Lambda function."
  value       = aws_lambda_function.worker.arn
}

output "worker_function_name" {
  description = "Name of the per-prefix Worker Lambda function."
  value       = aws_lambda_function.worker.function_name
}

output "worker_role_arn" {
  description = "ARN of the per-prefix Worker Lambda IAM role."
  value       = aws_iam_role.worker.arn
}
