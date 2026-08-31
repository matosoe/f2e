output "input_bucket" {
  value = aws_s3_bucket.input.id
}

output "file_intake_queue_url" {
  value = aws_sqs_queue.file_intake.url
}

output "chunk_jobs_queue_url" {
  value = aws_sqs_queue.chunk_jobs.url
}

output "output_events_queue_url" {
  value = aws_sqs_queue.output_events.url
}

output "organizer_function_arn" {
  value = aws_lambda_function.organizer.arn
}

output "worker_function_arn" {
  value = aws_lambda_function.worker.arn
}

output "lambda_role_arn" {
  value = aws_iam_role.lambda.arn
}
