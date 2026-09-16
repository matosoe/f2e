locals {
  monitored_queues = {
    file-intake       = aws_sqs_queue.file_intake.name
    chunk-jobs        = aws_sqs_queue.chunk_jobs.name
    output-events     = aws_sqs_queue.output_events.name
    completion-events = aws_sqs_queue.completion_events.name
  }
  monitored_dlqs = {
    file-intake       = aws_sqs_queue.file_intake_dlq.name
    chunk-jobs        = aws_sqs_queue.chunk_jobs_dlq.name
    completion-events = aws_sqs_queue.completion_events_dlq.name
  }
  monitored_functions = {
    organizer = aws_lambda_function.organizer.function_name
    worker    = aws_lambda_function.worker.function_name
  }
}
resource "aws_cloudwatch_metric_alarm" "dlq" {
  for_each            = local.monitored_dlqs
  alarm_name          = "${var.resource_prefix}-${var.environment}-${each.key}-dlq"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { QueueName = each.value }
  tags                = local.tags
}
resource "aws_cloudwatch_metric_alarm" "backlog_age" {
  for_each            = local.monitored_queues
  alarm_name          = "${var.resource_prefix}-${var.environment}-${each.key}-backlog-age"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateAgeOfOldestMessage"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = var.backlog_age_alarm_seconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { QueueName = each.value }
  tags                = local.tags
}
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  for_each            = local.monitored_functions
  alarm_name          = "${var.resource_prefix}-${var.environment}-${each.key}-errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = each.value }
  tags                = local.tags
}
resource "aws_cloudwatch_dashboard" "operations" {
  dashboard_name = "${var.resource_prefix}-${var.environment}-operations"
  dashboard_body = jsonencode({ widgets = [
    { type = "metric", width = 12, height = 6, properties = { title = "Queue backlog", region = var.aws_region, metrics = [for _, name in local.monitored_queues : ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", name]] } },
    { type = "metric", width = 12, height = 6, properties = { title = "Lambda errors", region = var.aws_region, metrics = [for _, name in local.monitored_functions : ["AWS/Lambda", "Errors", "FunctionName", name]] } }
  ] })
}
