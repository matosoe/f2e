locals {
  monitored_queues = {
    file-intake       = aws_sqs_queue.file_intake
    chunk-jobs        = aws_sqs_queue.chunk_jobs
    completion-events = aws_sqs_queue.completion_events
  }
  monitored_dlqs = {
    file-intake       = aws_sqs_queue.file_intake_dlq
    chunk-jobs        = aws_sqs_queue.chunk_jobs_dlq
    completion-events = aws_sqs_queue.completion_events_dlq
  }
  monitored_functions = {
    organizer            = aws_lambda_function.organizer
    worker               = aws_lambda_function.worker
    completion-publisher = aws_lambda_function.completion_publisher
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
  dimensions          = { QueueName = each.value.name }
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
  dimensions          = { QueueName = each.value.name }
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
  dimensions          = { FunctionName = each.value.function_name }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "lambda_throttles" {
  for_each            = local.monitored_functions
  alarm_name          = "${var.resource_prefix}-${var.environment}-${each.key}-throttles"
  namespace           = "AWS/Lambda"
  metric_name         = "Throttles"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = each.value.function_name }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "lambda_duration_near_timeout" {
  for_each            = local.monitored_functions
  alarm_name          = "${var.resource_prefix}-${var.environment}-${each.key}-duration-near-timeout"
  namespace           = "AWS/Lambda"
  metric_name         = "Duration"
  extended_statistic  = "p95"
  period              = 60
  evaluation_periods  = 2
  threshold           = var.lambda_timeout * 1000 * 0.8
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = each.value.function_name }
  tags                = local.tags
}

resource "aws_cloudwatch_metric_alarm" "worker_concurrency" {
  alarm_name          = "${var.resource_prefix}-${var.environment}-worker-concurrency"
  namespace           = "AWS/Lambda"
  metric_name         = "ConcurrentExecutions"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 2
  threshold           = var.worker_reserved_concurrency * 0.8
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions          = { FunctionName = aws_lambda_function.worker.function_name }
  tags                = local.tags
}

# ── T14: completion-publisher Streams iterator age alarm ─────────────────────
#
# A rising IteratorAge means the completion-publisher is falling behind:
# completion events are delayed, indicating stuck jobs or publisher outages.
# Threshold is 5 minutes (300 000 ms) — operators review before the 24-hour
# Streams retention boundary is approached.

resource "aws_cloudwatch_metric_alarm" "streams_iterator_age" {
  alarm_name          = "${var.resource_prefix}-${var.environment}-completion-publisher-iterator-age"
  namespace           = "AWS/Lambda"
  metric_name         = "IteratorAge"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 5
  threshold           = 300000 # 5 minutes in milliseconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  dimensions = {
    FunctionName = aws_lambda_function.completion_publisher.function_name
  }
  alarm_description = "Completion-publisher DynamoDB Streams iterator age exceeds 5 minutes. Review for stuck jobs or publisher outage. Runbook: documentacao/runbooks/streams-iterator-age.md"
  # alarm_actions and ok_actions are intentionally empty here. Configure
  # them via the var.alarm_sns_topic_arn variable (see variables.tf).
  alarm_actions = var.alarm_sns_topic_arn != "" ? [var.alarm_sns_topic_arn] : []
  ok_actions    = var.alarm_sns_topic_arn != "" ? [var.alarm_sns_topic_arn] : []
  tags          = local.tags
}

resource "aws_cloudwatch_dashboard" "operations" {
  dashboard_name = "${var.resource_prefix}-${var.environment}-operations"
  dashboard_body = jsonencode({ widgets = [
    { type = "metric", width = 12, height = 6, properties = { title = "Queue backlog", region = var.aws_region, metrics = [["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.file_intake.name], [".", ".", ".", aws_sqs_queue.chunk_jobs.name]] } },
    { type = "metric", width = 12, height = 6, properties = { title = "Lambda errors and throttles", region = var.aws_region, metrics = [["AWS/Lambda", "Errors", "FunctionName", aws_lambda_function.organizer.function_name], [".", ".", ".", aws_lambda_function.worker.function_name], [".", "Throttles", ".", aws_lambda_function.organizer.function_name], [".", ".", ".", aws_lambda_function.worker.function_name]] } },
    { type = "metric", width = 12, height = 6, properties = { title = "Duration and concurrency", region = var.aws_region, metrics = [["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.organizer.function_name], [".", ".", ".", aws_lambda_function.worker.function_name], [".", "ConcurrentExecutions", ".", aws_lambda_function.worker.function_name]] } }
  ] })
}
