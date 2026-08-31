resource "aws_s3_bucket" "input" {
  bucket = var.f2e_input_bucket
  tags   = local.tags
}

resource "aws_s3_bucket_notification" "input" {
  bucket = aws_s3_bucket.input.id

  queue {
    id        = "s3-to-file-intake"
    queue_arn = aws_sqs_queue.file_intake.arn
    events    = ["s3:ObjectCreated:*"]
  }

  # Ensure the queue policy exists before S3 tries to verify it.
  depends_on = [aws_sqs_queue_policy.file_intake]
}
