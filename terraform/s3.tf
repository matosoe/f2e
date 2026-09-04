resource "aws_s3_bucket" "input" {
  bucket        = var.f2e_input_bucket
  force_destroy = var.s3_force_destroy
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "input" {
  bucket                  = aws_s3_bucket.input.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "input" {
  bucket = aws_s3_bucket.input.id
  rule { object_ownership = "BucketOwnerEnforced" }
}

resource "aws_s3_bucket_versioning" "input" {
  bucket = aws_s3_bucket.input.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "input" {
  bucket = aws_s3_bucket.input.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_arn != "" ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_arn != "" ? var.kms_key_arn : null
    }
    bucket_key_enabled = var.kms_key_arn != ""
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "input" {
  bucket = aws_s3_bucket.input.id

  rule {
    id     = "expire-input-objects"
    status = "Enabled"
    filter {}

    expiration {
      days = var.s3_object_retention_days
    }

    noncurrent_version_expiration {
      noncurrent_days = var.s3_object_retention_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
  }
}

data "aws_iam_policy_document" "input_tls" {
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.input.arn, "${aws_s3_bucket.input.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "input_tls" {
  bucket = aws_s3_bucket.input.id
  policy = data.aws_iam_policy_document.input_tls.json
}

resource "aws_s3_bucket_notification" "input" {
  bucket = aws_s3_bucket.input.id

  dynamic "queue" {
    for_each = local.file_configurations
    content {
      id            = "s3-to-file-intake-${queue.key}"
      queue_arn     = aws_sqs_queue.file_intake.arn
      events        = ["s3:ObjectCreated:*"]
      filter_prefix = queue.value.prefix
      filter_suffix = var.s3_notification_suffix != "" ? var.s3_notification_suffix : null
    }
  }

  # Ensure the queue policy exists before S3 tries to verify it.
  depends_on = [aws_sqs_queue_policy.file_intake]
}
