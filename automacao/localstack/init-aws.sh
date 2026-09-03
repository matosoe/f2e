#!/usr/bin/env bash
set -euo pipefail
export AWS_DEFAULT_REGION=us-east-1 AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
qurl() { awslocal sqs get-queue-url --queue-name "$1" --query QueueUrl --output text; }
mkq() { awslocal sqs create-queue --queue-name "$1" --attributes "{\"ReceiveMessageWaitTimeSeconds\":\"1\",\"VisibilityTimeout\":\"${2:-60}\"}" >/dev/null; }
mkq file-intake-dlq; mkq chunk-jobs-dlq
for q in file-intake chunk-jobs; do
  dlq="$(qurl "$q-dlq")"; arn="$(awslocal sqs get-queue-attributes --queue-url "$dlq" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"
  visibility=60
  [[ "$q" == chunk-jobs ]] && visibility=1800
  attrs="{\"ReceiveMessageWaitTimeSeconds\":\"1\",\"VisibilityTimeout\":\"$visibility\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$arn\\\",\\\"maxReceiveCount\\\":\\\"1\\\"}\"}"
  awslocal sqs create-queue --queue-name "$q" --attributes "$attrs" >/dev/null
done
mkq output-events 1800
awslocal s3 mb s3://f2e-input 2>/dev/null || true
awslocal dynamodb create-table \
  --table-name f2e-job-ledger \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST >/dev/null
awslocal dynamodb wait table-exists --table-name f2e-job-ledger
# E2E sends the explicit organizer contract to file-intake after uploading.
# Do not also wire S3 notifications here: that would plan every object twice.
role='arn:aws:iam::000000000000:role/f2e-lambda-role'
for name in organizer worker; do
  zip="/opt/f2e/$name.zip"; env="Variables={AWS_ENDPOINT_URL=http://localstack:4566,AWS_REGION=us-east-1,F2E_ENVIRONMENT=local,F2E_RECORD_LENGTH=100,F2E_RECORDS_PER_CHUNK=1000,F2E_BATCH_SIZE=10,F2E_MAX_RECEIVE_COUNT=1,F2E_LEDGER_TABLE=f2e-job-ledger,F2E_LEDGER_RETENTION_DAYS=90,F2E_CHUNK_QUEUE_URL=$(qurl chunk-jobs),F2E_OUTPUT_QUEUE_URL=$(qurl output-events)}"
  awslocal lambda get-function --function-name "f2e-$name" >/dev/null 2>&1 && awslocal lambda update-function-code --function-name "f2e-$name" --zip-file "fileb://$zip" >/dev/null || awslocal lambda create-function --function-name "f2e-$name" --runtime provided.al2 --handler bootstrap --role "$role" --zip-file "fileb://$zip" --timeout 300 --environment "$env" >/dev/null
done
for name in organizer worker; do
  awslocal lambda wait function-active-v2 --function-name "f2e-$name"
done
# One chunk per worker invocation bounds execution time and isolates retries.
# Throughput is controlled by the event-source maximum concurrency.
for pair in "file-intake organizer 1" "chunk-jobs worker 1"; do
  set -- $pair; arn="$(awslocal sqs get-queue-attributes --queue-url "$(qurl "$1")" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)"; awslocal lambda list-event-source-mappings --function-name "f2e-$2" --event-source-arn "$arn" --query 'EventSourceMappings[0].UUID' --output text | grep -qv None || awslocal lambda create-event-source-mapping --function-name "f2e-$2" --event-source-arn "$arn" --batch-size "$3" --function-response-types ReportBatchItemFailures >/dev/null
done
echo 'F2E LocalStack provisioned.'
