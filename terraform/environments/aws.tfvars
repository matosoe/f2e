# AWS — production/staging environment
#
# Usage (from the terraform/ directory):
#   terraform init
#   terraform apply -var-file=environments/aws.tfvars
#
# Prerequisites:
#   - AWS credentials configured (env vars, ~/.aws/credentials, or instance profile)
#   - Lambda zips built: automacao/subir-ambiente.sh (build-only step)
#   - f2e_input_bucket must be globally unique; change it if "f2e-input" is taken

localstack_endpoint     = ""
lambda_aws_endpoint_url = ""

environment    = "development"
aws_region     = "us-east-1"
lambda_zip_dir = "../.build"

lambda_runtime   = "provided.al2023"
lambda_timeout   = 300
lambda_memory_mb = 512

# Change to a globally unique bucket name for AWS deployments.
f2e_input_bucket      = "f2e-input"
f2e_record_length     = 100
f2e_records_per_chunk = 1000
f2e_batch_size        = 10

sqs_visibility_timeout = 1800
sqs_max_receive_count  = 3
organizer_batch_size   = 1
worker_batch_size      = 1
