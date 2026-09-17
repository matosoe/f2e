# LocalStack — Docker Compose local environment
#
# Usage (from the terraform/ directory, with LocalStack already running):
#   terraform init
#   terraform apply -var-file=environments/local.tfvars
#
# The lambda zips must exist before running Terraform.
# Build them first with:  automacao/subir-ambiente.sh  (or manually)

localstack_endpoint     = "http://localhost:4566"
lambda_aws_endpoint_url = "http://localstack:4566"

environment    = "local"
aws_region     = "us-east-1"
lambda_zip_dir = "../.build"

# Lambda runtime supported by LocalStack 3.x
lambda_runtime = "provided.al2"
lambda_timeout = 60
# Perfil de throughput local: até 12 Workers de 1 GiB cabem dentro dos 15 GiB
# disponíveis, deixando margem para o LocalStack e o Docker Desktop.
lambda_memory_mb = 1024

f2e_input_bucket           = "f2e-input"
f2e_record_length          = 100
f2e_records_per_chunk      = 1000
f2e_batch_size             = 10
f2e_target_chunk_bytes     = 0
worker_publish_concurrency = 8

sqs_visibility_timeout         = 1800
sqs_max_receive_count          = 3
organizer_batch_size           = 1
worker_batch_size              = 1
organizer_reserved_concurrency = 4
worker_maximum_concurrency     = 12
