#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
terraform_dir="$root/../terraform"
tfvars="${F2E_AWS_TFVARS:-$terraform_dir/environments/aws.local.tfvars}"

[[ -f "$tfvars" ]] || { echo "Arquivo de variáveis AWS não encontrado: $tfvars" >&2; exit 1; }

cleanup() {
  local status=$?
  "$root/parar-ambiente-aws.sh" || {
    echo 'ATENÇÃO: não foi possível destruir o ambiente AWS automaticamente.' >&2
    status=1
  }
  exit "$status"
}
trap cleanup EXIT

"$root/subir-ambiente-aws.sh"

environment="$(terraform -chdir="$terraform_dir" output -raw environment)"
[[ "$environment" == "development" ]] || {
  echo "E2E AWS permitido somente em environment=development; ambiente atual: $environment." >&2
  exit 1
}

export F2E_E2E_TARGET=aws
export F2E_E2E_AWS_REGION="$(terraform -chdir="$terraform_dir" output -raw aws_region)"
export F2E_E2E_INPUT_BUCKET="$(terraform -chdir="$terraform_dir" output -raw input_bucket)"
export F2E_E2E_INTAKE_QUEUE_NAME="$(terraform -chdir="$terraform_dir" output -raw file_intake_queue_name)"
export F2E_E2E_OUTPUT_QUEUE_NAME="$(terraform -chdir="$terraform_dir" output -raw output_events_queue_name)"
export F2E_E2E_LEDGER_TABLE="$(terraform -chdir="$terraform_dir" output -raw ledger_table_name)"
export F2E_E2E_INTAKE_DLQ_NAME="$(terraform -chdir="$terraform_dir" output -raw file_intake_dlq_name)"
export F2E_E2E_CHUNK_DLQ_NAME="$(terraform -chdir="$terraform_dir" output -raw chunk_jobs_dlq_name)"

(cd "$root/../e2e" && go test -count=1 -v -timeout 30m ./...)
