#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
terraform_dir="$root/../terraform"
tfvars="${F2E_AWS_TFVARS:-$terraform_dir/environments/aws.local.tfvars}"

for command in aws go terraform; do
  command -v "$command" >/dev/null || { echo "$command não encontrado no PATH." >&2; exit 1; }
done
[[ -f "$tfvars" ]] || {
  echo "Arquivo de variáveis AWS não encontrado: $tfvars" >&2
  echo "Crie-o a partir de terraform/environments/ALTERAR_aws.local.tfvars.example." >&2
  exit 1
}
aws sts get-caller-identity >/dev/null || {
  echo "Não foi possível validar as credenciais AWS ativas." >&2
  exit 1
}

"$root/build-lambdas.sh"
terraform -chdir="$terraform_dir" init -input=false
terraform -chdir="$terraform_dir" apply -input=false -auto-approve -var-file="$tfvars"

echo 'Ambiente AWS disponível. Ele foi mantido em execução para inspeção manual.'
bucket="$(terraform -chdir="$terraform_dir" output -raw input_bucket)"
echo 'Envie arquivos para um dos prefixos configurados:'
terraform -chdir="$terraform_dir" output -json s3_configured_prefixes | jq -r --arg bucket "$bucket" '.[] | "  s3://\($bucket)/\(.)"'
