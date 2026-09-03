#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
terraform_dir="$root/../terraform"
tfvars="${F2E_AWS_TFVARS:-$terraform_dir/environments/aws.local.tfvars}"

for command in aws jq terraform; do
  command -v "$command" >/dev/null || { echo "$command não encontrado no PATH." >&2; exit 1; }
done
[[ -f "$tfvars" ]] || { echo "Arquivo de variáveis AWS não encontrado: $tfvars" >&2; exit 1; }
aws sts get-caller-identity >/dev/null || {
  echo "Não foi possível validar as credenciais AWS ativas." >&2
  exit 1
}

terraform -chdir="$terraform_dir" init -input=false

purge_bucket_versions() {
  local bucket="$1" region="$2" listing deletion count

  echo "Removendo objetos, versões e marcadores de exclusão de s3://$bucket..."
  while :; do
    # delete-objects accepts at most 1,000 keys. Starting a new listing after
    # each batch also handles buckets created before force_destroy was enabled.
    listing="$(aws s3api list-object-versions --bucket "$bucket" --region "$region" --max-items 1000 --output json)"
    deletion="$(jq -c '{Objects: ((.Versions // []) + (.DeleteMarkers // []) | map({Key: .Key, VersionId: .VersionId})), Quiet: true}' <<<"$listing")"
    count="$(jq '.Objects | length' <<<"$deletion")"
    [[ "$count" -gt 0 ]] || break
    aws s3api delete-objects --bucket "$bucket" --region "$region" --delete "$deletion" >/dev/null
  done
}

# Terraform removes outputs during a partial destroy, while the bucket resource
# can remain in state. Read the exact resource address instead of inferring a
# bucket name from tfvars or the AWS account.
bucket="$(terraform -chdir="$terraform_dir" state show -no-color aws_s3_bucket.input 2>/dev/null | awk -F ' = ' '/^    bucket[[:space:]]*=/ {gsub(/"/, "", $2); print $2; exit}')"
region="$(terraform -chdir="$terraform_dir" console -var-file="$tfvars" <<< 'var.aws_region' | tr -d '"')"
if [[ -n "$bucket" && -n "$region" ]]; then
  tags="$(aws s3api get-bucket-tagging --bucket "$bucket" --region "$region" --output json)"
  project="$(jq -r '.TagSet[] | select(.Key == "Project") | .Value' <<<"$tags")"
  environment="$(jq -r '.TagSet[] | select(.Key == "Environment") | .Value' <<<"$tags")"
  [[ "$project" == "f2e" && "$environment" == "development" ]] || {
    echo "Recusado: s3://$bucket não possui as tags Project=f2e e Environment=development." >&2
    exit 1
  }
  purge_bucket_versions "$bucket" "$region"
fi

terraform -chdir="$terraform_dir" destroy -input=false -auto-approve -var-file="$tfvars"

echo 'Ambiente AWS destruído.'
