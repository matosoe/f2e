#!/usr/bin/env bash
# benchmark-matrix.sh — Executa o E2E com a matriz de parâmetros definida no
# plano T19: memória (512/1024/2048 MiB), concorrência (4/8/16) e tamanho de
# chunk (1000/5000/10000). Varia UM fator por rodada; os outros ficam no baseline.
#
# Pré-requisitos:
#   - Ambiente AWS provisionado via subir-ambiente-aws.sh
#   - Permissões para atualizar variáveis Lambda (aws lambda update-function-configuration)
#   - E2E_METRICS_DIR apontando para um diretório de saída
#
# Uso:
#   F2E_AWS_TFVARS=terraform/environments/aws.local.tfvars bash automacao/benchmark-matrix.sh
#
# ATENÇÃO: este script realiza deploys parciais na AWS (somente atualização
# de configuração Lambda, sem terraform apply). Requer autorização antes da
# execução.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
terraform_dir="$root/../terraform"
metrics_dir="${E2E_METRICS_DIR:-$root/../.build/benchmark}"
mkdir -p "$metrics_dir"

# ── Baseline ──────────────────────────────────────────────────────────────────
BASELINE_MEMORY=1024
BASELINE_CONCURRENCY=4
BASELINE_RECORDS_PER_CHUNK=1000

# ── Matrizes ──────────────────────────────────────────────────────────────────
MEMORY_VARIANTS=(512 1024 2048)
CONCURRENCY_VARIANTS=(4 8 16)
CHUNK_VARIANTS=(1000 5000 10000)

# ── Helpers ───────────────────────────────────────────────────────────────────
worker_function_name() {
  terraform -chdir="$terraform_dir" output -raw worker_function_name 2>/dev/null
}

update_lambda_config() {
  local fn="$1" memory="$2"
  aws lambda update-function-configuration \
    --function-name "$fn" \
    --memory-size "$memory" \
    --no-cli-pager >/dev/null
  aws lambda wait function-updated --function-name "$fn"
}

update_ssm_chunk_size() {
  local param="${F2E_RECORDS_PER_CHUNK_PARAM:-/f2e/development/global-limits}" size="$1"
  current=$(aws ssm get-parameter --name "$param" --query "Parameter.Value" --output text 2>/dev/null || echo "{}")
  updated=$(echo "$current" | python3 -c "import sys,json; d=json.load(sys.stdin); d['recordsPerChunk']=$size; print(json.dumps(d))")
  aws ssm put-parameter --name "$param" --value "$updated" --type String --overwrite --no-cli-pager >/dev/null
}

run_e2e() {
  local label="$1" tags="${2:-@regression}"
  local outfile="$metrics_dir/benchmark-${label}.json"
  echo "── Rodando E2E: $label (tags: $tags) ──"
  export E2E_METRICS_FILE="$outfile"
  export E2E_TAGS="$tags"
  (cd "$root/../e2e" && go test -count=1 -v -timeout 30m ./...) || true
  echo "  → métricas: $outfile"
}

# ── Verificação de ambiente ────────────────────────────────────────────────────
fn=$(worker_function_name)
[[ -n "$fn" ]] || { echo "Não foi possível obter o nome da função Lambda. Terraform aplicado?" >&2; exit 1; }

export F2E_E2E_TARGET=aws
export F2E_E2E_AWS_REGION="$(terraform -chdir="$terraform_dir" output -raw aws_region)"
export F2E_E2E_INPUT_BUCKET="$(terraform -chdir="$terraform_dir" output -raw input_bucket)"
export F2E_E2E_INTAKE_QUEUE_NAME="$(terraform -chdir="$terraform_dir" output -raw file_intake_queue_name)"
export F2E_E2E_OUTPUT_QUEUE_NAME="$(terraform -chdir="$terraform_dir" output -raw output_events_queue_name)"
export F2E_E2E_LEDGER_TABLE="$(terraform -chdir="$terraform_dir" output -raw ledger_table_name)"
export F2E_E2E_INTAKE_DLQ_NAME="$(terraform -chdir="$terraform_dir" output -raw file_intake_dlq_name)"
export F2E_E2E_CHUNK_DLQ_NAME="$(terraform -chdir="$terraform_dir" output -raw chunk_jobs_dlq_name)"

echo "Função Worker: $fn"
echo "Saída de métricas: $metrics_dir"

# ── Baseline ──────────────────────────────────────────────────────────────────
echo "=== BASELINE: memory=${BASELINE_MEMORY}MiB concurrency=${BASELINE_CONCURRENCY} chunk=${BASELINE_RECORDS_PER_CHUNK} ==="
update_lambda_config "$fn" "$BASELINE_MEMORY"
update_ssm_chunk_size "$BASELINE_RECORDS_PER_CHUNK"
run_e2e "baseline"

# ── Variação de memória ────────────────────────────────────────────────────────
for mem in "${MEMORY_VARIANTS[@]}"; do
  [[ "$mem" -eq "$BASELINE_MEMORY" ]] && continue
  echo "=== MEMÓRIA: ${mem}MiB (baseline: concurrency=${BASELINE_CONCURRENCY} chunk=${BASELINE_RECORDS_PER_CHUNK}) ==="
  update_lambda_config "$fn" "$mem"
  run_e2e "memory-${mem}"
done
# Restaura baseline de memória
update_lambda_config "$fn" "$BASELINE_MEMORY"

# ── Variação de tamanho de chunk ───────────────────────────────────────────────
for chunk in "${CHUNK_VARIANTS[@]}"; do
  [[ "$chunk" -eq "$BASELINE_RECORDS_PER_CHUNK" ]] && continue
  echo "=== CHUNK: ${chunk} records (baseline: memory=${BASELINE_MEMORY}MiB concurrency=${BASELINE_CONCURRENCY}) ==="
  update_ssm_chunk_size "$chunk"
  run_e2e "chunk-${chunk}"
done
# Restaura baseline de chunk
update_ssm_chunk_size "$BASELINE_RECORDS_PER_CHUNK"

echo "=== Benchmark concluído. Relatórios em $metrics_dir ==="
