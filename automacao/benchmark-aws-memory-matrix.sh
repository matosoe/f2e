#!/usr/bin/env bash
# AWS counterpart of benchmark-local-memory-matrix.sh.  It deliberately keeps
# the real Lambda CPU allocation coupled to the configured memory size.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
project_tools="$root/../.build/tools"
[[ -d "$project_tools" ]] && export PATH="$project_tools:$PATH"
run_id="${BENCHMARK_AWS_MATRIX_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
results_dir="${BENCHMARK_AWS_MATRIX_RESULTS_DIR:-$root/resultados/benchmark-aws-memory-matrix/$run_id}"
tfvars="${F2E_AWS_TFVARS:-$root/../terraform/environments/aws.local.tfvars}"

memories="${BENCHMARK_AWS_MEMORY_MATRIX:-128 256 512 1024}"
smoke_records="${BENCHMARK_AWS_SMOKE_RECORDS:-10000}"
full_records="${BENCHMARK_AWS_FULL_RECORDS:-5000000}"
records_per_chunk="${BENCHMARK_AWS_RECORDS_PER_CHUNK:-62500}"
record_length="${BENCHMARK_AWS_RECORD_LENGTH:-100}"
target_chunk_bytes="${BENCHMARK_AWS_TARGET_CHUNK_BYTES:-6250000}"
worker_concurrency="${BENCHMARK_AWS_WORKER_CONCURRENCY:-10}"
publish_concurrency="${BENCHMARK_AWS_PUBLISH_CONCURRENCY:-8}"
max_message_bytes="${BENCHMARK_AWS_MAX_MESSAGE_BYTES:-256000}"
worker_timeout="${BENCHMARK_AWS_WORKER_TIMEOUT_SECONDS:-600}"
case_timeout="${BENCHMARK_AWS_CASE_TIMEOUT_SECONDS:-21600}"
visibility_timeout="${BENCHMARK_AWS_SQS_VISIBILITY_TIMEOUT:-3600}"
ssm_endpoint_url="${BENCHMARK_AWS_SSM_ENDPOINT_URL:-}"

for command in jq; do
  command -v "$command" >/dev/null || { echo "$command não encontrado no PATH." >&2; exit 1; }
done
for value in "$smoke_records" "$full_records" "$records_per_chunk" "$record_length" "$worker_concurrency" "$publish_concurrency" "$max_message_bytes" "$worker_timeout" "$case_timeout" "$visibility_timeout"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "Os parâmetros numéricos devem ser inteiros positivos." >&2; exit 2; }
done
[[ "$target_chunk_bytes" =~ ^[1-9][0-9]*$ ]] || { echo "BENCHMARK_AWS_TARGET_CHUNK_BYTES deve ser um inteiro positivo." >&2; exit 2; }
[[ "$target_chunk_bytes" -ge 5242880 ]] || { echo "BENCHMARK_AWS_TARGET_CHUNK_BYTES deve ser pelo menos 5 MiB." >&2; exit 2; }
(( target_chunk_bytes % record_length == 0 )) || { echo "BENCHMARK_AWS_TARGET_CHUNK_BYTES deve ser múltiplo de BENCHMARK_AWS_RECORD_LENGTH." >&2; exit 2; }
[[ "$max_message_bytes" -ge 1024 && "$max_message_bytes" -le 256000 ]] || { echo "BENCHMARK_AWS_MAX_MESSAGE_BYTES deve ficar entre 1 KiB e 250 KiB." >&2; exit 2; }
[[ "$worker_timeout" -le 900 ]] || { echo "BENCHMARK_AWS_WORKER_TIMEOUT_SECONDS não pode exceder 900 s (limite da AWS Lambda)." >&2; exit 2; }
[[ -z "$ssm_endpoint_url" || "$ssm_endpoint_url" =~ ^https?:// ]] || { echo "BENCHMARK_AWS_SSM_ENDPOINT_URL deve ser uma URL HTTP(S)." >&2; exit 2; }
for memory in $memories; do
  [[ "$memory" =~ ^(128|256|512|1024)$ ]] || { echo "Memória AWS inválida: $memory (use 128, 256, 512 ou 1024)." >&2; exit 2; }
done

mkdir -p "$results_dir"
summary="$results_dir/summary.json"
reports=()
failed=0
environment_started=0

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ "$environment_started" -eq 1 ]]; then
    echo "Encerrando o ambiente AWS descartável..."
    F2E_AWS_TFVARS="$tfvars" TF_VAR_ssm_endpoint="$ssm_endpoint_url" "$root/parar-ambiente-aws.sh" || echo "AVISO: o destroy falhou; encerre o ambiente manualmente." >&2
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

append_reports() {
  local case_dir="$1" report
  for report in "$case_dir/10k/report.json" "$case_dir/5m/report.json"; do
    [[ -f "$report" ]] && reports+=("$report")
  done
  return 0
}

for memory in $memories; do
  case_dir="$results_dir/memory-${memory}mb"
  echo "=== AWS matrix: memory=${memory}MiB (CPU proporcional da Lambda) ==="
  environment_started=1
  if env \
    F2E_AWS_TFVARS="$tfvars" \
    BENCHMARK_RESULTS_DIR="$case_dir" \
    BENCHMARK_SMOKE_RECORDS_PER_CHUNK="$smoke_records" \
    BENCHMARK_FULL_RECORDS="$full_records" \
    BENCHMARK_FULL_RECORDS_PER_CHUNK="$records_per_chunk" \
    BENCHMARK_RECORD_LENGTH="$record_length" \
    BENCHMARK_TARGET_CHUNK_BYTES="$target_chunk_bytes" \
    BENCHMARK_FULL_LABEL=5m \
    BENCHMARK_WORKER_CONCURRENCY="$worker_concurrency" \
    BENCHMARK_WORKER_MEMORY_MB="$memory" \
    BENCHMARK_LAMBDA_TIMEOUT="$worker_timeout" \
    BENCHMARK_SQS_VISIBILITY_TIMEOUT="$visibility_timeout" \
    BENCHMARK_PUBLISH_CONCURRENCY="$publish_concurrency" \
    BENCHMARK_OUTPUT_MODE=bundle \
    BENCHMARK_MAX_ENVELOPES_PER_MESSAGE=0 \
    BENCHMARK_MAX_MESSAGE_BYTES="$max_message_bytes" \
    BENCHMARK_TIMEOUT_SECONDS="$case_timeout" \
    BENCHMARK_KEEP_ENVIRONMENT=1 \
    BENCHMARK_SSM_ENDPOINT_URL="$ssm_endpoint_url" \
    "$root/benchmark-aws-5m.sh"; then
    :
  else
    failed=1
    echo "Perfil AWS de ${memory} MiB falhou; os demais serão executados." >&2
  fi
  append_reports "$case_dir"
done

mapfile -t reports < <(find "$results_dir" -type f \( -path '*/10k/report.json' -o -path '*/5m/report.json' \) -print | sort)
if [[ ${#reports[@]} -gt 0 ]]; then
  jq -s --arg runId "$run_id" \
    '{runId:$runId,kind:"aws-memory-matrix",memoriesMB:(map(.parameters.lambdaMemoryMB) | unique | sort),cases:(map({stage:.label,status,parameters,input,durationsMs,result,resources}) | sort_by(.parameters.lambdaMemoryMB, (if .stage == "10k" then 0 else 1 end)))}' \
    "${reports[@]}" > "$summary"
else
  jq -n --arg runId "$run_id" --arg memories "$memories" \
    '{runId:$runId,kind:"aws-memory-matrix",memoriesMB:($memories|split(" ")|map(tonumber)),cases:[]}' > "$summary"
fi

echo "Resumo da matriz AWS: $summary"
[[ "$failed" -eq 0 ]] || exit 1
