#!/usr/bin/env bash
# Controlled local-only matrix. It is intentionally never called by go test,
# Godog, or the official E2E suite.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
results_dir="${BENCHMARK_LOCAL_MATRIX_RESULTS_DIR:-$root/resultados/benchmark-local-memory-matrix/$run_id}"

# memoryMiB:equivalent-vCPU. Lambda assigns CPU proportionally to memory; the
# values below approximate memory / 1769 MiB, where AWS documents one vCPU.
matrix="${BENCHMARK_LOCAL_MEMORY_CPU_MATRIX:-128:0.07 256:0.14 512:0.29 1024:0.58}"
smoke_records="${BENCHMARK_LOCAL_SMOKE_RECORDS:-10000}"
full_records="${BENCHMARK_LOCAL_FULL_RECORDS:-5000000}"
records_per_chunk="${BENCHMARK_LOCAL_RECORDS_PER_CHUNK:-50000}"
publish_concurrency="${BENCHMARK_LOCAL_PUBLISH_CONCURRENCY:-8}"
output_mode="${BENCHMARK_LOCAL_OUTPUT_MODE:-bundle}"
max_envelopes="${BENCHMARK_LOCAL_MAX_ENVELOPES_PER_MESSAGE:-0}"
max_message_bytes="${BENCHMARK_LOCAL_MAX_MESSAGE_BYTES:-256000}"
smoke_timeout="${BENCHMARK_LOCAL_SMOKE_TIMEOUT_SECONDS:-900}"
full_timeout="${BENCHMARK_LOCAL_FULL_TIMEOUT_SECONDS:-7200}"
worker_timeout="${BENCHMARK_LOCAL_WORKER_TIMEOUT_SECONDS:-600}"

for command in jq docker; do
  command -v "$command" >/dev/null || { echo "$command not found" >&2; exit 1; }
done
[[ "$smoke_records" =~ ^[1-9][0-9]*$ && "$full_records" =~ ^[1-9][0-9]*$ ]] || { echo "record counts must be positive integers" >&2; exit 2; }
[[ "$max_message_bytes" =~ ^[1-9][0-9]*$ && "$max_message_bytes" -le 256000 ]] || { echo "BENCHMARK_LOCAL_MAX_MESSAGE_BYTES must be between 1 and 256000" >&2; exit 2; }

mkdir -p "$results_dir"
summary="$results_dir/summary.json"
reports=()
failed=0

run_case() {
  local memory="$1" cpus="$2" label="$3" records="$4" timeout="$5"
  local case_dir="$results_dir/memory-${memory}mb-cpu-${cpus}/$label"
  local report="$case_dir/report.json"
  mkdir -p "$case_dir"

  echo "=== local matrix: memory=${memory}MiB cpu=${cpus} vCPU stage=${label} records=${records} ==="
  if env \
    BENCHMARK_RESULTS_DIR="$case_dir" \
    BENCHMARK_RECORDS="$records" \
    BENCHMARK_RECORDS_PER_CHUNK="$records_per_chunk" \
    BENCHMARK_PUBLISH_CONCURRENCY="$publish_concurrency" \
    BENCHMARK_OUTPUT_MODE="$output_mode" \
    BENCHMARK_MAX_ENVELOPES_PER_MESSAGE="$max_envelopes" \
    BENCHMARK_MAX_MESSAGE_BYTES="$max_message_bytes" \
    BENCHMARK_TIMEOUT_SECONDS="$timeout" \
    BENCHMARK_WORKER_MEMORY_MB="$memory" \
    BENCHMARK_WORKER_CPUS="$cpus" \
    BENCHMARK_WORKER_CONTAINER_MEMORY="${memory}m" \
    BENCHMARK_WORKER_TIMEOUT_SECONDS="$worker_timeout" \
    "$root/benchmark-local-5m.sh"; then
    :
  else
    failed=1
    echo "Stage ${label} failed for ${memory} MiB / ${cpus} vCPU; see ${case_dir}." >&2
  fi
  if [[ -f "$report" ]]; then
    reports+=("$report")
  fi
}

for profile in $matrix; do
  IFS=: read -r memory cpus extra <<<"$profile"
  [[ -n "$memory" && -n "$cpus" && -z "${extra:-}" && "$memory" =~ ^[1-9][0-9]*$ && "$cpus" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]] || {
    echo "Invalid matrix profile '$profile'; use memoryMiB:decimal-vCPU." >&2
    exit 2
  }
  run_case "$memory" "$cpus" smoke "$smoke_records" "$smoke_timeout"
  smoke_report="$results_dir/memory-${memory}mb-cpu-${cpus}/smoke/report.json"
  if [[ -f "$smoke_report" ]] && [[ "$(jq -r .status "$smoke_report")" == "COMPLETED" ]]; then
    run_case "$memory" "$cpus" full "$full_records" "$full_timeout"
  else
    failed=1
    echo "Skipping 5M stage for ${memory} MiB / ${cpus} vCPU because smoke did not complete." >&2
  fi
done

if [[ ${#reports[@]} -gt 0 ]]; then
  jq -s --arg runId "$run_id" --arg matrix "$matrix" \
    '{runId:$runId,kind:"local-memory-cpu-matrix",matrix:$matrix,cases:map({stage:(.input.records | if . == 10000 then "smoke" else "full" end),status,parameters,input,durationsMs,result,resources})}' \
    "${reports[@]}" > "$summary"
else
  jq -n --arg runId "$run_id" --arg matrix "$matrix" '{runId:$runId,kind:"local-memory-cpu-matrix",matrix:$matrix,cases:[]}' > "$summary"
fi

echo "Local matrix summary: $summary"
[[ "$failed" -eq 0 ]] || exit 1
