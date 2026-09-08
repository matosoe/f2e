#!/usr/bin/env bash
# benchmark-p3-memory.sh -- Matriz controlada do item P3.10 (right-sizing).
# Cada ponto usa a mesma massa de capacidade, chunks e teto de 10 Workers.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
project_tools="$root/../.build/tools"
[[ -d "$project_tools" ]] && export PATH="$project_tools:$PATH"
memories="${P3_MEMORY_VARIANTS:-128 256 512 1024}"
repetitions="${P3_REPETITIONS:-3}"
timeout_margin_ms="${P3_TIMEOUT_MARGIN_MS:-48000}"
# Preço por GB-s usado somente como estimativa comparativa de computação.
# Atualize-o para a região/arquitetura e data da execução antes de usar o valor
# monetário em decisão financeira; requests, free tier e outros serviços ficam fora.
gb_second_usd="${P3_LAMBDA_GB_SECOND_USD:-0.0000166667}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
results_dir="${P3_RESULTS_DIR:-$root/resultados/benchmark-p3-memory/$run_id}"
report_file="${P3_REPORT_FILE:-$root/../documentacao/benchmark/p3-right-sizing-${run_id}.md}"
tfvars="${F2E_AWS_TFVARS:-$root/../terraform/environments/aws.local.tfvars}"
environment_started=0

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ "$environment_started" -eq 1 ]]; then
    F2E_AWS_TFVARS="$tfvars" "$root/parar-ambiente-aws.sh" || echo "AVISO: destroy final falhou; remova o ambiente manualmente." >&2
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

[[ "$repetitions" =~ ^[1-9][0-9]*$ && "$repetitions" -ge 3 ]] || { echo "P3_REPETITIONS deve ser >= 3." >&2; exit 2; }
[[ "$timeout_margin_ms" =~ ^[1-9][0-9]*$ && "$timeout_margin_ms" -le 60000 ]] || { echo "P3_TIMEOUT_MARGIN_MS deve estar entre 1 e 60000." >&2; exit 2; }
[[ "$gb_second_usd" =~ ^[0-9]+([.][0-9]+)?$ ]] || { echo "P3_LAMBDA_GB_SECOND_USD deve ser numérico." >&2; exit 2; }
for memory in $memories; do
  [[ "$memory" =~ ^(128|256|512|1024)$ ]] || { echo "Memória P3 inválida: $memory (use 128, 256, 512 ou 1024)." >&2; exit 2; }
done

for command in jq; do command -v "$command" >/dev/null || { echo "$command não encontrado no PATH." >&2; exit 1; }; done
mkdir -p "$results_dir" "$(dirname "$report_file")"

echo "=== P3.10: matriz de memória ($run_id) ==="
echo "Resultados brutos: $results_dir"
for memory in $memories; do
  for repetition in $(seq 1 "$repetitions"); do
    case_dir="$results_dir/memory-${memory}/run-${repetition}"
    echo "=== memória=${memory} MiB, repetição ${repetition}/${repetitions} ==="
    environment_started=1
    if ! env \
      BENCHMARK_WORKER_MEMORY_MB="$memory" \
      BENCHMARK_WORKER_CONCURRENCY=10 \
      BENCHMARK_PUBLISH_CONCURRENCY=1 \
      BENCHMARK_LAMBDA_TIMEOUT=60 \
      BENCHMARK_FULL_RECORDS_PER_CHUNK=50000 \
      BENCHMARK_FULL_RECORDS=500000 \
      BENCHMARK_FULL_LABEL=capacity \
      BENCHMARK_TIMEOUT_SECONDS=1800 \
      BENCHMARK_PREFLIGHT_TIMEOUT_MS=60000 \
      BENCHMARK_KEEP_ENVIRONMENT=1 \
      BENCHMARK_RESULTS_DIR="$case_dir" \
      "$root/benchmark-aws-5m.sh"; then
      echo "AVISO: memória=${memory} MiB, repetição=$repetition falhou; a matriz continuará para medir os demais pontos." >&2
      if [[ ! -f "$case_dir/capacity/report.json" ]]; then
        mkdir -p "$case_dir/capacity"
        jq -n --argjson memory "$memory" \
          '{status:"HARNESS_FAILED",parameters:{lambdaMemoryMB:$memory},durationsMs:{processing:0},result:{lambdaErrors:1,lambdaThrottles:0,intakeDlq:0,chunkDlq:0},resources:{cpu:{},memory:{},gc:{}}}' \
          > "$case_dir/capacity/report.json"
      fi
    fi
  done
done

mapfile -t reports < <(find "$results_dir" -path '*/capacity/report.json' -type f | sort)
expected_reports=$(( $(wc -w <<<"$memories") * repetitions ))
[[ "${#reports[@]}" -eq "$expected_reports" ]] || { echo "Esperados $expected_reports relatórios de capacidade; encontrados ${#reports[@]}." >&2; exit 1; }

jq -s --argjson unitPrice "$gb_second_usd" '
  def percentile($p): sort | .[((length - 1) * $p | floor)];
  def median: percentile(0.5);
  [ .[] |
    . as $r |
    ($r.resources.memory // {}) as $m |
    ($r.resources.gc // {}) as $gc |
    {
      memoryMB: $r.parameters.lambdaMemoryMB,
      status: $r.status,
      processingMs: $r.durationsMs.processing,
      workerP95Ms: ($m.p95DurationMs // 0),
      workerP99Ms: ($m.p99DurationMs // 0),
      peakMemoryMB: ($m.peakMaxMemoryUsedMB // 0),
      initP95Ms: ($m.p95InitDurationMs // 0),
      cpuAveragePercent: ($r.resources.cpu.averageCpuUtilizationPercent // 0),
      gcCycles: ($gc.totalCycles // 0),
      gcPauseMs: ($gc.totalPauseMs // 0),
      errors: ($r.result.lambdaErrors // 0),
      throttles: ($r.result.lambdaThrottles // 0),
      intakeDlq: ($r.result.intakeDlq // 0),
      chunkDlq: ($r.result.chunkDlq // 0),
      billedMs: ($m.totalBilledDurationMs // 0),
      gbSeconds: (($m.totalBilledDurationMs // 0) / 1000 * ($r.parameters.lambdaMemoryMB / 1024)),
      estimatedComputeCostUSD: (($m.totalBilledDurationMs // 0) / 1000 * ($r.parameters.lambdaMemoryMB / 1024) * $unitPrice)
    }
  ] as $runs |
  ($runs | group_by(.memoryMB) | map({
    memoryMB: .[0].memoryMB,
    runs: length,
    successfulRuns: map(select(.status == "COMPLETED" and .errors == 0 and .throttles == 0 and .intakeDlq == 0 and .chunkDlq == 0)) | length,
    medianProcessingMs: (map(.processingMs) | median),
    medianWorkerP95Ms: (map(.workerP95Ms) | median),
    worstWorkerP99Ms: (map(.workerP99Ms) | max),
    peakMemoryMB: (map(.peakMemoryMB) | max),
    p95InitDurationMs: (map(.initP95Ms) | percentile(0.95)),
    medianCpuAveragePercent: (map(.cpuAveragePercent) | median),
    totalGcCycles: (map(.gcCycles) | add),
    totalGcPauseMs: (map(.gcPauseMs) | add),
    medianGBSeconds: (map(.gbSeconds) | median),
    medianEstimatedComputeCostUSD: (map(.estimatedComputeCostUSD) | median),
    totalErrors: (map(.errors) | add),
    totalThrottles: (map(.throttles) | add),
    eligible: false
  })) as $candidates |
  {runs:$runs, candidates:$candidates}' "${reports[@]}" > "$results_dir/runs.json"

jq --argjson repetitions "$repetitions" --argjson timeoutMarginMs "$timeout_margin_ms" '
  . as $all |
  .candidates |= map(.eligible = (.successfulRuns == $repetitions and .worstWorkerP99Ms <= $timeoutMarginMs and .peakMemoryMB < (.memoryMB * 0.8))) |
  . + {selection: ([.candidates[] | select(.eligible)] | sort_by(.medianEstimatedComputeCostUSD, .medianWorkerP95Ms, .memoryMB) | .[0] // null), timeoutMarginMs:$timeoutMarginMs}
' "$results_dir/runs.json" > "$results_dir/summary.json"

{
  echo "# P3.10 — right-sizing de memória do Worker"
  echo
  echo "- Execução: \`$run_id\`"
  echo "- Massa: capacidade com 500.000 linhas de 100 bytes; aproximadamente 9–10 chunks de 5 MiB; saída single (uma mensagem por linha)."
  echo "- Controles: 10 Workers, concorrência interna 1, timeout 60 s, arquitetura definida no tfvars."
  echo "- Repetições por ponto: $repetitions; margem de timeout (p99): ${timeout_margin_ms} ms."
  echo "- Custo: somente computação Lambda, usando \`P3_LAMBDA_GB_SECOND_USD=$gb_second_usd\`; não inclui requests, free tier ou outros serviços."
  echo
  echo "| Memória | Execuções válidas | Mediana E2E | p95 Worker | Pior p99 Worker | Pico memória | p95 init | CPU média | GC (ciclos/pausa) | GB-s mediano | Custo computação mediano | Erros/throttles | Elegível |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|"
  jq -r '.candidates[] | "| \(.memoryMB) MiB | \(.successfulRuns)/\(.runs) | \((.medianProcessingMs / 1000 * 100 | round / 100)) s | \(.medianWorkerP95Ms | round) ms | \(.worstWorkerP99Ms | round) ms | \(.peakMemoryMB) MB | \(.p95InitDurationMs | round) ms | \((.medianCpuAveragePercent * 100 | round / 100))% | \(.totalGcCycles)/\((.totalGcPauseMs * 100 | round / 100)) ms | \((.medianGBSeconds * 1000 | round / 1000)) | $\((.medianEstimatedComputeCostUSD * 1000000 | round / 1000000)) | \(.totalErrors)/\(.totalThrottles) | \(if .eligible then \"sim\" else \"não\" end) |"' "$results_dir/summary.json"
  echo
  selection="$(jq -r '.selection | if . == null then empty else "\(.memoryMB) MiB" end' "$results_dir/summary.json")"
  if [[ -n "$selection" ]]; then
    echo "## Decisão"
    echo
    echo "Configuração recomendada: **$selection**. É a alternativa elegível de menor custo estimado; em empate, vence a menor p95 do Worker."
  else
    echo "## Decisão"
    echo
    echo "Nenhuma configuração foi promovida: ao menos uma condição de integridade, margem de memória (pico < 80%) ou p99 <= ${timeout_margin_ms} ms não foi atendida."
  fi
  echo
  echo "Artefatos brutos e resumo estruturado: \`$results_dir\`."
} > "$report_file"

echo "Relatório P3.10: $report_file"
