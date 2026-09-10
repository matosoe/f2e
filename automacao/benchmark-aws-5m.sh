#!/usr/bin/env bash
# Benchmark E2E deliberadamente separado da suíte funcional.
# Executa 10 mil registros e, após validá-los, 5 milhões de registros na AWS.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
project_tools="$root/../.build/tools"
[[ -d "$project_tools" ]] && export PATH="$project_tools:$PATH"
terraform_dir="$root/../terraform"
tfvars="${F2E_AWS_TFVARS:-$terraform_dir/environments/aws.local.tfvars}"
record_length="${BENCHMARK_RECORD_LENGTH:-100}"
smoke_records_per_chunk="${BENCHMARK_SMOKE_RECORDS_PER_CHUNK:-1000}"
full_records_per_chunk="${BENCHMARK_FULL_RECORDS_PER_CHUNK:-50000}"
full_records="${BENCHMARK_FULL_RECORDS:-5000000}"
full_label="${BENCHMARK_FULL_LABEL:-5m}"
target_chunk_bytes="${BENCHMARK_TARGET_CHUNK_BYTES:-5242880}"
worker_concurrency="${BENCHMARK_WORKER_CONCURRENCY:-10}"
worker_memory_mb="${BENCHMARK_WORKER_MEMORY_MB:-1024}"
lambda_timeout="${BENCHMARK_LAMBDA_TIMEOUT:-60}"
sqs_visibility_timeout="${BENCHMARK_SQS_VISIBILITY_TIMEOUT:-1800}"
publish_concurrency="${BENCHMARK_PUBLISH_CONCURRENCY:-1}"
output_mode="${BENCHMARK_OUTPUT_MODE:-single}"
max_envelopes="${BENCHMARK_MAX_ENVELOPES_PER_MESSAGE:-1}"
max_message_bytes="${BENCHMARK_MAX_MESSAGE_BYTES:-256000}"
preflight_timeout_ms="${BENCHMARK_PREFLIGHT_TIMEOUT_MS:-0}"
timeout_seconds="${BENCHMARK_TIMEOUT_SECONDS:-14400}"
poll_seconds="${BENCHMARK_POLL_SECONDS:-5}"
keep_environment="${BENCHMARK_KEEP_ENVIRONMENT:-0}"
ssm_endpoint_url="${BENCHMARK_SSM_ENDPOINT_URL:-}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
result_dir="${BENCHMARK_RESULTS_DIR:-$root/resultados/benchmark-aws-5m/$run_id}"
prefix_id="example-text"
prefix="${prefix_id}/"

for value in "$record_length" "$smoke_records_per_chunk" "$full_records_per_chunk" "$full_records" "$target_chunk_bytes" "$worker_concurrency" "$worker_memory_mb" "$lambda_timeout" "$sqs_visibility_timeout" "$publish_concurrency" "$max_message_bytes" "$timeout_seconds" "$poll_seconds"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "Os parâmetros numéricos devem ser inteiros positivos." >&2; exit 2; }
done
[[ "$preflight_timeout_ms" =~ ^[0-9]+$ ]] || { echo "BENCHMARK_PREFLIGHT_TIMEOUT_MS deve ser zero ou inteiro positivo." >&2; exit 2; }
[[ "$max_envelopes" =~ ^[0-9]+$ ]] || { echo "BENCHMARK_MAX_ENVELOPES_PER_MESSAGE deve ser zero ou inteiro positivo." >&2; exit 2; }
[[ "$output_mode" == "single" || "$output_mode" == "bundle" ]] || { echo "BENCHMARK_OUTPUT_MODE deve ser single ou bundle." >&2; exit 2; }
[[ "$max_message_bytes" -ge 1024 && "$max_message_bytes" -le 256000 ]] || { echo "BENCHMARK_MAX_MESSAGE_BYTES deve ficar entre 1 KiB e 250 KiB." >&2; exit 2; }
if [[ "$output_mode" == "single" ]]; then
  max_envelopes=1
fi
[[ "$worker_concurrency" -ge 2 ]] || { echo "BENCHMARK_WORKER_CONCURRENCY deve ser >= 2." >&2; exit 2; }
[[ "$worker_concurrency" -le 16 ]] || { echo "BENCHMARK_WORKER_CONCURRENCY deve ser <= 16 para a matriz P1." >&2; exit 2; }
[[ "$publish_concurrency" -le 16 ]] || { echo "BENCHMARK_PUBLISH_CONCURRENCY deve ser <= 16." >&2; exit 2; }
[[ "$target_chunk_bytes" -ge 5242880 && "$target_chunk_bytes" -le 104857600 ]] || { echo "BENCHMARK_TARGET_CHUNK_BYTES deve ficar entre 5 e 100 MiB." >&2; exit 2; }
[[ "$keep_environment" == 0 || "$keep_environment" == 1 ]] || { echo "BENCHMARK_KEEP_ENVIRONMENT deve ser 0 ou 1." >&2; exit 2; }
[[ -z "$ssm_endpoint_url" || "$ssm_endpoint_url" =~ ^https?:// ]] || { echo "BENCHMARK_SSM_ENDPOINT_URL deve ser uma URL HTTP(S)." >&2; exit 2; }
[[ "$full_label" =~ ^[a-z0-9-]+$ ]] || { echo "BENCHMARK_FULL_LABEL deve conter apenas letras minúsculas, números e hífen." >&2; exit 2; }
for command in aws go jq terraform; do
  command -v "$command" >/dev/null || { echo "$command não encontrado no PATH." >&2; exit 1; }
done
[[ -f "$tfvars" ]] || { echo "Arquivo de variáveis AWS não encontrado: $tfvars" >&2; exit 1; }

mkdir -p "$result_dir"
log="$result_dir/run.log"
exec > >(tee -a "$log") 2>&1

now_ms() { date +%s%3N; }
elapsed_ms() { echo $(( $(now_ms) - $1 )); }
tf_output() { terraform -chdir="$terraform_dir" output -raw "$1" | tr -d '\r'; }
aws_region() { tf_output aws_region; }
aws_cli() { aws --region "$region" --no-cli-pager "$@"; }
# Prevent Git Bash from rewriting AWS resource names beginning with / as
# Windows paths. Do not use this wrapper for local upload paths.
aws_cli_no_pathconv() { MSYS_NO_PATHCONV=1 aws --region "$region" --no-cli-pager "$@"; }
aws_ssm_cli() {
  local endpoint_args=()
  [[ -z "$ssm_endpoint_url" ]] || endpoint_args=(--endpoint-url "$ssm_endpoint_url")
  MSYS_NO_PATHCONV=1 aws --region "$region" --no-cli-pager "${endpoint_args[@]}" ssm "$@"
}

environment_started=0
cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM
  if [[ "$environment_started" -eq 1 && "$keep_environment" -eq 0 ]]; then
    echo "Encerrando o ambiente AWS descartável..."
    F2E_AWS_TFVARS="$tfvars" "$root/parar-ambiente-aws.sh" || echo "AVISO: o destroy falhou; encerre o ambiente manualmente." >&2
  elif [[ "$environment_started" -eq 1 ]]; then
    echo "BENCHMARK_KEEP_ENVIRONMENT=1: ambiente mantido para inspeção."
  fi
  exit "$exit_code"
}
trap cleanup EXIT INT TERM

account="$(aws sts get-caller-identity --query Account --output text)"
expected_account="$(sed -nE 's/^[[:space:]]*aws_account_id[[:space:]]*=[[:space:]]*"([0-9]{12})".*/\1/p' "$tfvars" | tail -1)"
environment="$(sed -nE 's/^[[:space:]]*environment[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$tfvars" | tail -1)"
[[ -n "$expected_account" && "$account" == "$expected_account" ]] || { echo "Conta AWS ativa ($account) difere de aws_account_id ($expected_account)." >&2; exit 1; }
[[ "$environment" == "development" ]] || { echo "Benchmark recusado fora de environment=development." >&2; exit 1; }

# P1: check the regional Lambda quota before provisioning. MaximumConcurrency
# caps the SQS poller; it is not a reservation and must fit the unreserved pool.
regional_quota="$(aws service-quotas get-service-quota --service-code lambda --quota-code L-B99A9384 --query Quota.Value --output text 2>/dev/null || true)"
[[ "$regional_quota" =~ ^[0-9]+(\.[0-9]+)?$ ]] || { echo "Não foi possível consultar a cota regional de concorrência Lambda (L-B99A9384)." >&2; exit 1; }
quota_integer="${regional_quota%%.*}"
[[ "$quota_integer" -ge "$worker_concurrency" ]] || { echo "Concorrência solicitada ($worker_concurrency) excede a cota regional Lambda ($regional_quota)." >&2; exit 1; }
echo "Cota regional Lambda: $regional_quota; teto solicitado do mapping: $worker_concurrency; reserva do Worker: nenhuma."

echo "=== F2E AWS benchmark, saída $output_mode: $run_id ==="
echo "Conta: $account; resultados: $result_dir"

# Only the text-prefix worker receives chunks in this benchmark. The account
# used by this project has a regional concurrency quota of 10 and AWS requires
# all 10 to remain unreserved, so the exact cap is applied at the SQS poller.
# A second var-file is deliberately passed after aws.local.tfvars so benchmark
# safety values cannot be silently shadowed by Terraform precedence.
override_tfvars="$result_dir/terraform-benchmark.tfvars.json"
jq -n \
  --arg id "$prefix_id" --argjson concurrency "$worker_concurrency" --argjson memory "$worker_memory_mb" \
  --argjson timeout "$lambda_timeout" --argjson visibility "$sqs_visibility_timeout" --arg ssmEndpoint "$ssm_endpoint_url" \
  '{lambda_timeout:$timeout,sqs_visibility_timeout:$visibility,prefix_worker_config:{($id):{reserved_concurrency:-1,maximum_concurrency:$concurrency,memory_mb:$memory}}}
   + (if $ssmEndpoint == "" then {} else {ssm_endpoint:$ssmEndpoint} end)' \
  > "$override_tfvars"

provision_start="$(now_ms)"
environment_started=1
F2E_AWS_TFVARS="$tfvars" F2E_AWS_OVERRIDE_TFVARS="$override_tfvars" "$root/subir-ambiente-aws.sh"
provision_ms="$(elapsed_ms "$provision_start")"

region="$(aws_region)"
bucket="$(tf_output input_bucket)"
ledger_table="$(tf_output ledger_table_name)"
chunk_queue_url="$(terraform -chdir="$terraform_dir" output -json prefix_chunk_queue_urls | jq -r --arg id "$prefix_id" '.[$id]' | tr -d '\r')"
intake_dlq_name="$(tf_output file_intake_dlq_name)"
chunk_dlq_name="${chunk_queue_url##*/}-dlq"
intake_dlq_url="$(aws_cli sqs get-queue-url --queue-name "$intake_dlq_name" --query QueueUrl --output text)"
chunk_dlq_url="$(aws_cli sqs get-queue-url --queue-name "$chunk_dlq_name" --query QueueUrl --output text)"
chunk_queue_arn="$(aws_cli sqs get-queue-attributes --queue-url "$chunk_queue_url" --attribute-names QueueArn --query Attributes.QueueArn --output text)"
worker_arn="$(terraform -chdir="$terraform_dir" output -json prefix_worker_function_arns | jq -r --arg id "$prefix_id" '.[$id]' | tr -d '\r')"
worker_name="${worker_arn##*:function:}"
output_queue_url="$(terraform -chdir="$terraform_dir" output -json prefix_output_queue_urls | jq -r --arg id "$prefix_id" '.[$id]' | tr -d '\r')"
output_queue_name="${output_queue_url##*/}"
log_group="/aws/lambda/$(sed -nE 's/^[[:space:]]*resource_prefix[[:space:]]*=[[:space:]]*"([^"]+)".*/\1/p' "$tfvars" | tail -1)-${environment}-worker"
ssm_path="$(terraform -chdir="$terraform_dir" output -raw ssm_file_config_path)/${bucket}/${prefix_id}"

# The target Worker is the sole consumer of its exclusive prefix chunk queue.
# Any additional mapping is an infrastructure error, not something the
# benchmark silently disables.
mappings="$(aws_cli lambda list-event-source-mappings --event-source-arn "$chunk_queue_arn" --output json)"
target_mapping=""
wait_mapping_state() {
  local uuid="$1" wanted="$2" deadline=$(( $(date +%s) + 180 )) state
  while (( $(date +%s) < deadline )); do
    state="$(aws_cli lambda get-event-source-mapping --uuid "$uuid" --query State --output text)"
    [[ "$state" == "$wanted" ]] && return 0
    [[ "$state" == "Failed" ]] && { echo "Mapping $uuid entrou em estado Failed." >&2; return 1; }
    sleep 2
  done
  echo "Timeout aguardando mapping $uuid chegar a $wanted (atual: $state)." >&2
  return 1
}
while IFS=$'\t' read -r uuid function_arn; do
  uuid="${uuid//$'\r'/}"
  function_arn="${function_arn//$'\r'/}"
  [[ -n "$uuid" ]] || continue
  if [[ "$function_arn" == *":function:${worker_name}" || "$function_arn" == *":function:${worker_name}:"* ]]; then
    target_mapping="$uuid"
    aws_cli lambda update-event-source-mapping --uuid "$uuid" --enabled --scaling-config "MaximumConcurrency=$worker_concurrency" >/dev/null
    wait_mapping_state "$uuid" Enabled
  else
    echo "Fila exclusiva $chunk_queue_url possui consumidor inesperado: $function_arn" >&2
    exit 1
  fi
done < <(jq -r '.EventSourceMappings[] | [.UUID,.FunctionArn] | @tsv' <<<"$mappings")
[[ -n "$target_mapping" ]] || { echo "Event-source mapping do Worker $worker_name não encontrado." >&2; exit 1; }

current_config="$(aws_ssm_cli get-parameter --name "$ssm_path" --query Parameter.Value --output text)"

# Keep SQS publication sequential inside each invocation. The requested
# parallelism is supplied exclusively by the ten Lambda Worker environments.
worker_env="$(aws_cli lambda get-function-configuration --function-name "$worker_name" --query Environment.Variables --output json)"
worker_env="$(jq -c --arg concurrency "$publish_concurrency" '. + {F2E_PUBLISH_CONCURRENCY:$concurrency}' <<<"$worker_env")"
aws_cli lambda update-function-configuration --function-name "$worker_name" --environment "{\"Variables\":$worker_env}" >/dev/null
aws_cli lambda wait function-updated-v2 --function-name "$worker_name"
aws_cli lambda get-function-configuration --function-name "$worker_name" > "$result_dir/worker-configuration.json"
aws_cli lambda list-event-source-mappings --event-source-arn "$chunk_queue_arn" > "$result_dir/event-source-mappings.json"

queue_count() {
  aws_cli sqs get-queue-attributes --queue-url "$1" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --output json |
    jq '[.Attributes.ApproximateNumberOfMessages // "0", .Attributes.ApproximateNumberOfMessagesNotVisible // "0"] | map(tonumber) | add'
}

wait_queue_count() {
  local queue_url="$1" expected="$2" deadline=$(( $(date +%s) + 180 )) actual
  while (( $(date +%s) < deadline )); do
    actual="$(queue_count "$queue_url")"
    [[ "$actual" -eq "$expected" ]] && { echo "$actual"; return 0; }
    sleep 5
  done
  echo "$actual"
}

sample_output_bundles() {
  local case_dir="$1"
  if [[ "$output_mode" != "bundle" ]]; then
    jq -n '{samples:0}' > "$case_dir/bundle-sample-summary.json"
    return
  fi
  aws_cli sqs receive-message --queue-url "$output_queue_url" --max-number-of-messages 10 \
    --visibility-timeout 0 --wait-time-seconds 1 --message-attribute-names All --output json \
    > "$case_dir/bundle-sample.json"
  jq '[.Messages[]? | {logicalEvents:(.Body|fromjson|.items|length),bodyBytes:(.Body|utf8bytelength)}] as $samples |
      {samples:($samples|length),minLogicalEvents:([$samples[].logicalEvents]|min//0),maxLogicalEvents:([$samples[].logicalEvents]|max//0),averageLogicalEvents:(if ($samples|length)>0 then ([$samples[].logicalEvents]|add/length) else 0 end),minBodyBytes:([$samples[].bodyBytes]|min//0),maxBodyBytes:([$samples[].bodyBytes]|max//0),averageBodyBytes:(if ($samples|length)>0 then ([$samples[].bodyBytes]|add/length) else 0 end)}' \
    "$case_dir/bundle-sample.json" > "$case_dir/bundle-sample-summary.json"
}

purge_queue() {
  local queue_url="$1" deadline=$(( $(date +%s) + 75 ))
  while ! aws_cli sqs purge-queue --queue-url "$queue_url" >/dev/null 2>&1; do
    (( $(date +%s) < deadline )) || { echo "Não foi possível limpar $queue_url." >&2; return 1; }
    sleep 5
  done
  while [[ "$(queue_count "$queue_url")" -ne 0 ]]; do
    (( $(date +%s) < deadline )) || { echo "Fila não esvaziou: $queue_url." >&2; return 1; }
    sleep 2
  done
}

find_job_id() {
  local object_key="$1" values
  values="$(jq -cn --arg key "$object_key" '{":key":{S:$key},":job":{S:"JOB"}}')"
  aws_cli dynamodb scan --table-name "$ledger_table" --consistent-read \
    --filter-expression '#objectKey = :key AND #sortKey = :job' \
    --expression-attribute-names '{"#objectKey":"key","#sortKey":"sk"}' \
    --expression-attribute-values "$values" --projection-expression 'jobId' \
    --query 'Items[0].jobId.S' --output text
}

get_job() {
  local job_id="$1" key
  key="$(jq -cn --arg pk "JOB#$job_id" '{pk:{S:$pk},sk:{S:"JOB"}}')"
  aws_cli dynamodb get-item --table-name "$ledger_table" --consistent-read --key "$key" --query Item --output json
}

collect_metrics() {
  local case_dir="$1" start_ms="$2" end_ms="$3" start_iso="$4" end_iso="$5"
  sleep 15 # CloudWatch Logs ingestion is asynchronous.
  aws_cli_no_pathconv logs filter-log-events --log-group-name "$log_group" --start-time "$start_ms" --end-time "$((end_ms + 60000))" > "$case_dir/cloudwatch-logs.json"

  jq '[.events[].message | fromjson? | select(.msg == "worker invocation resources" and .cpuAvailable == true)] as $cpu |
      [$cpu[].cpuUtilizationPercent] as $pct |
      {samples:($cpu|length),cpuTotalMs:([$cpu[].cpuTotalMs]|add//0),cpuUserMs:([$cpu[].cpuUserMs]|add//0),cpuSystemMs:([$cpu[].cpuSystemMs]|add//0),averageCpuUtilizationPercent:(if ($pct|length)>0 then ($pct|add/length) else 0 end),peakCpuUtilizationPercent:($pct|max//0),p95CpuUtilizationPercent:(if ($pct|length)>0 then ($pct|sort|.[(((length-1)*0.95)|floor)]) else 0 end)}' \
      "$case_dir/cloudwatch-logs.json" > "$case_dir/cpu-summary.json"

  jq '[.events[].message | fromjson? | select(.type == "platform.report") | .record.metrics] as $reports |
      [$reports[].maxMemoryUsedMB] as $memory |
      [$reports[].durationMs] as $duration |
      [$reports[] | (.initDurationMs // 0)] as $init |
      {samples:($reports|length),configuredMemoryMB:($reports[0].memorySizeMB//0),averageMaxMemoryUsedMB:(if ($memory|length)>0 then ($memory|add/length) else 0 end),peakMaxMemoryUsedMB:($memory|max//0),p95MaxMemoryUsedMB:(if ($memory|length)>0 then ($memory|sort|.[(((length-1)*0.95)|floor)]) else 0 end),totalBilledDurationMs:([$reports[].billedDurationMs]|add//0),averageDurationMs:(if ($duration|length)>0 then ($duration|add/length) else 0 end),p95DurationMs:(if ($duration|length)>0 then ($duration|sort|.[(((length-1)*0.95)|floor)]) else 0 end),p99DurationMs:(if ($duration|length)>0 then ($duration|sort|.[(((length-1)*0.99)|floor)]) else 0 end),peakDurationMs:($duration|max//0),initSamples:([$init[] | select(. > 0)]|length),averageInitDurationMs:(if ([$init[] | select(. > 0)]|length)>0 then ([$init[] | select(. > 0)]|add / length) else 0 end),p95InitDurationMs:(if ([$init[] | select(. > 0)]|length)>0 then ([$init[] | select(. > 0)]|sort|.[(((length-1)*0.95)|floor)]) else 0 end),p99InitDurationMs:(if ([$init[] | select(. > 0)]|length)>0 then ([$init[] | select(. > 0)]|sort|.[(((length-1)*0.99)|floor)]) else 0 end)}' \
      "$case_dir/cloudwatch-logs.json" > "$case_dir/memory-summary.json"

  jq '[.events[].message | fromjson? | select(.msg == "worker invocation gc")] as $gc |
      [$gc[].gcCycles] as $cycles |
      [$gc[].gcPauseTotalMs] as $pause |
      [$gc[].heapAllocBytes] as $heap |
      {samples:($gc|length),totalCycles:($cycles|add//0),totalPauseMs:($pause|add//0),peakHeapAllocBytes:($heap|max//0),p95HeapAllocBytes:(if ($heap|length)>0 then ($heap|sort|.[(((length-1)*0.95)|floor)]) else 0 end)}' \
      "$case_dir/cloudwatch-logs.json" > "$case_dir/gc-summary.json"

  for spec in 'Invocations Sum' 'ConcurrentExecutions Maximum' 'Duration Average Maximum' 'Errors Sum' 'Throttles Sum'; do
    read -r -a parts <<<"$spec"
    metric="${parts[0]}"
    aws_cli cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name "$metric" \
      --dimensions "Name=FunctionName,Value=$worker_name" --start-time "$start_iso" --end-time "$end_iso" \
      --period 60 --statistics "${parts[@]:1}" > "$case_dir/cloudwatch-${metric}.json"
  done
}

run_case() {
  local records="$1" label="$2" case_records_per_chunk="$3"
  local case_dir="$result_dir/$label"
  local data_file="${BENCHMARK_DATA_FILE:-$root/dados/benchmark-${records}x${record_length}.txt}"
  local object_key="${prefix}${run_id}-${label}-${records}x${record_length}.txt"
  mkdir -p "$case_dir"

  echo "=== Caso $label: $records linhas ==="
  benchmark_config="$(jq -c \
    --arg outputMode "$output_mode" --argjson maxEnvelopes "$max_envelopes" --argjson maxMessageBytes "$max_message_bytes" \
    --argjson chunk "$case_records_per_chunk" --argjson targetChunkBytes "$target_chunk_bytes" --argjson recordLength "$record_length" \
    '. + {recordsPerChunk:$chunk,targetChunkBytes:$targetChunkBytes,maxRecordLengthBytes:$recordLength,outputMode:$outputMode,maxEnvelopesPerMessage:$maxEnvelopes,maxMessageBytes:$maxMessageBytes}' <<<"$current_config")"
  aws_ssm_cli put-parameter --name "$ssm_path" --type String --value "$benchmark_config" --overwrite >/dev/null
  generation_start="$(now_ms)"
  if [[ -f "$data_file" && -f "$data_file.manifest.json" ]] &&
     jq -e --argjson records "$records" --argjson length "$record_length" \
       '.records == $records and .recordLengthBytes == $length and .sizeBytes == ($records * $length)' \
       "$data_file.manifest.json" >/dev/null; then
    echo "Reutilizando massa válida: $data_file"
  else
    "$root/gerar-arquivo.sh" --output "$data_file" --records "$records" --record-length "$record_length" --force
  fi
  generation_ms="$(elapsed_ms "$generation_start")"
  manifest="$(<"$data_file.manifest.json")"
  size_bytes="$(jq -r .sizeBytes <<<"$manifest")"

  # Each case owns its queue state. This also removes DLQ artifacts left by a
  # prior failed/debug run, so they cannot contaminate the current verdict.
  purge_queue "$output_queue_url"
  purge_queue "$intake_dlq_url"
  purge_queue "$chunk_dlq_url"
  case_start_ms="$(now_ms)"
  case_start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  upload_start="$(now_ms)"
  aws_cli s3 cp "$data_file" "s3://$bucket/$object_key" --no-progress --checksum-algorithm CRC32
  upload_ms="$(elapsed_ms "$upload_start")"

  deadline=$(( $(date +%s) + timeout_seconds ))
  job_id=""
  while [[ -z "$job_id" || "$job_id" == "None" ]]; do
    (( $(date +%s) < deadline )) || { echo "Timeout aguardando criação do job." >&2; return 1; }
    job_id="$(find_job_id "$object_key")"
    [[ "$job_id" != "None" ]] || sleep "$poll_seconds"
  done
  echo "Job: $job_id"

  job='{}'
  last_progress=0
  while (( $(date +%s) < deadline )); do
    job="$(get_job "$job_id")"
    status="$(jq -r '.status.S // "WAITING"' <<<"$job")"
    completed="$(jq -r '.completedChunks.N // "0"' <<<"$job")"
    expected="$(jq -r '.expectedChunks.N // "0"' <<<"$job")"
    published="$(jq -r '.recordsPublished.N // .recordsProduced.N // "0"' <<<"$job")"
    now="$(date +%s)"
    if (( now - last_progress >= 15 )); then
      printf 'status=%s chunks=%s/%s records=%s/%s elapsed=%ss\n' "$status" "$completed" "$expected" "$published" "$records" "$((now - case_start_ms / 1000))"
      last_progress="$now"
      if [[ "$(queue_count "$chunk_dlq_url")" -gt 0 ]]; then
        echo "Chunk enviado à DLQ; encerrando a espera do caso como FAILED." >&2
        status="FAILED"
        break
      fi
    fi
    [[ "$status" == "COMPLETED" || "$status" == "FAILED" ]] && break
    sleep "$poll_seconds"
  done
  process_ms="$(elapsed_ms "$case_start_ms")"
  case_end_ms="$(now_ms)"
  case_end_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  [[ "$status" == "FAILED" ]] || status="$(jq -r '.status.S // "TIMEOUT"' <<<"$job")"
  expected_chunks="$(jq -r '.expectedChunks.N // "0"' <<<"$job")"
  completed_chunks="$(jq -r '.completedChunks.N // "0"' <<<"$job")"
  records_read="$(jq -r '.recordsRead.N // "0"' <<<"$job")"
  records_published="$(jq -r '.recordsPublished.N // .recordsProduced.N // "0"' <<<"$job")"
  messages_published="$(jq -r '.messagesPublished.N // "0"' <<<"$job")"
  records_rejected="$(jq -r '.recordsRejected.N // "0"' <<<"$job")"
  records_ignored="$(jq -r '.recordsIgnored.N // "0"' <<<"$job")"
  output_messages="$(wait_queue_count "$output_queue_url" "$messages_published")"
  sample_output_bundles "$case_dir"
  bundle_sample="$(<"$case_dir/bundle-sample-summary.json")"
  intake_dlq="$(queue_count "$intake_dlq_url")"
  chunk_dlq="$(queue_count "$chunk_dlq_url")"
  throughput="$(awk -v records="$records_published" -v ms="$process_ms" 'BEGIN{if(ms>0) printf "%.2f",records*1000/ms; else print 0}')"
  mib_per_second="$(awk -v bytes="$size_bytes" -v ms="$process_ms" 'BEGIN{if(ms>0) printf "%.2f",bytes*1000/ms/1048576; else print 0}')"

  collect_metrics "$case_dir" "$case_start_ms" "$case_end_ms" "$case_start_iso" "$case_end_iso"
  cpu="$(<"$case_dir/cpu-summary.json")"
  memory="$(<"$case_dir/memory-summary.json")"
  gc="$(<"$case_dir/gc-summary.json")"
  lambda_errors="$(jq '[.Datapoints[]?.Sum] | add // 0' "$case_dir/cloudwatch-Errors.json")"
  lambda_throttles="$(jq '[.Datapoints[]?.Sum] | add // 0' "$case_dir/cloudwatch-Throttles.json")"

  jq -n \
    --arg label "$label" --arg jobId "$job_id" --arg status "$status" --arg objectKey "$object_key" \
    --arg startedAt "$case_start_iso" --arg endedAt "$case_end_iso" --arg sha256 "$(jq -r .sha256 <<<"$manifest")" \
    --argjson records "$records" --argjson recordLength "$record_length" --argjson size "$size_bytes" \
    --arg outputMode "$output_mode" --argjson maxEnvelopes "$max_envelopes" --argjson maxMessageBytes "$max_message_bytes" \
    --argjson chunks "$case_records_per_chunk" --argjson targetChunkBytes "$target_chunk_bytes" --argjson workers "$worker_concurrency" --argjson memoryMb "$worker_memory_mb" --argjson timeout "$lambda_timeout" --argjson publishConcurrency "$publish_concurrency" \
    --argjson generation "$generation_ms" --argjson upload "$upload_ms" --argjson processing "$process_ms" \
    --argjson expected "$expected_chunks" --argjson completed "$completed_chunks" --argjson read "$records_read" \
    --argjson published "$records_published" --argjson messages "$messages_published" --argjson rejected "$records_rejected" --argjson ignored "$records_ignored" \
    --argjson queueMessages "$output_messages" --argjson intakeDlq "$intake_dlq" --argjson chunkDlq "$chunk_dlq" --argjson throughput "$throughput" --argjson mibps "$mib_per_second" \
    --argjson cpu "$cpu" --argjson memory "$memory" --argjson gc "$gc" --argjson bundleSample "$bundle_sample" --argjson lambdaErrors "$lambda_errors" --argjson lambdaThrottles "$lambda_throttles" \
    '{label:$label,jobId:$jobId,status:$status,objectKey:$objectKey,startedAt:$startedAt,endedAt:$endedAt,input:{records:$records,recordLengthBytes:$recordLength,sizeBytes:$size,sha256:$sha256},parameters:{recordsPerChunk:$chunks,targetChunkBytes:$targetChunkBytes,workerConcurrency:$workers,lambdaMemoryMB:$memoryMb,lambdaTimeoutSeconds:$timeout,publishConcurrencyPerWorker:$publishConcurrency,outputMode:$outputMode,maxEnvelopesPerMessage:$maxEnvelopes,maxMessageBytes:$maxMessageBytes,sqsApiBatchSize:10},durationsMs:{generation:$generation,upload:$upload,processing:$processing},result:{expectedChunks:$expected,completedChunks:$completed,recordsRead:$read,recordsPublished:$published,messagesPublished:$messages,approximateOutputQueueMessages:$queueMessages,averageLogicalEventsPerMessage:(if $messages > 0 then $published / $messages else 0 end),intakeDlq:$intakeDlq,chunkDlq:$chunkDlq,recordsRejected:$rejected,recordsIgnored:$ignored,recordsPerSecond:$throughput,inputMiBPerSecond:$mibps,lambdaErrors:$lambdaErrors,lambdaThrottles:$lambdaThrottles},resources:{cpu:$cpu,memory:$memory,gc:$gc,bundleSample:$bundleSample,rawLogs:"cloudwatch-logs.json"}}' > "$case_dir/report.json"
  cat "$case_dir/report.json"

  if [[ "$status" != "COMPLETED" || "$records_read" -ne "$records" || "$records_published" -ne "$records" || "$messages_published" -le 0 || "$output_messages" -ne "$messages_published" || "$records_rejected" -ne 0 || "$records_ignored" -ne 0 || "$completed_chunks" -ne "$expected_chunks" || "$intake_dlq" -ne 0 || "$chunk_dlq" -ne 0 ]]; then
    echo "Caso $label falhou na validação; o próximo caso não será iniciado." >&2
    return 1
  fi
  echo "Caso $label concluído e validado."
}

run_case 10000 10k "$smoke_records_per_chunk"

if [[ "$preflight_timeout_ms" -gt 0 ]]; then
  smoke_p99="$(jq -r '.resources.memory.p99DurationMs // 0' "$result_dir/10k/report.json")"
  nominal_chunk_records=$(( (target_chunk_bytes + record_length - 1) / record_length ))
  projected_p99="$(awk -v p99="$smoke_p99" -v chunk="$nominal_chunk_records" 'BEGIN{printf "%.3f", p99 * chunk / 10000}')"
  if awk -v projected="$projected_p99" -v limit="$preflight_timeout_ms" 'BEGIN{exit !(projected > limit)}'; then
    echo "Preflight rejeitou $worker_memory_mb MiB: p99 projetado ${projected_p99} ms > ${preflight_timeout_ms} ms." >&2
    mkdir -p "$result_dir/$full_label"
    jq --arg label "$full_label" --argjson records "$full_records" --argjson chunks "$full_records_per_chunk" \
      --argjson projected "$projected_p99" --argjson limit "$preflight_timeout_ms" '
      . as $smoke |
      {label:$label,status:"PREFLIGHT_REJECTED",startedAt:$smoke.startedAt,endedAt:$smoke.endedAt,
       input:{records:$records,recordLengthBytes:$smoke.input.recordLengthBytes,sizeBytes:($records * $smoke.input.recordLengthBytes)},
       parameters:($smoke.parameters + {recordsPerChunk:$chunks}),durationsMs:{generation:0,upload:0,processing:0},
       result:{expectedChunks:0,completedChunks:0,recordsRead:0,recordsPublished:0,messagesPublished:0,approximateOutputQueueMessages:0,intakeDlq:0,chunkDlq:0,recordsRejected:0,recordsIgnored:0,recordsPerSecond:0,inputMiBPerSecond:0,lambdaErrors:0,lambdaThrottles:0,preflightProjectedP99Ms:$projected,preflightLimitMs:$limit},
       resources:{cpu:$smoke.resources.cpu,memory:($smoke.resources.memory + {p95DurationMs:$projected,p99DurationMs:$projected,peakDurationMs:$projected,measurementSource:"smoke_projection"}),gc:$smoke.resources.gc,rawLogs:"../10k/cloudwatch-logs.json"}}' \
      "$result_dir/10k/report.json" > "$result_dir/$full_label/report.json"
    exit 1
  fi
fi

run_case "$full_records" "$full_label" "$full_records_per_chunk"

jq -n \
  --arg runId "$run_id" --arg account "$account" --arg region "$region" --arg worker "$worker_name" \
  --arg fullLabel "$full_label" --argjson provisionMs "$provision_ms" --slurpfile smoke "$result_dir/10k/report.json" --slurpfile full "$result_dir/$full_label/report.json" \
  '{runId:$runId,aws:{accountId:$account,region:$region},workerFunction:$worker,provisionMs:$provisionMs,cases:{"10k":$smoke[0],($fullLabel):$full[0]}}' > "$result_dir/report.json"
echo "Benchmark completo: $result_dir/report.json"
