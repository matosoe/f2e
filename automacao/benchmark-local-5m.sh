#!/usr/bin/env bash
# Benchmark E2E pesado e deliberadamente separado da suíte go test/Godog.
# Produz 5 milhões de registros de 100 bytes (500.000.000 bytes) por padrão.
set -euo pipefail

root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"

records="${BENCHMARK_RECORDS:-5000000}"
record_length="${BENCHMARK_RECORD_LENGTH:-100}"
records_per_chunk="${BENCHMARK_RECORDS_PER_CHUNK:-50000}"
publish_concurrency="${BENCHMARK_PUBLISH_CONCURRENCY:-8}"
output_mode="${BENCHMARK_OUTPUT_MODE:-bundle}"
max_envelopes="${BENCHMARK_MAX_ENVELOPES_PER_MESSAGE:-100}"
max_message_bytes="${BENCHMARK_MAX_MESSAGE_BYTES:-24000}"
timeout_seconds="${BENCHMARK_TIMEOUT_SECONDS:-7200}"
sample_seconds="${BENCHMARK_SAMPLE_SECONDS:-2}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)"
result_dir="${BENCHMARK_RESULTS_DIR:-$root/resultados/benchmark-local-5m/$run_id}"
data_file="${BENCHMARK_DATA_FILE:-$root/dados/benchmark-${records}x${record_length}.txt}"
prefix="example-text/benchmark"
object_key="${prefix}/${run_id}-${records}x${record_length}.txt"
endpoint="http://localhost:4566"
region="us-east-1"
bucket="f2e-input"
table="f2e-job-ledger"

# Keep every CLI call in the same LocalStack account namespace used by init-aws.sh.
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_SESSION_TOKEN=""
export AWS_DEFAULT_REGION="$region"

case "$output_mode" in single|bundle) ;; *) echo "BENCHMARK_OUTPUT_MODE must be single or bundle" >&2; exit 2 ;; esac
for value in "$records" "$record_length" "$records_per_chunk" "$publish_concurrency" "$max_message_bytes" "$timeout_seconds" "$sample_seconds"; do
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "benchmark numeric values must be positive integers" >&2; exit 2; }
done
for command in aws docker go jq; do
  command -v "$command" >/dev/null || { echo "$command not found" >&2; exit 1; }
done

mkdir -p "$result_dir"
resource_csv="$result_dir/docker-resources.csv"
report="$result_dir/report.json"
log="$result_dir/run.log"
stop_file="$result_dir/.stop-sampler"
rm -f "$stop_file"
exec > >(tee -a "$log") 2>&1

now_ms() { date +%s%3N; }
elapsed_ms() { echo $(( $(now_ms) - $1 )); }
aws_local() { aws --endpoint-url "$endpoint" --region "$region" "$@"; }
aws_local_no_pathconv() { MSYS_NO_PATHCONV=1 aws --endpoint-url "$endpoint" --region "$region" "$@"; }
queue_url() { aws_local sqs get-queue-url --queue-name "$1" --query QueueUrl --output text; }
queue_count() {
  aws_local sqs get-queue-attributes --queue-url "$(queue_url "$1")" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible --output json |
    jq '[.Attributes.ApproximateNumberOfMessages, .Attributes.ApproximateNumberOfMessagesNotVisible] | map(tonumber) | add'
}

sample_resources() {
  printf 'timestamp_utc,name,cpu_percent,memory_percent,memory_usage,net_io,block_io,pids\n' > "$resource_csv"
  while [[ ! -e "$stop_file" ]]; do
    timestamp="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemPerc}}|{{.MemUsage}}|{{.NetIO}}|{{.BlockIO}}|{{.PIDs}}' 2>/dev/null |
      while IFS='|' read -r name cpu mem_pct mem_usage net_io block_io pids; do
        printf '%s,%s,%s,%s,"%s","%s","%s",%s\n' "$timestamp" "$name" "${cpu%%%}" "${mem_pct%%%}" "$mem_usage" "$net_io" "$block_io" "$pids" >> "$resource_csv"
      done
    sleep "$sample_seconds"
  done
}

sampler_pid=""
stop_sampler() {
  if [[ -n "$sampler_pid" ]]; then
    touch "$stop_file"
    wait "$sampler_pid" 2>/dev/null || true
    sampler_pid=""
  fi
}
trap stop_sampler EXIT

echo "=== F2E local 5M benchmark: $run_id ==="
echo "Results: $result_dir"

cpu_model="$(grep -m1 '^model name' /proc/cpuinfo | cut -d: -f2- | sed 's/^ //')"
logical_threads="$(nproc)"
physical_cores="$(awk '/^physical id/{package=$NF}/^core id/{seen[package ":" $NF]=1} END{print length(seen)}' /proc/cpuinfo)"
host_ram_bytes="$(( $(awk '/^MemTotal:/{print $2}' /proc/meminfo) * 1024 ))"
docker_cpus="$(docker info --format '{{.NCPU}}')"
docker_ram_bytes="$(docker info --format '{{.MemTotal}}')"
printf 'Host: %s; %s cores; %s threads; %s bytes RAM\n' "$cpu_model" "$physical_cores" "$logical_threads" "$host_ram_bytes"
printf 'Docker Desktop: %s CPUs; %s bytes RAM\n' "$docker_cpus" "$docker_ram_bytes"

generation_start="$(now_ms)"
if [[ -f "$data_file" && -f "$data_file.manifest.json" ]] &&
   jq -e --argjson records "$records" --argjson length "$record_length" \
     '.records == $records and .recordLengthBytes == $length and .sizeBytes == ($records * $length)' \
     "$data_file.manifest.json" >/dev/null; then
  echo "Reusing valid data file: $data_file"
else
  "$root/gerar-arquivo.sh" --output "$data_file" --records "$records" --record-length "$record_length" --force
fi
generation_ms="$(elapsed_ms "$generation_start")"
manifest="$(<"$data_file.manifest.json")"
actual_size="$(jq -r .sizeBytes <<<"$manifest")"
[[ "$actual_size" -eq $((records * record_length)) ]] || { echo "invalid generated size: $actual_size" >&2; exit 1; }

provision_start="$(now_ms)"
"$root/subir-ambiente.sh"
provision_ms="$(elapsed_ms "$provision_start")"

# Retain the Terraform-managed prefixId and chunkQueueURL so this benchmark
# exercises the exclusive queue instead of the retired shared queue.
config_path="/f2e/local/file-config/${bucket}/example-text"
base_config="$(aws_local_no_pathconv ssm get-parameter --name "$config_path" --query Parameter.Value --output text)"
config=$(jq -c \
  --arg mode "$output_mode" \
  --argjson chunk "$records_per_chunk" --argjson length "$record_length" --argjson envelopes "$max_envelopes" --argjson messageBytes "$max_message_bytes" \
  '. + {recordsPerChunk:$chunk,maxRecordLengthBytes:$length,outputMode:$mode,maxEnvelopesPerMessage:$envelopes,maxMessageBytes:$messageBytes}' <<<"$base_config")
aws_local_no_pathconv ssm put-parameter --name "$config_path" --type String --value "$config" --overwrite >/dev/null

# update-function-configuration replaces the Variables map, so merge first.
worker_name="f2e-local-example-text-worker"
worker_env="$(aws_local lambda get-function-configuration --function-name "$worker_name" --query Environment.Variables --output json)"
worker_env="$(jq -c --arg concurrency "$publish_concurrency" '. + {F2E_PUBLISH_CONCURRENCY:$concurrency}' <<<"$worker_env")"
aws_local lambda update-function-configuration --function-name "$worker_name" --environment "{\"Variables\":$worker_env}" >/dev/null
aws_local lambda wait function-updated-v2 --function-name "$worker_name"

for q in file-intake f2e-local-example-text-chunks f2e-local-example-text-output file-intake-dlq f2e-local-example-text-chunks-dlq; do
  aws_local sqs purge-queue --queue-url "$(queue_url "$q")" >/dev/null 2>&1 || true
done

sample_resources &
sampler_pid=$!
upload_start="$(now_ms)"
# AWS CLI releases may default multipart uploads to CRC64NVME, which LocalStack
# 3.8 does not implement. CRC32 is supported by both LocalStack and real S3.
aws_local s3 cp "$data_file" "s3://$bucket/$object_key" --no-progress --checksum-algorithm CRC32
upload_ms="$(elapsed_ms "$upload_start")"

intake="$(queue_url file-intake)"
version_id="$(aws_local s3api head-object --bucket "$bucket" --key "$object_key" --query VersionId --output text)"
[[ -n "$version_id" && "$version_id" != "None" ]] || { echo "uploaded object has no immutable VersionId" >&2; exit 1; }
# Use the production S3-notification admission path so the dedicated SSM prefix
# configuration (chunk size, output mode and record width) is actually resolved.
request="$(jq -cn --arg bucket "$bucket" --arg key "$object_key" --arg version "$version_id" '{Records:[{eventName:"ObjectCreated:Put",s3:{bucket:{name:$bucket},object:{key:$key,versionId:$version}}}]}')"
process_start="$(now_ms)"
aws_local sqs send-message --queue-url "$intake" --message-body "$request" >/dev/null

deadline=$(( $(date +%s) + timeout_seconds ))
job_id=""
job='{}'
last_progress=0
while (( $(date +%s) < deadline )); do
  items="$(aws_local dynamodb scan --table-name "$table" --consistent-read --output json)"
  job="$(jq -c --arg key "$object_key" '[.Items[] | select(.sk.S == "JOB" and .key.S == $key)] | sort_by(.createdAt.S) | last // {}' <<<"$items")"
  job_id="$(jq -r '.jobId.S // empty' <<<"$job")"
  status="$(jq -r '.status.S // "WAITING"' <<<"$job")"
  completed="$(jq -r '.completedChunks.N // "0"' <<<"$job")"
  expected="$(jq -r '.expectedChunks.N // "0"' <<<"$job")"
  published="$(jq -r '.recordsPublished.N // .recordsProduced.N // "0"' <<<"$job")"
  now="$(date +%s)"
  if (( now - last_progress >= 10 )); then
    printf 'status=%s chunks=%s/%s records=%s/%s elapsed=%ss\n' "$status" "$completed" "$expected" "$published" "$records" "$((now - process_start / 1000))"
    last_progress="$now"
  fi
  [[ "$status" == "COMPLETED" || "$status" == "FAILED" ]] && break
  sleep 2
done
process_ms="$(elapsed_ms "$process_start")"
stop_sampler

status="$(jq -r '.status.S // "TIMEOUT"' <<<"$job")"
completed_chunks="$(jq -r '.completedChunks.N // "0"' <<<"$job")"
expected_chunks="$(jq -r '.expectedChunks.N // "0"' <<<"$job")"
records_published="$(jq -r '.recordsPublished.N // .recordsProduced.N // "0"' <<<"$job")"
records_read="$(jq -r '.recordsRead.N // "0"' <<<"$job")"
records_rejected="$(jq -r '.recordsRejected.N // "0"' <<<"$job")"
records_ignored="$(jq -r '.recordsIgnored.N // "0"' <<<"$job")"
output_messages="$(queue_count f2e-local-example-text-output)"
intake_dlq="$(queue_count file-intake-dlq)"
chunk_dlq="$(queue_count f2e-local-example-text-chunks-dlq)"

peak_cpu="$(awk -F, 'NR>1 && $3+0>max{max=$3+0} END{printf "%.2f", max+0}' "$resource_csv")"
peak_memory_pct="$(awk -F, 'NR>1 && $4+0>max{max=$4+0} END{printf "%.2f", max+0}' "$resource_csv")"
throughput="$(awk -v records="$records_published" -v ms="$process_ms" 'BEGIN{if(ms>0) printf "%.2f", records*1000/ms; else print "0"}')"
mib_per_second="$(awk -v bytes="$actual_size" -v ms="$process_ms" 'BEGIN{if(ms>0) printf "%.2f", bytes*1000/ms/1048576; else print "0"}')"

jq -n \
  --arg runId "$run_id" --arg status "$status" --arg jobId "$job_id" --arg objectKey "$object_key" \
  --arg cpuModel "$cpu_model" --arg outputMode "$output_mode" --arg sha256 "$(jq -r .sha256 <<<"$manifest")" \
  --argjson physicalCores "$physical_cores" --argjson logicalThreads "$logical_threads" --argjson hostRamBytes "$host_ram_bytes" \
  --argjson dockerCpus "$docker_cpus" --argjson dockerRamBytes "$docker_ram_bytes" \
  --argjson records "$records" --argjson recordLengthBytes "$record_length" --argjson sizeBytes "$actual_size" \
  --argjson recordsPerChunk "$records_per_chunk" --argjson publishConcurrency "$publish_concurrency" --argjson maxEnvelopes "$max_envelopes" --argjson maxMessageBytes "$max_message_bytes" \
  --argjson generationMs "$generation_ms" --argjson provisionMs "$provision_ms" --argjson uploadMs "$upload_ms" --argjson processMs "$process_ms" \
  --argjson expectedChunks "$expected_chunks" --argjson completedChunks "$completed_chunks" --argjson recordsRead "$records_read" \
  --argjson recordsPublished "$records_published" --argjson recordsRejected "$records_rejected" --argjson recordsIgnored "$records_ignored" \
  --argjson outputMessages "$output_messages" --argjson intakeDlq "$intake_dlq" --argjson chunkDlq "$chunk_dlq" \
  --argjson throughput "$throughput" --argjson mibPerSecond "$mib_per_second" --argjson peakContainerCpuPercent "$peak_cpu" --argjson peakContainerMemoryPercent "$peak_memory_pct" \
  '{runId:$runId,status:$status,jobId:$jobId,objectKey:$objectKey,machine:{cpu:$cpuModel,physicalCores:$physicalCores,logicalThreads:$logicalThreads,ramBytes:$hostRamBytes,dockerDesktop:{cpus:$dockerCpus,ramBytes:$dockerRamBytes}},input:{records:$records,recordLengthBytes:$recordLengthBytes,sizeBytes:$sizeBytes,sha256:$sha256},parameters:{recordsPerChunk:$recordsPerChunk,publishConcurrency:$publishConcurrency,outputMode:$outputMode,maxEnvelopesPerMessage:$maxEnvelopes,maxMessageBytes:$maxMessageBytes},durationsMs:{generation:$generationMs,provision:$provisionMs,upload:$uploadMs,processing:$processMs},result:{expectedChunks:$expectedChunks,completedChunks:$completedChunks,recordsRead:$recordsRead,recordsPublished:$recordsPublished,recordsRejected:$recordsRejected,recordsIgnored:$recordsIgnored,outputMessages:$outputMessages,intakeDlq:$intakeDlq,chunkDlq:$chunkDlq,recordsPerSecond:$throughput,inputMiBPerSecond:$mibPerSecond},resources:{peakSingleContainerCpuPercent:$peakContainerCpuPercent,peakSingleContainerMemoryPercent:$peakContainerMemoryPercent,rawSamples:"docker-resources.csv"}}' > "$report"

cat "$report"
if [[ "$status" != "COMPLETED" || "$records_published" -ne "$records" || "$records_read" -ne "$records" || "$records_rejected" -ne 0 || "$records_ignored" -ne 0 || "$completed_chunks" -ne "$expected_chunks" || "$intake_dlq" -ne 0 || "$chunk_dlq" -ne 0 ]]; then
  echo "Benchmark validation failed; see $report and $log" >&2
  exit 1
fi
echo "Benchmark completed and validated: $report"
