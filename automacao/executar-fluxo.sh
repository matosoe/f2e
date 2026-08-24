#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
source "$root/parametros.sh"

agora_ms() {
  date +%s%3N
}

exibir_duracao() {
  local descricao="$1" inicio="$2" fim duracao
  fim="$(agora_ms)"
  duracao=$((fim - inicio))
  printf '%s: %d.%03ds\n' "$descricao" "$((duracao / 1000))" "$((duracao % 1000))"
}

executar_etapa() {
  local descricao="$1" inicio status
  shift
  inicio="$(agora_ms)"
  printf 'Iniciando: %s\n' "$descricao"
  if "$@"; then
    exibir_duracao "Concluída: $descricao" "$inicio"
  else
    status=$?
    exibir_duracao "Falhou: $descricao" "$inicio" >&2
    return "$status"
  fi
}

preparar_arquivo() {
  data="$root/dados/entrada-${QUANTIDADE_REGISTROS}.txt"
  [[ -f "$data" ]] || "$root/gerar-arquivo.sh"
}

limpar_filas() {
  local q
  for q in file-intake chunk-jobs output-events file-intake-dlq chunk-jobs-dlq output-events-dlq; do
    awsq purge-queue --queue-url "$(awsq get-queue-url --queue-name "$q" --query QueueUrl --output text)" 2>/dev/null || true
  done
}

enviar_arquivo() {
  aws s3 --endpoint-url http://localhost:4566 --region us-east-1 cp "$data" "s3://f2e-input/input/entrada-${QUANTIDADE_REGISTROS}.txt"
}

aguardar_processamento() {
  local out count
  out="$(awsq get-queue-url --queue-name output-events --query QueueUrl --output text)"
  for _ in {1..150}; do
    count="$(awsq get-queue-attributes --queue-url "$out" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text)"
    [[ "$count" -ge "$QUANTIDADE_REGISTROS" ]] && break
    sleep 2
  done
  [[ "$count" -ge "$QUANTIDADE_REGISTROS" ]] || { echo "Timeout: $count mensagens" >&2; return 1; }
}

inicio_fluxo="$(agora_ms)"
cleanup() {
  local inicio
  inicio="$(agora_ms)"
  docker compose -f "$root/docker-compose.yml" down --remove-orphans >/dev/null
  exibir_duracao 'Concluída: encerramento do ambiente' "$inicio"
  exibir_duracao 'Duração total do fluxo' "$inicio_fluxo"
}
trap cleanup EXIT
awsq() { aws sqs --endpoint-url http://localhost:4566 --region us-east-1 "$@"; }

executar_etapa 'subida do ambiente' "$root/subir-ambiente.sh"
executar_etapa 'preparação do arquivo de entrada' preparar_arquivo
executar_etapa 'limpeza das filas' limpar_filas
executar_etapa 'upload do arquivo para o S3' enviar_arquivo
executar_etapa 'processamento das mensagens' aguardar_processamento
executar_etapa 'validação do fluxo' "$root/validar-fluxo.sh"
