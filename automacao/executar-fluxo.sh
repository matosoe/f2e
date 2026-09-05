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
  for q in file-intake chunk-jobs output-events file-intake-dlq chunk-jobs-dlq; do
    awsq purge-queue --queue-url "$(awsq get-queue-url --queue-name "$q" --query QueueUrl --output text)" 2>/dev/null || true
  done
}

exibir_resumo_filas() {
  local q url disponiveis em_processamento

  printf '\nResumo das filas (antes da validação)\n'
  printf '%-20s %14s %18s\n' 'Fila' 'Disponíveis' 'Em processamento'
  printf '%-20s %14s %18s\n' '--------------------' '--------------' '------------------'

  for q in file-intake chunk-jobs output-events file-intake-dlq chunk-jobs-dlq; do
    url="$(awsq get-queue-url --queue-name "$q" --query QueueUrl --output text)"
    disponiveis="$(awsq get-queue-attributes --queue-url "$url" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text)"
    em_processamento="$(awsq get-queue-attributes --queue-url "$url" --attribute-names ApproximateNumberOfMessagesNotVisible --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text)"
    printf '%-20s %14s %18s\n' "$q" "$disponiveis" "$em_processamento"
  done
  printf '\n'
}

enviar_arquivo() {
  aws s3 --endpoint-url http://localhost:4566 --region us-east-1 cp "$data" "s3://f2e-input/input/entrada-${QUANTIDADE_REGISTROS}.txt"
}

enviar_solicitacao_organizer() {
  local intake body
  intake="$(awsq get-queue-url --queue-name file-intake --query QueueUrl --output text)"
  body="$(jq -cn --arg bucket f2e-input --arg key "input/entrada-${QUANTIDADE_REGISTROS}.txt" '{schemaVersion:"1",files:[{bucket:$bucket,key:$key,dataType:"text"}]}')"
  awsq send-message --queue-url "$intake" --message-body "$body" >/dev/null
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
  local inicio duracao_total tps
  duracao_total=$(($(agora_ms) - inicio_fluxo))

  if [[ "${DERRUBAR_AMBIENTE:-false}" == "true" ]]; then
    inicio="$(agora_ms)"
    printf 'Iniciando: encerramento do ambiente\n'
    docker compose -f "$root/docker-compose.yml" down --remove-orphans >/dev/null
    exibir_duracao 'Concluída: encerramento do ambiente' "$inicio"
  else
    printf 'Ambiente mantido em execução (DERRUBAR_AMBIENTE=false).\n'
  fi

  printf '\n=== RESUMO DA EXECUÇÃO ===\n'
  printf 'Total de registros: %d\n' "$QUANTIDADE_REGISTROS"
  exibir_duracao 'Duração total' "$inicio_fluxo"
  if [[ $duracao_total -gt 0 ]]; then
    tps=$(echo "scale=2; $QUANTIDADE_REGISTROS * 1000 / $duracao_total" | bc)
    printf 'TPS geral: %.2f registros/s\n' "$tps"
  fi
  printf '==========================\n\n'
}
trap cleanup EXIT
awsq() { aws sqs --endpoint-url http://localhost:4566 --region us-east-1 "$@"; }

executar_etapa 'subida do ambiente' "$root/subir-ambiente.sh"
executar_etapa 'preparação do arquivo de entrada' preparar_arquivo
executar_etapa 'limpeza das filas' limpar_filas
executar_etapa 'upload do arquivo para o S3' enviar_arquivo
executar_etapa 'envio da solicitação ao Organizer' enviar_solicitacao_organizer
executar_etapa 'processamento das mensagens' aguardar_processamento
executar_etapa 'resumo das filas' exibir_resumo_filas
executar_etapa 'validação do fluxo' "$root/validar-fluxo.sh"
