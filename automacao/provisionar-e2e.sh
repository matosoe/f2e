#!/usr/bin/env bash
# provisionar-e2e.sh — Provisiona o ambiente E2E sem destruí-lo depois.
# Use quando você quer manter o ambiente ativo para executar a suíte várias vezes.
# Contraste com subir-ambiente.sh, que sempre recria o ambiente do zero.
#
# Uso: bash automacao/provisionar-e2e.sh
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
for cmd in docker go aws; do command -v "$cmd" >/dev/null || { echo "$cmd não encontrado" >&2; exit 1; }; done
docker info >/dev/null 2>&1 || { echo 'Docker não está disponível.' >&2; exit 1; }

bash "$root/build-lambdas.sh"

# Verifica se o ambiente já está no ar antes de subir.
if docker compose -f "$root/docker-compose.yml" exec -T localstack awslocal lambda get-function --function-name f2e-worker >/dev/null 2>&1; then
  echo "Ambiente E2E já provisionado. Use testar-e2e.sh para executar a suíte."
  exit 0
fi

docker compose -f "$root/docker-compose.yml" up -d

for _ in {1..60}; do
  if docker compose -f "$root/docker-compose.yml" exec -T localstack awslocal lambda get-function --function-name f2e-worker >/dev/null 2>&1; then
    echo "Ambiente provisionado. Execute: bash automacao/testar-e2e.sh"
    exit 0
  fi
  sleep 2
done

echo 'LocalStack não foi provisionado no prazo. Logs recentes:' >&2
docker compose -f "$root/docker-compose.yml" logs --tail 100 localstack >&2 || true
exit 1
