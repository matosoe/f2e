#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"
for cmd in docker go aws; do command -v "$cmd" >/dev/null || { echo "$cmd não encontrado" >&2; exit 1; }; done
docker info >/dev/null 2>&1 || { echo 'Docker não está disponível.' >&2; exit 1; }
bash "$root/build-lambdas.sh"
# The E2E environment is disposable. Recreating it prevents stale Lambda code,
# event-source mappings and in-flight SQS messages from crossing test runs.
docker compose -f "$root/docker-compose.yml" down --volumes --remove-orphans
docker compose -f "$root/docker-compose.yml" up -d
[[ $? -eq 0 ]] || exit $?
for _ in {1..60}; do
  if docker compose -f "$root/docker-compose.yml" exec -T localstack awslocal lambda get-function --function-name f2e-worker >/dev/null 2>&1; then
    exit 0
  fi
  sleep 2
done
echo 'LocalStack não foi provisionado no prazo. Logs recentes:' >&2
docker compose -f "$root/docker-compose.yml" logs --tail 100 localstack >&2 || true
exit 1
