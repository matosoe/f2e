#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"
cd "$framework/lambdas"
for cmd in docker go aws; do command -v "$cmd" >/dev/null || { echo "$cmd não encontrado" >&2; exit 1; }; done
docker info >/dev/null 2>&1 || { echo 'Docker não está disponível.' >&2; exit 1; }
mkdir -p "$framework/.build/organizer" "$framework/.build/worker"
GOOS=linux GOARCH=amd64 go build -o "$framework/.build/organizer/bootstrap" ./cmd/organizer
GOOS=linux GOARCH=amd64 go build -o "$framework/.build/worker/bootstrap" ./cmd/worker
build="$(cd "$framework/.build" && pwd)"
build_win="$(cygpath -w "$build")"
docker run --rm --mount "type=bind,source=$build_win,target=/work" alpine:3.20 sh -c 'apk add --no-cache zip >/dev/null && chmod +x /work/organizer/bootstrap /work/worker/bootstrap && cd /work/organizer && zip -q -j ../organizer.zip bootstrap && cd /work/worker && zip -q -j ../worker.zip bootstrap'
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
