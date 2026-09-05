#!/usr/bin/env bash
# destruir-e2e.sh — Remove o ambiente E2E descartável (containers, volumes, redes).
# Nunca é executado automaticamente por provisionar-e2e.sh ou testar-e2e.sh.
# Equivalente à parte de teardown do subir-ambiente.sh.
#
# Uso: bash automacao/destruir-e2e.sh
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
docker compose -f "$root/docker-compose.yml" down --volumes --remove-orphans
echo "Ambiente E2E destruído."
