#!/usr/bin/env bash
# testar-e2e.sh — Executa a suíte E2E em ambiente já provisionado (LocalStack ou AWS).
# Não provisiona nem destrói recursos.
#
# Variáveis de ambiente relevantes:
#   E2E_TAGS          — filtro de tags Godog (ex: "@smoke"); omitir para executar tudo exceto @load
#   E2E_LOAD_TESTS    — "true" para incluir cenários @load (default: false)
#   E2E_METRICS_FILE  — caminho do relatório JSON de métricas (default: e2e/e2e-metrics.json)
#   F2E_E2E_TARGET    — "aws" para usar credenciais AWS reais; omitir para LocalStack
#
# Uso: bash automacao/testar-e2e.sh
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"

export E2E_METRICS_FILE="${E2E_METRICS_FILE:-$framework/e2e/e2e-metrics.json}"

cd "$framework/e2e"
go test -v -timeout 20m ./...

echo ""
echo "Relatório de métricas: $E2E_METRICS_FILE"
