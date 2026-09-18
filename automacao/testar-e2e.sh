#!/usr/bin/env bash
# testar-e2e.sh — Executa a suíte E2E em ambiente já provisionado (LocalStack ou AWS).
# Não provisiona nem destrói recursos.
#
# Variáveis de ambiente relevantes:
#   E2E_TAGS          — filtro de tags Godog (ex: "@smoke"); omitir para executar tudo exceto @load
#   E2E_LOAD_TESTS    — "true" para incluir cenários @load (default: false)
#   E2E_REPORT_DIR    — pasta dos cinco relatórios (default: e2e/relatorios)
#   E2E_METRICS_FILE  — cópia opcional do resumo JSON para integração legada
#   F2E_E2E_TARGET    — "aws" para usar credenciais AWS reais; omitir para LocalStack
#
# Uso: bash automacao/testar-e2e.sh
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"

export E2E_REPORT_DIR="${E2E_REPORT_DIR:-$framework/e2e/relatorios}"
# O LocalStack 3.8.x apresenta uma condição de corrida no SQS com cenários
# paralelos. A execução local é sequencial por padrão; benchmarks podem definir
# E2E_CONCURRENCY explicitamente.
export E2E_CONCURRENCY="${E2E_CONCURRENCY:-1}"

cd "$framework/e2e"
go test -count=1 -v -timeout 20m ./...

echo ""
echo "Relatórios: $E2E_REPORT_DIR"
