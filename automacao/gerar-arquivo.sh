#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
framework="$(cd "$root/.." && pwd)"
source "$root/parametros.sh"

out="$root/dados/entrada-${QUANTIDADE_REGISTROS}.txt"
records="$QUANTIDADE_REGISTROS"
length=100
args=()
while (($#)); do
  case "$1" in
    --output) out="$2"; shift 2 ;;
    --records) records="$2"; shift 2 ;;
    --record-length) length="$2"; shift 2 ;;
    --force) args+=(--force); shift ;;
    *) echo "argumento inválido: $1" >&2; exit 2 ;;
  esac
done

cd "$framework"
go run ./automacao/cmd/generate-fixed-file \
  --output "$out" \
  --records "$records" \
  --record-length "$length" \
  "${args[@]}"
