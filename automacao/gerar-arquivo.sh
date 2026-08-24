#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
source "$root/parametros.sh"
out="$root/dados/entrada-${QUANTIDADE_REGISTROS}.txt"; records="$QUANTIDADE_REGISTROS"; length=100; force=false
while (($#)); do case "$1" in --output) out="$2";shift 2;;--records) records="$2";shift 2;;--record-length) length="$2";shift 2;;--force) force=true;shift;;*) echo "argumento inválido: $1" >&2;exit 2;;esac;done
[[ -e "$out" && "$force" != true ]] && { echo "Arquivo já existe: $out (use --force)" >&2; exit 1; }
mkdir -p "$(dirname "$out")"; : > "$out"
for ((i=1;i<=records;i++)); do printf "%08d" "$i" >> "$out"; printf '%*s\n' $((length-9)) '' | tr ' ' X >> "$out"; done
size="$(wc -c < "$out" | tr -d ' ')"; hash="$(sha256sum "$out" | awk '{print $1}')"
printf '{"records":%s,"recordLengthBytes":%s,"sizeBytes":%s,"sha256":"%s"}\n' "$records" "$length" "$size" "$hash" > "$out.manifest.json"
echo "Gerado: $out ($records registros, $hash)"
