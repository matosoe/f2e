#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")" && pwd)"
source "$root/parametros.sh"
awsq(){ aws sqs --endpoint-url http://localhost:4566 --region us-east-1 "$@"; }
url="$(awsq get-queue-url --queue-name output-events --query QueueUrl --output text)"
total="$(awsq get-queue-attributes --queue-url "$url" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text)"
[[ "$total" == "$QUANTIDADE_REGISTROS" ]] || { echo "Quantidade inválida: $total/$QUANTIDADE_REGISTROS mensagens" >&2; exit 1; }

awsq purge-queue --queue-url "$url" >/dev/null
echo "$total/$QUANTIDADE_REGISTROS mensagens na fila final; fila limpa."
