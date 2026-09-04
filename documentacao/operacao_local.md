# Guia de execução local

Este guia descreve como executar e verificar o F2E localmente. O LocalStack
simula S3, SQS, Lambda, DynamoDB, IAM e CloudWatch Logs; nenhuma conta AWS é
necessária.

> Execute os comandos no **Git Bash**, a partir da raiz do repositório. O
> ambiente é descartável e a subida remove recursos de execuções anteriores.

## Pré-requisitos

Tenha no `PATH`: Git Bash, Docker Desktop em execução, Go 1.26+, AWS CLI v2,
`zip`, `jq`, `bc` e `sha256sum`.

O AWS CLI precisa de credenciais sintaticamente válidas para o LocalStack. Elas
não são enviadas à AWS. Se necessário, defina-as na sessão:

```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

docker info
go version
aws --version
```

## Execução ponta a ponta

A execução recomendada é:

```bash
cd automacao
./executar-fluxo.sh
```

O script:

1. Compila as Lambdas e cria ZIPs determinísticos.
2. Recria o LocalStack e provisiona bucket, filas, DLQs, ledger, Lambdas e
   mapeamentos SQS.
3. Gera a massa em `automacao/dados/` e limpa as filas.
4. Envia o arquivo ao bucket `f2e-input`.
5. Aguarda um evento por registro em `output-events`.
6. Exibe o resumo das filas e valida a quantidade de eventos.

A massa padrão contém 90 registros. Um resultado esperado é:

```text
90/90 mensagens na fila final; fila limpa.
```

A validação consome a contagem e limpa `output-events`. Isso impede que eventos
dessa execução interfiram na próxima.

## Ajustar a massa

Edite [`automacao/parametros.sh`](../automacao/parametros.sh):

```bash
QUANTIDADE_REGISTROS=20000
DERRUBAR_AMBIENTE=false
```

`QUANTIDADE_REGISTROS` controla o arquivo gerado. Com `DERRUBAR_AMBIENTE=true`,
o LocalStack é encerrado ao fim do fluxo. O gerador cria registros de 100 bytes,
compatíveis com `F2E_RECORD_LENGTH=100`, e grava um manifesto com tamanho e
SHA-256 em `automacao/dados/`.

O LocalStack também provisiona configurações SSM sob
`/f2e/local/file-config/f2e-input` e uma notificação S3 para os prefixos
`example-*`. Por exemplo, um upload em `s3://f2e-input/example-text/arquivo.txt`
é resolvido como `text` sem publicar manualmente um `OrganizerRequest`.

## Execução manual

Use este caminho para inspecionar a saída antes da limpeza:

```bash
cd automacao
./subir-ambiente.sh
./gerar-arquivo.sh --force

aws s3 --endpoint-url http://localhost:4566 --region us-east-1 \
  cp dados/entrada-90.txt s3://f2e-input/input/entrada-90.txt

intake="$(aws sqs --endpoint-url http://localhost:4566 --region us-east-1 \
  get-queue-url --queue-name file-intake --query QueueUrl --output text)"
aws sqs --endpoint-url http://localhost:4566 --region us-east-1 send-message \
  --queue-url "$intake" \
  --message-body '{"schemaVersion":"1","files":[{"bucket":"f2e-input","key":"input/entrada-90.txt","dataType":"fixed-width"}]}'
```

Ajuste `entrada-90.txt` se tiver alterado a quantidade. O ambiente local usa o
contrato explícito do Organizer, em vez de uma notificação S3, para impedir que
os testes E2E criem jobs duplicados. Não execute `./validar-fluxo.sh` antes de
terminar a inspeção, pois ele limpa a fila final.

## Verificar os resultados

Crie este atalho para consultas SQS locais:

```bash
awsq() { aws sqs --endpoint-url http://localhost:4566 --region us-east-1 "$@"; }
```

### Filas e DLQs

```bash
for q in file-intake chunk-jobs output-events file-intake-dlq chunk-jobs-dlq; do
  url="$(awsq get-queue-url --queue-name "$q" --query QueueUrl --output text)"
  awsq get-queue-attributes --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --query 'Attributes' --output json
done
```

Depois de estabilizar, o esperado é:

| Fila | Resultado |
|---|---|
| `file-intake` | 0 mensagens |
| `chunk-jobs` | 0 mensagens |
| `output-events` | Igual a `QUANTIDADE_REGISTROS`, antes da validação |
| `file-intake-dlq` | 0 mensagens |
| `chunk-jobs-dlq` | 0 mensagens |

As contagens SQS são aproximadas. Aguarde alguns segundos caso existam
mensagens em processamento.

### Conteúdo de um evento

```bash
out="$(awsq get-queue-url --queue-name output-events --query QueueUrl --output text)"
awsq receive-message --queue-url "$out" --max-number-of-messages 1 \
  --message-attribute-names All --attribute-names All --output json | jq .
```

O body é um Envelope F2E: verifique `eventId`, `sourceRecordId`, origem S3,
`jobId`, `chunkId`, `recordNumber` e `data.raw`. Para `binary`, o payload está
em `data.base64`. A leitura não apaga a mensagem; use o `ReceiptHandle` retornado
com `delete-message` se precisar removê-la manualmente.

### Validação automatizada

Quando a inspeção manual não for necessária:

```bash
cd automacao
./validar-fluxo.sh
```

O comando falha se a fila não tiver exatamente `QUANTIDADE_REGISTROS` eventos;
em caso de sucesso, limpa a fila.

### Ledger e logs

Cada arquivo e chunk é registrado no DynamoDB local:

```bash
aws dynamodb scan --endpoint-url http://localhost:4566 --region us-east-1 \
  --table-name f2e-job-ledger --output json | jq '.Items'
```

Procure chunks com status `COMPLETED`. Status `FAILED` ou mensagens nas DLQs
devem ser investigados nos logs:

```bash
docker compose -f automacao/docker-compose.yml logs --tail 200 localstack
docker compose -f automacao/docker-compose.yml logs localstack | rg 'organizer|worker|error'
```

## Testes

Com o LocalStack ativo:

```bash
cd e2e
go test -v -timeout 25m ./...

# Executar somente cenários com uma tag
E2E_TAGS='@binary' go test -v -timeout 2m ./...
```

Testes unitários não precisam do LocalStack:

```bash
go test ./...
(cd lambdas && go test ./...)
```

## Diagnóstico rápido

- **Docker não está disponível:** inicie o Docker Desktop.
- **Provisionamento expirou:** execute
  `docker compose -f automacao/docker-compose.yml logs --tail 100 localstack`.
- **Mensagens em DLQ:** leia a fila, confira os logs e corrija a causa antes de
  limpá-la.
- **Contagem incompleta:** espere `chunk-jobs` esvaziar e confira
  `chunk-jobs-dlq`.
- **Erro de credenciais:** exporte as credenciais de teste apresentadas acima.

## Encerrar o ambiente

```bash
cd automacao
./parar-ambiente.sh
```

A próxima execução de `./subir-ambiente.sh` recria todos os recursos locais.
