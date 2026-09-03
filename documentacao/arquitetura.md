# F2E — Arquitetura da Solução

## 1. Visão Geral

O **F2E (File-to-Events)** é um framework em Go para transformar arquivos potencialmente grandes armazenados no Amazon S3 em eventos individuais publicados no Amazon SQS. Atua como ponte entre integrações orientadas a batch e arquiteturas event-driven, eliminando a necessidade de processadores ECS dedicados e ociosos.

Dois modos de entrada são suportados:

- **Notificação S3**: o bucket dispara `ObjectCreated` → SQS; o Organizer trata como arquivo `fixed-width` com opções padrão.
- **`OrganizerRequest` explícito**: sistema externo publica contrato versionado na fila, declarando o formato e as opções do arquivo.

---

## 2. Infraestrutura AWS

### Amazon S3

| Recurso | Configuração |
|---|---|
| Bucket de entrada | Versionamento habilitado, SSE-S3 ou SSE-KMS, acesso público bloqueado |
| Lifecycle | Expiração automática dos objetos (padrão: 15 dias) |
| Notificação | `ObjectCreated:*` → SQS `file-intake` |

### Amazon SQS

| Fila | Propósito | DLQ |
|---|---|---|
| `file-intake` | Recebe notificações S3 ou `OrganizerRequest`; aciona o Organizer | `file-intake-dlq` |
| `chunk-jobs` | Recebe `ChunkJob` publicados pelo Organizer; aciona o Worker | `chunk-jobs-dlq` |
| `output-events` | Recebe `OutputEvent` (Envelope v2) publicados pelo Worker; consumido pelo downstream | — |

Mensagens inválidas são re-enfileiradas até `maxReceiveCount` e então movidas para a DLQ. CloudWatch alarms monitoram mensagens visíveis nas DLQs e a idade da mensagem mais antiga nas filas principais.

### AWS Lambda

| Função | Trigger | Responsabilidade |
|---|---|---|
| `organizer` | `file-intake` SQS | Lê metadados do arquivo (HEAD S3), planeja chunk boundaries, registra o `JobPlan` no ledger e publica `ChunkJob` em `chunk-jobs` |
| `worker` | `chunk-jobs` SQS | Faz Range GET no S3, parseia os registros do chunk, envolve cada um em um Envelope v2 e publica em lotes de 10 em `output-events` |

Ambas as funções:
- Runtime: `provided.al2` (binário Go estático, arquitetura `arm64` ou `x86_64`)
- Falhas parciais: `BatchItemFailures` — apenas mensagens com erro são devolvidas para reprocessamento
- X-Ray tracing ativo em produção
- Logs JSON estruturados no CloudWatch

### Amazon DynamoDB

Tabela `job-ledger` (chave composta `pk` + `sk`, billing PAY_PER_REQUEST):

| Fase | Transição de status |
|---|---|
| Organizer planeja | `PENDING` |
| Organizer publica na fila | `SCHEDULED` (ou `SCHEDULING_FAILED`) |
| Worker inicia | `RUNNING` |
| Worker finaliza | `COMPLETED` ou `FAILED` |

- TTL configurável para limpeza automática
- Suporta reconciliação e replay por arquivo ou job
- PITR habilitado em produção

### Amazon CloudWatch

- Log groups com retenção configurável (padrão: 30 dias)
- Alarmes: mensagens visíveis em DLQs, backlog age nas filas, erros nas funções Lambda

---

## 3. Fluxo de Dados

```
1. Arquivo chega no S3
       │
       ▼
2a. Notificação ObjectCreated → SQS file-intake
    OU
2b. Sistema externo envia OrganizerRequest → SQS file-intake
       │
       ▼
3. Lambda Organizer é invocada
   ├── HEAD no objeto S3 (obtém Size, ETag, VersionId)
   ├── Planeja N faixas de byte (chunks) conforme formato e configurações
   ├── Registra JobPlan no DynamoDB (ledger)
   └── Publica N ChunkJob messages → SQS chunk-jobs
       │
       ▼ (N invocações paralelas)
4. Lambda Worker é invocada por chunk
   ├── Range GET no S3 (valida ETag/VersionId — imutabilidade garantida)
   ├── Parseia os registros do chunk conforme o formato declarado
   ├── Envolve cada registro em um Envelope v2 (eventId estável, sourceRecordId imutável)
   ├── Publica em lotes de 10 → SQS output-events
   └── Registra ChunkResult no DynamoDB
       │
       ▼
5. Consumidores downstream recebem eventos via SQS output-events
```

---

## 4. Formatos Suportados

| Formato | Disponibilidade | Observação |
|---|---|---|
| `fixed-width` | Produção | Registros de comprimento fixo + LF |
| `jsonl` | Produção | Uma linha = um objeto JSON |
| `ndjson` | Produção | Equivalente ao `jsonl` |
| `text` | Produção | Linhas de texto arbitrário |
| `csv` | Preview | Requer `F2E_ENABLE_PREVIEW_FORMATS=true`; RFC 4180, sem remoção de header |
| `json` (array) | Preview | Requer `F2E_ENABLE_PREVIEW_FORMATS=true`; parser ciente de nesting |
| `binary` | Experimental | Requer `F2E_ENABLE_EXPERIMENTAL_FORMATS=true`; não divisível |
| `multi-line` | Experimental | Requer `F2E_ENABLE_EXPERIMENTAL_FORMATS=true`; registros com múltiplas linhas físicas |

Para formatos de registros variáveis delimitados por LF (`jsonl`, `ndjson`, `text`), o Organizer usa `MaxRecordLengthBytes` para calcular as faixas nominais. Cada chunk recebe uma "gordura" (`trailingPaddingBytes`) para cobrir registros que cruzam fronteiras; o Worker descarta fragmentos fora da sua faixa nominal, garantindo exatamente um evento por registro sem duplicação nem lacuna.

---

## 5. Envelope v2

Cada evento publicado em `output-events` segue este contrato:

```json
{
  "schemaVersion": "2",
  "schema": "record:1",
  "format": "jsonl",
  "eventId": "<uuid — estável entre retries do mesmo job>",
  "sourceRecordId": "<uuid — estável para o mesmo registro físico>",
  "origin": {
    "bucket": "f2e-input",
    "key": "path/arquivo.jsonl",
    "etag": "\"abc123\"",
    "versionId": "v1",
    "fileSize": 102400
  },
  "job": {
    "jobId": "...",
    "chunkId": "00000001",
    "recordNumber": 42,
    "startByte": 1000,
    "endByte": 1050
  },
  "data": {
    "raw": "{\"campo\": \"valor\"}"
  },
  "transactionId": "...",
  "correlationId": "...",
  "traceId": "...",
  "sourceSystem": "..."
}
```

- `eventId`: muda em replay explícito; consumidores que deduplicam o job usam este campo.
- `sourceRecordId`: imutável para o mesmo registro físico; consumidores que deduplicam o dado físico usam este campo.
- `transactionId`, `correlationId`, `traceId`, `sourceSystem`: atravessam Organizer e Worker sem alteração.
- Message Attributes publicados: `schema` (ex.: `record:1`) e `format` (ex.: `jsonl`).

O JSON Schema normativo está em `documentacao/schemas/envelope-v2.schema.json`.

---

## 6. Arquitetura do Código Go

O repositório contém dois módulos Go conectados por `go.work`:

### Módulo raiz — Framework (`github.com/f2e/f2e`)

```
internal/
├── domain/
│   ├── f2e/              # Contratos centrais: ChunkJob, Envelope, OrganizerRequest, JobLedger types
│   └── fixedwidth/       # Parser de arquivos de largura fixa (MVP)
├── application/
│   ├── organizer/        # Caso de uso: planejamento e publicação de chunks
│   ├── worker/           # Caso de uso: leitura de chunk, parsing e publicação de eventos
│   └── port/             # Interfaces (ObjectStore, Queue, JobLedger, SourceResolver, RecordProcessor)
├── adapter/
│   └── inbound/s3event/  # Decodifica notificação S3 → []FileReference
└── platform/
    ├── aws/              # Implementações concretas: S3 (Head + RangeGet), SQS, DynamoDB
    └── config/           # Leitura e validação de variáveis de ambiente
```

### Módulo de execução — Lambdas (`github.com/f2e/f2e/lambdas`)

```
lambdas/
└── cmd/
    ├── organizer/main.go  # Bootstrap: inicializa clientes AWS, compõe Service, registra handler
    └── worker/main.go     # Bootstrap: inicializa clientes AWS, compõe Service, registra handler
```

### Regras de dependência

```
lambdas/cmd ──► application ──► port ◄── platform/aws
                    │
                    ▼
                domain/f2e

adapter/inbound/s3event ──► domain/f2e
platform/config (independente)
```

`application` nunca importa AWS SDK — depende apenas das interfaces em `port`. `lambdas/cmd` é o único código que importa `aws-lambda-go`. `domain` não depende de nada externo ao módulo.

---

## 7. Extensibilidade

A interface `RecordProcessor` permite que a aplicação hospedeira injete lógica antes da publicação:

```go
type RecordProcessor interface {
    Process(ctx context.Context, envelope Envelope[RecordPayload]) (*Envelope[RecordPayload], error)
}
```

Retornar `nil` descarta o registro silenciosamente. Extensões não podem remover `eventId`, `sourceRecordId` nem a localização técnica.

---

## 8. Desenvolvimento Local

```bash
# Sobe LocalStack + builda e implanta as Lambdas
cd automacao && ./subir-ambiente.sh

# Executa fluxo completo com dados de exemplo (resultado esperado: 0 erros, 0 DLQ)
./executar-fluxo.sh

# Testes unitários
go test ./...              # módulo raiz
cd lambdas && go test ./... # módulo lambdas

# Testes E2E (requer ambiente ativo)
cd e2e && go test -v -timeout 25m ./...
```

---

## 9. Infraestrutura como Código

Provisionamento via Terraform (`terraform/`):

| Arquivo | Recursos |
|---|---|
| `s3.tf` | Bucket, políticas TLS, versionamento, lifecycle |
| `sqs.tf` | Filas principais, DLQs, política S3→SQS |
| `lambda.tf` | Funções, aliases `live`, event source mappings |
| `dynamodb.tf` | Tabela `job-ledger` com TTL e PITR |
| `iam.tf` | Roles e políticas mínimas para Organizer e Worker |
| `monitoring.tf` | Alarmes CloudWatch para DLQs, backlog age, erros Lambda |
| `variables.tf` / `locals.tf` | Parâmetros por ambiente (`local`, `staging`, `production`) |

Ambientes definidos em `terraform/environments/`: `local.tfvars`, `staging.tfvars`, `production.tfvars`.
