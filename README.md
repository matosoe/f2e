# F2E — File-to-Events

O F2E é um framework em Go que transforma arquivos armazenados no Amazon S3 em eventos individuais no Amazon SQS. Ele conecta integrações baseadas em arquivos a consumidores orientados a eventos, com processamento paralelo, rastreabilidade por registro e mecanismos de recuperação.

> Este repositório é uma prova de conceito. A arquitetura e a infraestrutura estão prontas para validação local e evolução, mas ainda não foram dimensionadas ou homologadas para produção. Consulte [Uso em produção](#uso-em-produção).

## Como funciona

1. Um arquivo é enviado ao S3, que gera uma notificação, ou um sistema publica uma requisição explícita na fila de intake.
2. A Lambda **Organizer** obtém os metadados do objeto, planeja faixas de bytes (*chunks*) e registra o plano no ledger DynamoDB.
3. A Lambda **Worker** processa os chunks em paralelo, lê o objeto com S3 Range GET e publica um evento por registro na fila de saída.
4. Os consumidores recebem eventos no formato Envelope v2; o ledger registra o resultado de cada chunk para apoiar auditoria, reconciliação e replay.

```text
S3 ObjectCreated ou OrganizerRequest
                 │
                 ▼
          SQS file-intake
                 │
                 ▼
          Lambda Organizer ──► DynamoDB job-ledger
                 │
                 ▼
            SQS chunk-jobs
                 │
                 ▼
           Lambda Worker ──► SQS output-events ──► consumidores
```

As falhas de processamento são tratadas por reprocessamento seletivo de mensagens SQS; após o limite de tentativas, seguem para as respectivas DLQs. As Lambdas retornam falhas parciais de lote, evitando repetir mensagens já concluídas.

## Formatos de entrada

| Formato | Status | Características e condições |
|---|---|---|
| `fixed-width` | Produção | Registros de tamanho fixo terminados em LF. |
| `text` | Produção | Um evento por linha de texto. |
| `jsonl` / `ndjson` | Produção | Um valor JSON por linha; a validação pode ser desabilitada por arquivo. |
| `csv` | Produção | Parser RFC 4180, incluindo campos entre aspas e quebras de linha. Header e BOM não são removidos. |
| `json` (array) | Produção | Um evento por elemento do array, inclusive para arrays aninhados configurados por caminho. |
| `binary` | Produção | Arquivo não divisível, publicado como payload Base64 e limitado ao tamanho máximo do evento. |
| `multi-line` | Produção | Registros com múltiplas linhas físicas, definidos por layout de marcadores. |

Para `text`, `jsonl` e `ndjson`, informe `maxRecordLengthBytes`. Esse limite permite que o Organizer defina as fronteiras dos chunks e que os Workers tratem registros que cruzam uma fronteira sem gerar duplicatas ou lacunas.

A decisão formal e os limites de cada formato estão no [ADR 0004](documentacao/adr/0004-formatos-suportados.md) e nos [contratos](documentacao/contratos.md).

## Evento de saída e rastreabilidade

Cada registro é publicado como um Envelope v2. Ele preserva a origem do arquivo, a posição do registro e o contexto de correlação recebido na entrada.

```json
{
  "schemaVersion": "2",
  "schema": "record:1",
  "format": "ndjson",
  "eventId": "<id estável durante retries>",
  "sourceRecordId": "<id imutável do registro físico>",
  "origin": { "bucket": "f2e-input", "key": "entrada/eventos.ndjson" },
  "job": { "jobId": "…", "chunkId": "00000001", "recordNumber": 42 },
  "data": { "raw": "{\"campo\":\"valor\"}" }
}
```

- Use `eventId` para deduplicar tentativas de um mesmo job. Ele muda em um replay explícito.
- Use `sourceRecordId` para deduplicar o mesmo registro físico, inclusive entre replays.
- `transactionId`, `correlationId`, `traceId` e `sourceSystem`, quando informados, são propagados até o evento final.

O [JSON Schema normativo](documentacao/schemas/envelope-v2.schema.json) e o [contrato completo](documentacao/contratos.md) definem o payload.

## Início rápido — ambiente local

### Pré-requisitos

- Git Bash
- Go 1.26 ou superior
- Docker
- AWS CLI v2
- `zip` e `jq`

O ambiente local usa LocalStack para S3, SQS, Lambda, DynamoDB, IAM e CloudWatch Logs.

```bash
cd automacao
./executar-fluxo.sh
```

O script compila e provisiona o ambiente, gera uma massa de dados, executa o fluxo ponta a ponta e valida a saída. Para a massa padrão, o resultado esperado é `20.000/20.000 — nenhuma lacuna, nenhuma DLQ e nenhum erro.`

Comandos úteis:

```bash
# Subir ou reconstruir o ambiente sem executar a massa
cd automacao && ./subir-ambiente.sh

# Executar a massa e validar novamente
cd automacao && ./executar-fluxo.sh

# Parar o ambiente
cd automacao && ./parar-ambiente.sh

# Testes unitários (a partir da raiz)
go test ./...
(cd lambdas && go test ./...)

# Testes end-to-end, com o ambiente ativo
(cd e2e && go test -v -timeout 25m ./...)
```

Veja instruções operacionais e limitações do simulador em [Operação local](documentacao/operacao_local.md).
Para uma conta AWS real, consulte [Operação na AWS](documentacao/operacao_aws.md).

## Organização do repositório

| Diretório | Conteúdo |
|---|---|
| `internal/domain` | Contratos centrais e parsers de registros. |
| `internal/application` | Casos de uso do Organizer e Worker, independentes do AWS SDK. |
| `internal/adapter` | Adaptadores de entrada, como notificações S3. |
| `internal/platform` | Integrações AWS e carregamento de configuração. |
| `lambdas/` | Binários e bootstrap das Lambdas Organizer e Worker. |
| `terraform/` | Infraestrutura como código para ambientes local, staging e produção. |
| `automacao/` | LocalStack, scripts de build, execução e validação local. |
| `e2e/` | Cenários de integração ponta a ponta. |
| `documentacao/` | Arquitetura, contratos, ADRs, runbooks e decisões técnicas. |

O repositório possui dois módulos Go coordenados por `go.work`: o framework na raiz e as funções em `lambdas/`.

## Configuração e limites

Os parâmetros são definidos por variáveis de ambiente. Os principais são:

| Variável | Padrão | Finalidade |
|---|---:|---|
| `F2E_RECORD_LENGTH` | `100` | Tamanho do registro `fixed-width`, incluindo LF. |
| `F2E_RECORDS_PER_CHUNK` | `1000` | Quantidade nominal de registros por chunk. |
| `F2E_BATCH_SIZE` | `10` | Eventos publicados por lote SQS. |
| `F2E_MAX_FILE_BYTES` | `10 GiB` | Tamanho máximo aceito por arquivo. |
| `F2E_MAX_CHUNK_BYTES` | `64 MiB` | Tamanho máximo de um chunk. |
| `F2E_MAX_EVENT_BYTES` | `256 KiB` | Limite do body e atributos da mensagem SQS. |

Todos os limites e opções por arquivo estão documentados em [Contratos versionados](documentacao/contratos.md).

## Documentação

- [Arquitetura da solução](documentacao/arquitetura.md)
- [Contratos versionados](documentacao/contratos.md)
- [Especificação do Envelope de Eventos](documentacao/Especificação%20—%20Envelope%20de%20Eventos%20do%20F2E.md)
- [Operação local](documentacao/operacao_local.md)
- [Operação na AWS](documentacao/operacao_aws.md)
- [Runbooks operacionais](documentacao/runbooks.md)
- [ADRs](documentacao/adr/)
- [Infraestrutura por ambiente](terraform/environments/README.md)

## Uso em produção

Antes de implantar, valide carga e concorrência com arquivos reais, defina SLOs, alarmes e capacidade. Também é necessário aprovar contas, regiões, rede, IAM, KMS, classificação de dados, retenções e estratégia de recuperação. O F2E preserva a posição de cada registro na origem, mas o processamento distribuído não oferece ordenação global dos eventos de saída. Para o checklist completo, veja o [plano de adequação corporativa](documentacao/plano_adequacao_corporativa_f2e.md).

## Licença

Consulte [LICENSE](LICENSE).
