# F2E — File-to-Events

O F2E é um framework em Go que transforma arquivos armazenados no Amazon S3 em eventos individuais no Amazon SQS. Ele conecta integrações baseadas em arquivos a consumidores orientados a eventos, com processamento paralelo, rastreabilidade por registro e mecanismos de recuperação.

> Este repositório é uma prova de conceito. A arquitetura e a infraestrutura estão prontas para validação local e evolução, mas ainda não foram dimensionadas ou homologadas para produção. Consulte [Uso em produção](#uso-em-produção).

## Como funciona

1. Um arquivo é enviado a um prefixo configurado do S3, que gera uma notificação, ou um sistema publica uma requisição explícita na fila de intake.
2. Para notificações S3, a Lambda **Organizer** resolve no Parameter Store a configuração de `bucket + prefixo`, obtém os metadados do objeto, planeja faixas de bytes (*chunks*) e registra o plano no ledger DynamoDB.
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

O F2E suporta três modos de delimitação:

| Modo | Registro lógico |
|---|---|
| `text` | Uma linha física terminada por CR, LF ou CRLF. |
| `json` | Um elemento do array selecionado em `arrayPath`, com parsing estrutural. |
| `multi-line` | Linhas físicas agrupadas por marcadores configurados. |

Tipos removidos (`fixed-width`, `jsonl`, `ndjson`, `csv`, `binary`) produzem erro explícito. Para informações sobre migração, consulte [contratos](documentacao/contratos.md) e o [ADR 0005](documentacao/adr/0005-tres-modos-de-delimitacao.md).

Para `text` com arquivos grandes (mais de `F2E_RECORDS_PER_CHUNK` linhas), informe `maxRecordLengthBytes`. Esse limite permite que o Organizer defina as fronteiras dos chunks e que os Workers tratem registros que cruzam uma fronteira sem gerar duplicatas ou lacunas. Arquivos pequenos podem omitir `maxRecordLengthBytes` (modo single-chunk).

### Configuração por prefixo no SSM

Cada prefixo de entrada possui um parâmetro `String` no AWS Systems Manager
Parameter Store. O nome segue o padrão
`/f2e/<ambiente>/file-config/<bucket>/<identificador-do-prefixo>`. O Organizer
lista as configurações do bucket e usa a correspondência de prefixo mais longa.
Alterações no valor passam a valer para novos eventos sem reconstruir as
Lambdas.

O Terraform cria estes exemplos no bucket do ambiente:

| Prefixo S3 | Modo |
|---|---|
| `example-text/` | `text` |
| `example-json/` | `json` (array) |
| `example-multi-line/` | `multi-line` |

O valor de cada parâmetro é um JSON completo. Exemplo para texto:

```json
{
  "bucket": "<bucket-criado>",
  "prefix": "example-text/",
  "dataType": "text",
  "recordsPerChunk": 1000,
  "batchSize": 10,
  "maxEventBytes": 262144,
  "maxFileBytes": 10737418240,
  "maxChunkBytes": 67108864,
  "jsonArraySearchBytes": 1048576,
  "maxRecordLengthBytes": 65536,
  "eventSchemaId": "f2e-record",
  "eventSchemaVersion": "1",
  "eventFormat": "json"
}
```

| Propriedade | Uso |
|---|---|
| `bucket` e `prefix` | Chave de seleção da configuração. O prefixo é relativo ao bucket e termina em `/`. |
| `dataType` | `text`, `json` ou `multi-line`. |
| `recordsPerChunk` | Granularidade do planejamento; valores menores geram mais chunks e disponibilizam mais paralelismo. |
| `batchSize` | Quantidade de eventos enviada por chamada `SendMessageBatch`, entre 1 e 10. |
| `maxEventBytes` | Limite serializado de cada evento, entre 1 KiB e 256 KiB. |
| `maxFileBytes` | Maior arquivo aceito pelo prefixo. |
| `maxChunkBytes` | Maior faixa de bytes entregue a um Worker. |
| `jsonArraySearchBytes` | Janela usada para localizar o array configurado em arquivos `json`. |
| `maxRecordLengthBytes` | Limite de linha para `text`; use `0` quando não se aplica. |
| `eventSchemaId`, `eventSchemaVersion`, `eventFormat` | Identificação do contrato dos eventos de saída. |
| `jsonArrayLayout` | Para `json`: contém `arrayPath` e `maxBytesPerElement`. |
| `multiLineLayout` | Para `multi-line`: contém marcadores, separador e `maxBytesPerRecord`. |

`jsonArrayLayout` é obrigatório para `json`; `multiLineLayout` é obrigatório
para `multi-line`. `recordsPerChunk` controla a granularidade e o paralelismo
disponível para um arquivo. `worker_maximum_concurrency` permanece um limite
global da infraestrutura, compartilhado por todos os prefixos.

### Limites globais

O Terraform também cria `/f2e/<ambiente>/global-limits`. O Organizer carrega
esse parâmetro no cold start; se estiver ausente ou inválido, a Lambda falha na
inicialização. Configurações de prefixo que excedam qualquer teto global são
rejeitadas antes do planejamento.

| Limite comum | Valor inicial |
|---|---:|
| Arquivo | 10 GiB |
| Chunk | 64 MiB |
| Evento SQS | 256 KiB |
| Lote de publicação | 10 eventos |
| Busca pelo array JSON | 16 MiB |

| `dataType` | Arquivo máximo | Registro/elemento máximo |
|---|---:|---:|
| `text` | 10 GiB | 258.048 bytes |
| `json` | 10 GiB | 258.048 bytes |
| `multi-line` | 10 GiB | 258.048 bytes |

O teto de registro reserva aproximadamente 4 KiB para o envelope. A validação final considera o
evento serializado e seus atributos; conteúdo com muito escape JSON
pode atingir o limite de 256 KiB antes do tamanho nominal acima.

```json
{
  "maxFileBytes": 10737418240,
  "maxChunkBytes": 67108864,
  "maxEventBytes": 262144,
  "maxBatchSize": 10,
  "maxJsonArraySearchBytes": 16777216,
  "inputTypes": {
    "text":        { "maxFileBytes": 10737418240, "maxRecordBytes": 258048 },
    "json":        { "maxFileBytes": 10737418240, "maxRecordBytes": 258048 },
    "multi-line":  { "maxFileBytes": 10737418240, "maxRecordBytes": 258048 }
  }
}
```

A decisão formal está no [ADR 0005](documentacao/adr/0005-tres-modos-de-delimitacao.md) e nos [contratos](documentacao/contratos.md).

## Evento de saída e rastreabilidade

Cada registro é publicado como um Envelope v2. Ele preserva a origem do arquivo, a posição do registro e o contexto de correlação recebido na entrada.

```json
{
  "schemaVersion": "2",
  "schema": "record:1",
  "format": "text",
  "eventId": "<id estável durante retries>",
  "sourceRecordId": "<id imutável do registro físico>",
  "origin": { "bucket": "f2e-input", "key": "entrada/registros.txt" },
  "job": { "jobId": "…", "chunkId": "00000001", "recordNumber": 42 },
  "data": { "raw": "2026-09-05;PAGAMENTO;00001;100.00" }
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

As variáveis de ambiente abaixo fornecem os padrões da infraestrutura e o
fallback do contrato explícito. Eventos S3 usam os valores do parâmetro SSM do
prefixo e carregam a configuração selecionada em cada `ChunkJob`.

| Variável | Padrão | Finalidade |
|---|---:|---|
| `F2E_RECORDS_PER_CHUNK` | `1000` | Quantidade nominal de registros por chunk. |
| `F2E_BATCH_SIZE` | `10` | Eventos publicados por lote SQS. |
| `F2E_MAX_FILE_BYTES` | `10 GiB` | Tamanho máximo aceito por arquivo. |
| `F2E_MAX_CHUNK_BYTES` | `64 MiB` | Tamanho máximo de um chunk. |
| `F2E_MAX_EVENT_BYTES` | `256 KiB` | Limite do body e atributos da mensagem SQS. |
| `F2E_FILE_CONFIG_PATH` | `/f2e/<ambiente>/file-config` | Raiz das configurações de prefixos no SSM. |

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
