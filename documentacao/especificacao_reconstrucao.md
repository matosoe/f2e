# Especificação de reconstrução independente — File-to-Events (F2E)

## 1. Finalidade deste documento

Este documento é a especificação de produto e de engenharia para reconstruir,
em um repositório novo e sem acesso ao código-fonte de referência, um building
block semelhante ao F2E. O sistema recebe arquivos no Amazon S3, converte cada
registro lógico em evento e publica os eventos no Amazon SQS.

O agente implementador deve produzir uma implementação nova a partir dos
comportamentos e critérios de aceite descritos aqui. Não deve clonar, importar,
copiar ou depender do repositório que originou esta especificação. Nomes
internos, organização de pacotes e detalhes de implementação podem variar,
desde que os contratos, garantias e testes obrigatórios sejam atendidos.

Neste documento:

- **DEVE** indica requisito obrigatório.
- **NÃO DEVE** indica proibição necessária para preservar o contrato.
- **DEVERIA** indica recomendação que pode ser alterada com justificativa em ADR.
- Blocos intitulados “orientação ao agente” fazem parte do processo de entrega;
  exemplos JSON e diagramas descrevem o sistema, não são comandos externos.

## 2. Resultado esperado

O projeto final DEVE entregar:

1. duas funções AWS Lambda escritas em Go: **Organizer** e **Worker**;
2. infraestrutura reproduzível com Terraform para AWS e configuração de
   desenvolvimento local com LocalStack;
3. contratos JSON versionados para entrada, trabalho interno, saída por
   registro, saída em bundle e conclusão do job;
4. ledger técnico em DynamoDB com admissão idempotente, manifesto de chunks,
   checkpoints, estados, contadores e outbox de conclusão;
5. configuração global e por prefixo no SSM Parameter Store;
6. testes unitários, de integração e ponta a ponta;
7. scripts idempotentes de build, provisionamento, execução, validação e
   desmontagem do ambiente local;
8. documentação de arquitetura, contratos, operação, recuperação e decisões.

O fluxo mínimo é:

```text
S3 ObjectCreated ou OrganizerRequest explícito
                         |
                         v
                  SQS file-intake
                         |
                         v
                  Lambda Organizer ----> SSM (configuração)
                         |               DynamoDB (ledger)
                         v
                  SQS chunk-jobs
                         |
                         v
                   Lambda Worker ------> S3 Range GET
                         |               DynamoDB (checkpoint/outbox)
                         +-------------> SQS output-events
                         +-------------> SQS completion-events
```

## 3. Escopo e não escopo

### 3.1 Escopo obrigatório

- Fluxo inbound File-to-Event.
- Entrada normal por notificação `ObjectCreated:*` do S3 encaminhada ao SQS.
- Entrada alternativa por `OrganizerRequest` explícito publicado no intake.
- Arquivos S3 versionados e não versionados.
- Três modos de delimitação: `text`, `json` e `multi-line`.
- Processamento paralelo por faixas de bytes.
- Entrega SQS Standard com semântica at-least-once.
- Saída padrão compartilhada e saída dedicada opcional por prefixo.
- Modos de publicação `single` e `bundle`.
- Evento técnico de conclusão separado dos eventos de registros.
- Recuperação por retry, DLQ, checkpoints e replay explícito.

### 3.2 Fora de escopo

- Event-to-File.
- SNS como destino de registros.
- Ordenação global ou exactly-once.
- Transformação de negócio do conteúdo.
- Persistência de negócio dos consumidores.
- Parsing nativo de CSV, XML, JSONL/NDJSON, fixed-width ou binário.
- Garantia de que consumidores já processaram os registros quando o evento de
  conclusão for publicado.

Tipos fora do escopo DEVEM falhar explicitamente como `unsupported data type`;
não devem ser aceitos como aliases silenciosos.

## 4. Stack e organização sugeridas

### 4.1 Tecnologias

- Go 1.26 ou superior.
- AWS Lambda custom runtime `provided.al2023`.
- AWS SDK for Go v2.
- Terraform com provider AWS.
- LocalStack para S3, SQS, Lambda, DynamoDB, IAM, SSM e CloudWatch Logs.
- AWS CLI v2, Docker, `zip` e `jq` nos scripts locais.

A implementação DEVE manter regras de domínio e casos de uso independentes do
SDK AWS por meio de interfaces. Um arranjo de diretórios recomendado é:

```text
cmd/
  organizer/               entrada da Lambda Organizer
  worker/                  entrada da Lambda Worker
internal/
  domain/                  contratos, identidades, estados e parsers
  application/
    organizer/             admissão, validação, planejamento e agendamento
    worker/                leitura, parsing, publicação e conclusão
    port/                  interfaces de storage, filas, ledger e configuração
  adapter/inbound/         parser de notificações S3
  platform/
    aws/                   implementações S3, SQS, DynamoDB e SSM
    config/                variáveis de ambiente e validação
terraform/                 infraestrutura
automacao/                 scripts e LocalStack
e2e/                       módulo e cenários ponta a ponta
documentacao/              contratos, arquitetura, ADRs e runbooks
```

O módulo E2E DEVERIA ser separado do módulo principal para não levar suas
dependências ao binário das Lambdas.

## 5. Componentes e responsabilidades

### 5.1 Organizer

O Organizer DEVE:

1. consumir lotes do intake e retornar falhas parciais no formato
   `ReportBatchItemFailures`;
2. distinguir notificação S3 de `OrganizerRequest` explícito;
3. aceitar múltiplos `Records` em uma notificação, processar somente
   `ObjectCreated:*`, decodificar a chave URL-encoded e ignorar eventos de teste;
4. para eventos S3, resolver no SSM a configuração de `bucket + prefix` com a
   correspondência de prefixo mais longa;
5. carregar e validar limites globais no cold start; configuração ausente ou
   inválida deve impedir a inicialização;
6. obter `VersionId`, `ETag` e tamanho do objeto por `HeadObject`;
7. fixar a identidade física, admitir o job de forma idempotente e persistir o
   snapshot exato da configuração;
8. validar formato, layouts, limites de arquivo, registro, chunk e mensagem;
9. criar e selar um manifesto imutável de chunks;
10. publicar `ChunkJob`s, registrando checkpoint individual somente depois da
    confirmação do SQS;
11. em retry, republicar apenas chunks do manifesto sem checkpoint;
12. para um plano válido sem chunks, como array JSON vazio, publicar uma
    mensagem de controle para o Worker reconciliar a conclusão.

O Organizer NÃO DEVE ler o arquivo inteiro para planejar `text` ou `json`.

### 5.2 Worker

O Worker DEVE:

1. consumir `chunk-jobs` com batch de uma mensagem e falhas parciais;
2. adquirir o chunk no ledger por escrita condicional antes de publicar dados;
3. reconhecer entrega de chunk já concluído e confirmá-la sem republicar;
4. ler somente a faixa necessária com S3 Range GET;
5. usar `VersionId` quando presente; sem versão, usar leitura condicional
   `If-Match` com o `ETag` admitido;
6. resolver fronteiras, extrair registros e preservar offsets físicos;
7. criar Envelope v1 determinístico para cada registro;
8. validar o tamanho serializado do corpo e dos atributos antes do envio;
9. publicar em lotes de até 10 mensagens e até 1 MiB total por chamada;
10. permitir de 1 a 16 chamadas `SendMessageBatch` simultâneas por invocação;
11. persistir contadores e conclusão do chunk de maneira idempotente;
12. ao tornar o job terminal, reconciliar a outbox, publicar o evento de
    conclusão e só depois marcar a intenção como entregue;
13. interromper publicação, registrar falha incompleta e propagar erro quando
    o contexto da Lambda expirar.

### 5.3 Consumidor downstream

O consumidor, fora deste projeto, é responsável por excluir mensagens, tratar
sua própria DLQ e aplicar efeitos idempotentes. Deve usar `eventId` para
deduplicar retries da mesma execução e `sourceRecordId` para deduplicar o mesmo
registro físico entre replays.

## 6. Identidades e garantias

Todas as funções hash abaixo usam SHA-256 em hexadecimal minúsculo. Separadores
fazem parte da entrada do hash.

| Identidade | Regra |
|---|---|
| `fileId` | `sha256(bucket + "/" + key + "/" + versionId + "/" + etag + "/" + sizeDecimal)` |
| `receiptId` | Identifica uma ocorrência de intake; redelivery da mesma mensagem SQS reutiliza o ID. |
| `jobId` | Estável no retry da mesma ocorrência e novo em replay explícito. |
| `chunkId` | Determinístico dentro do manifesto do job; sugestão: oito dígitos com zero à esquerda. |
| `sourceRecordId` | `sha256(fileId + "/" + byteOffsetDecimal)`; independe de chunk, schema e replay. |
| `eventId` | Determinístico para job + registro; estável em retries e diferente em replay. |
| `completion eventId` | `sha256(jobId + "/completion/" + intentVersionDecimal)`. |
| `bundleId` | SHA-256 da lista ordenada de `eventId`s contidos no bundle. |

A implementação DEVE documentar a fórmula exata de `jobId`, `chunkId` e
`eventId` escolhida e cobri-la com testes de estabilidade.

Garantias externas:

- entrega at-least-once, portanto duplicatas são possíveis;
- nenhuma ordenação entre chunks ou mensagens SQS Standard;
- um arquivo físico normalmente possui um job, mas replay cria novo job;
- snapshot de configuração e plano não mudam após a admissão;
- não há transação distribuída entre SQS e DynamoDB;
- nenhum dado pode ser truncado para caber em mensagem.

## 7. Contratos JSON

Todos os contratos internos de primeira versão usam `schemaVersion: "1"`,
exceto wrappers que possuem identificador próprio indicado abaixo. Campos
desconhecidos deveriam ser ignorados por leitores para permitir evolução
compatível.

### 7.1 OrganizerRequest explícito

```json
{
  "schemaVersion": "1",
  "files": [
    {
      "bucket": "meu-bucket",
      "key": "entrada/registros.txt",
      "versionId": "opcional",
      "etag": "opcional",
      "presignedUrl": "opcional",
      "dataType": "text",
      "maxRecordLengthBytes": 65536,
      "context": {
        "transactionId": "opcional",
        "correlationId": "opcional",
        "traceId": "opcional",
        "sourceSystem": "opcional"
      }
    }
  ]
}
```

`executionId` é um campo interno opcional preenchido pelo handler a partir da
mensagem SQS. Requisições explícitas usam seus campos e os defaults da Lambda;
não fazem seleção de prefixo no SSM.

### 7.2 Configuração de prefixo no SSM

Nome: `/f2e/<ambiente>/file-config/<bucket>/<prefixId>`.

```json
{
  "bucket": "meu-bucket",
  "prefix": "example-text/",
  "prefixId": "example-text",
  "dataType": "text",
  "recordsPerChunk": 1000,
  "batchSize": 10,
  "maxEventBytes": 1044480,
  "maxFileBytes": 10737418240,
  "maxChunkBytes": 67108864,
  "targetChunkBytes": 0,
  "maxRecordLengthBytes": 65536,
  "eventSchemaId": "f2e-record",
  "eventSchemaVersion": "1",
  "eventFormat": "json",
  "outputMode": "single",
  "maxEnvelopesPerMessage": 0,
  "maxMessageBytes": 0,
  "outputQueueURL": "https://sqs.../output-events",
  "chunkQueueURL": "https://sqs.../chunk-jobs",
  "responsible": "equipe-x"
}
```

`prefix` DEVE terminar em `/`. `prefixId` deve conter apenas letras, números,
`-` e `_`. A URL de saída vazia seleciona a fila padrão. `allowedSourceARNs`, se
implementado no documento, é apenas metadado até que políticas IAM/Terraform
façam a autorização real; a aplicação não deve fingir que o validou.

O snapshot persistido DEVE conter a configuração completa, hash SHA-256 do JSON
canônico, nome e versão do parâmetro, responsável, instante de carregamento e o
documento/versão dos limites globais.

### 7.3 Limites globais no SSM

Nome: `/f2e/<ambiente>/global-limits`.

```json
{
  "maxFileBytes": 10737418240,
  "maxChunkBytes": 67108864,
  "maxEventBytes": 1044480,
  "maxBatchSize": 10,
  "inputTypes": {
    "text":       { "maxFileBytes": 10737418240, "maxRecordBytes": 1040384 },
    "json":       { "maxFileBytes": 10737418240, "maxRecordBytes": 1040384 },
    "multi-line": { "maxFileBytes": 10737418240, "maxRecordBytes": 1040384 }
  }
}
```

Configuração por prefixo que exceda qualquer teto global DEVE ser rejeitada
antes do planejamento.

### 7.4 ChunkJob

Cada mensagem interna DEVE carregar, no mínimo:

```json
{
  "schemaVersion": "1",
  "jobId": "...",
  "fileId": "...",
  "chunkId": "00000001",
  "bucket": "meu-bucket",
  "key": "example-text/dados.txt",
  "versionId": "opcional",
  "etag": "...",
  "fileSize": 123456,
  "startRecord": 1,
  "startByte": 0,
  "endByteInclusive": 65535,
  "maxRecordLengthBytes": 65536,
  "trailingPaddingBytes": 65536,
  "dataType": "text",
  "context": {},
  "configuration": {},
  "configSnapshot": {}
}
```

Os layouts `multiLineLayout` e `jsonArrayLayout` devem ser propagados sem
alteração. Uma mensagem com `control` não lê conteúdo e existe apenas para o
Worker reconciliar a conclusão.

### 7.5 Envelope v1 de saída

```json
{
  "metadata": {
    "eventId": "...",
    "sourceRecordId": "...",
    "schema": { "id": "f2e-record", "version": "1" },
    "format": "json",
    "createdAt": "2026-09-16T12:00:00Z",
    "transactionId": "opcional",
    "correlationId": "opcional",
    "traceId": "opcional"
  },
  "source": {
    "type": "s3",
    "system": "opcional",
    "bucket": "meu-bucket",
    "key": "example-text/dados.txt",
    "versionId": "opcional",
    "etag": "...",
    "fileName": "dados.txt",
    "fileFormat": "text",
    "fileSize": 123456
  },
  "processing": {
    "jobId": "...",
    "chunkId": "00000001",
    "recordNumber": 42,
    "byteOffset": 1000,
    "byteLength": 35
  },
  "data": { "raw": "conteúdo original sem terminador" }
}
```

`createdAt` DEVE usar RFC 3339. O corpo e seus Message Attributes devem caber
no limite configurado. Atributos devem refletir ao menos schema e formato.

### 7.6 BundleEnvelope

No modo `bundle`, uma mensagem física possui:

```json
{
  "schemaVersion": "f2e-bundle/1",
  "bundleId": "...",
  "jobId": "...",
  "chunkId": "00000001",
  "items": [
    { "metadata": {}, "source": {}, "processing": {}, "data": {} }
  ]
}
```

Todos os itens pertencem ao mesmo chunk. `maxEnvelopesPerMessage = 0` remove o
limite de quantidade, mas não o limite de bytes. `maxMessageBytes = 0` usa
`maxEventBytes`. Cada envelope ainda respeita `maxEventBytes`. Se um único
envelope não couber, o registro falha com `MESSAGE_TOO_LARGE`; nunca é truncado.

### 7.7 CompletionEvent

```json
{
  "schemaVersion": "f2e-completion/1",
  "eventId": "...",
  "jobId": "...",
  "fileId": "...",
  "prefixId": "example-text",
  "source": {
    "bucket": "meu-bucket",
    "key": "example-text/dados.txt",
    "versionId": "opcional",
    "etag": "...",
    "fileSize": 123456
  },
  "config": {
    "configId": "hash-do-snapshot",
    "parameterName": "/f2e/prod/file-config/meu-bucket/example-text",
    "parameterVersion": 7
  },
  "status": "COMPLETED",
  "result": "SUCCESS",
  "counts": {
    "recordsRead": 1000,
    "recordsPublished": 1000,
    "recordsRejected": 0,
    "recordsIgnored": 0,
    "countsComplete": true
  },
  "timestamps": {
    "receivedAt": "2026-09-16T12:00:00Z",
    "completedAt": "2026-09-16T12:01:00Z"
  }
}
```

Esse evento é idempotente por `eventId`, mas pode ser entregue mais de uma vez.
Ele sinaliza estado terminal do F2E, não consumo downstream completo.

## 8. Regras de parsing e planejamento

### 8.1 Modo `text`

- Um registro é uma linha física terminada por CR, LF ou CRLF.
- CRLF é um único terminador; LFCR são dois terminadores.
- O terminador não integra `data.raw`.
- Linha vazia terminada é registro válido.
- Terminador final não cria registro adicional.
- Última linha sem terminador é erro `UNTERMINATED_LINE`.
- Arquivo vazio é `EMPTY_FILE`.
- UTF-8 inválido é `INVALID_UTF8`.
- Espaços devem ser preservados, sem trim.
- `maxRecordLengthBytes` é obrigatório quando houver divisão em chunks.

Para arquivo menor que 5 MiB, usar um chunk. Para arquivo maior, o alvo nominal
deve ser `targetChunkBytes` quando não zero; caso contrário, deve buscar cerca de
100 chunks balanceados. O alvo nunca pode ser menor que 5 MiB, maior que 100 MiB
ou maior que `maxChunkBytes`.

O Worker pode ler até `maxRecordLengthBytes` antes do início nominal, descartar
o primeiro fragmento e emitir somente registros cujo byte inicial esteja dentro
da faixa nominal. Deve ler extensão após o fim nominal suficiente para fechar o
último registro. Essa regra DEVE impedir lacunas e duplicatas, inclusive quando
CRLF cruza a fronteira.

### 8.2 Modo `json`

- O documento deve ser um array no nível raiz.
- Cada elemento deve ser um objeto JSON.
- `jsonArrayLayout.firstFieldName` e `maxBytesPerElement` são obrigatórios.
- O campo configurado deve ser a primeira chave de todo elemento.
- O produtor deve garantir que esse nome não apareça como chave estrutural fora
  dos elementos, inclusive em objetos aninhados.
- O reconhecedor deve interpretar sintaxe JSON, strings e escapes; ocorrência
  textual dentro de valor não delimita elemento.
- `data.raw` recebe o JSON completo do elemento.
- Array vazio é execução válida com zero eventos e conclusão `SUCCESS`.

O Organizer deve planejar usando metadados, sem ler o conteúdo. O Worker usa o
marcador estrutural e `maxBytesPerElement` para se realinhar e fechar elementos
nas bordas dos chunks.

### 8.3 Modo `multi-line`

```json
{
  "breakFields": [
    { "startByte": 0, "lengthBytes": 1, "value": "1" }
  ],
  "includeFields": [],
  "ignoreFields": [],
  "lineSeparator": "\u001c",
  "maxBytesPerRecord": 65536
}
```

- `breakFields` e `maxBytesPerRecord` são obrigatórios.
- Cada grupo de campos contém de 1 a 99 comparações por posição de byte.
- Todos os campos de um grupo devem corresponder, sem trim; linha curta não
  corresponde.
- A precedência é break, include, ignore.
- Break inicia novo registro; include acrescenta linha ao registro atual;
  ignore e linhas sem correspondência não integram `data.raw`.
- Cabeçalhos e trailers ignorados devem ser contabilizados separadamente.
- Separador lógico padrão entre linhas: ASCII FS (`\x1C`).
- Terminadores físicos seguem as regras de `text`.

## 9. Estados, contadores e ledger

### 9.1 Máquina de estados

```text
Job:   RECEIVED -> VALIDATING -> PLANNING -> PROCESSING -> COMPLETED
          |            |            |             |
          +------------+------------+-------------+--> FAILED
                       +------------+-----------------> REJECTED

Chunk: PENDING -> RUNNING -> COMPLETED
                      |
                      +-> RETRY_PENDING -> RUNNING
                      +-> FAILED
```

Estados terminais não podem regredir. `COMPLETED` possui resultado `SUCCESS` ou
`WITH_REJECTIONS`; `FAILED` representa falha técnica e `REJECTED`, arquivo ou
configuração não elegível.

Motivos mínimos: `INVALID_UTF8`, `UNTERMINATED_LINE`, `OVERSIZED_RECORD`,
`EMPTY_FILE`, `INVALID_CONFIG`, `UNSUPPORTED_TYPE`, `MESSAGE_TOO_LARGE`,
`MULTILINE_HEADER` e `MULTILINE_TRAILER`.

### 9.2 Invariantes de contagem

Com manifesto selado:

```text
expectedChunks = pendingChunks + runningChunks + retryPendingChunks
               + completedChunks + failedChunks
```

Para chunk concluído:

```text
recordsRead = recordsPublished + recordsRejected + recordsIgnored
```

`messagesPublished` é diferente de `recordsPublished` no modo bundle. Também
devem ser rastreados `sendMessageBatchCalls`, `physicalLinesIgnored`, razões por
categoria e `countsComplete`. Reenvios não podem inflar totais lógicos.

### 9.3 Modelo DynamoDB

Uma tabela on-demand com chave composta `pk`/`sk` DEVE manter itens agrupados
por `pk = JOB#<jobId>`. Sort keys sugeridas:

- `JOB` para o agregado;
- `CHUNK#<chunkId>` para manifesto e estado;
- `COMPLETION_INTENT#<versão-com-padding>` para a outbox;
- `AUDIT#REPLAY#<instante>#<novoJobId>` para auditoria de replay.

O ledger deve usar operações condicionais e transações onde houver atualização
de chunk e agregado. Cada item deve ter TTL `expiresAt`. O agregado deve manter
`statusIndex` e `updatedAt`; criar GSI `status-time-index` para localizar jobs
parados sem Scan. Habilitar PITR fora do LocalStack e criptografia em repouso.

## 10. Infraestrutura como código

O Terraform DEVE criar:

- bucket S3 de entrada com bloqueio público, ownership controls, versionamento,
  criptografia, política TLS e lifecycle;
- filas `file-intake`, `chunk-jobs`, `output-events`, `completion-events`;
- DLQs de intake, chunks e conclusão, com redrive policy;
- filas de saída dedicadas opcionais por prefixo;
- tabela DynamoDB do ledger;
- parâmetros SSM globais e por prefixo;
- Lambdas, aliases publicados e event source mappings;
- roles IAM distintas e de privilégio mínimo para Organizer e Worker;
- log groups com retenção, dashboard e alarmes;
- políticas que neguem transporte inseguro.

As filas são Standard. `worker_batch_size` DEVE ser 1. O visibility timeout deve
ser no mínimo seis vezes o timeout da Lambda. A DLQ deve reter mensagens por
mais tempo que a fila principal. Produção deve exigir KMS gerenciada pelo
cliente e destino SNS para alarmes; LocalStack pode usar chaves gerenciadas e
tracing `PassThrough`.

Valores iniciais recomendados:

| Item | Valor |
|---|---:|
| Arquivo máximo | 10 GiB |
| Chunk máximo | 64 MiB |
| Evento configurado | 1.020 KiB (1 MiB - 4 KiB) |
| Lote SQS | 10 mensagens e no máximo 1 MiB total |
| Tentativas antes da DLQ | 3 |
| Concorrência máxima do Worker | 4 |
| Memória Lambda | 256 MiB |
| Armazenamento efêmero | 512 MiB |
| Retenção de logs | 30 dias |
| Retenção do ledger/objeto | 15 dias |
| Retenção de fila/DLQ | 13/14 dias |

A fila de chunks e o Worker são compartilhados entre prefixos. Uma saída
dedicada isola apenas o backlog de consumo. Isolamento de segurança ou capacidade
exige implantação F2E completa separada.

## 11. Variáveis de ambiente mínimas

| Variável | Default/uso |
|---|---|
| `AWS_ENDPOINT_URL` | endpoint LocalStack; ausente em AWS real |
| `AWS_REGION` | `us-east-1` |
| `F2E_ENVIRONMENT` | `local` |
| `F2E_INPUT_BUCKET` | bucket de entrada |
| `F2E_INTAKE_QUEUE_URL` | URL do intake |
| `F2E_CHUNK_QUEUE_URL` | URL dos chunks |
| `F2E_OUTPUT_QUEUE_URL` | saída padrão |
| `F2E_COMPLETION_QUEUE_URL` | conclusão |
| `F2E_LEDGER_TABLE` | tabela do ledger |
| `F2E_FILE_CONFIG_PATH` | `/f2e/<ambiente>/file-config` |
| `F2E_GLOBAL_LIMITS_PARAMETER` | `/f2e/<ambiente>/global-limits` |
| `F2E_RECORDS_PER_CHUNK` | `1000` |
| `F2E_BATCH_SIZE` | `10`, faixa 1–10 |
| `F2E_MAX_EVENT_BYTES` | `1044480`, faixa 1 KiB–1 MiB |
| `F2E_MAX_FILE_BYTES` | `10737418240` |
| `F2E_MAX_CHUNK_BYTES` | `67108864` |
| `F2E_TARGET_CHUNK_BYTES` | `0` ou 5–100 MiB, limitado pelo chunk |
| `F2E_MAX_RECEIVE_COUNT` | `3` |
| `F2E_LEDGER_RETENTION_DAYS` | retenção do ledger |
| `F2E_OUTPUT_MODE` | vazio/`single` ou `bundle` |
| `F2E_MAX_ENVELOPES_PER_MESSAGE` | `0` sem limite de quantidade |
| `F2E_MAX_MESSAGE_BYTES` | `0` usa o limite do evento |
| `F2E_PUBLISH_CONCURRENCY` | `1`, normalizado para 1–16 |
| `F2E_EVENT_SCHEMA_ID` | `f2e-record` |
| `F2E_EVENT_SCHEMA_VERSION` | `1` |
| `F2E_EVENT_FORMAT` | `json` |

Configuração numérica incoerente DEVE impedir a inicialização, exceto a
concorrência de publicação, que pode ser normalizada para 1–16.

## 12. Segurança e resiliência

- Nunca confiar em `VersionId`, ETag ou tamanho fornecidos sem validar contra o
  objeto acessado.
- URLs pré-assinadas devem aceitar somente HTTPS em produção, impedir acesso a
  hosts/metadados internos e validar range e identidade da resposta.
- Dados sensíveis não devem aparecer integralmente em logs.
- Logs estruturados devem conter `jobId`, `fileId`, `chunkId`, tentativa e IDs
  de correlação quando disponíveis.
- S3 versionado é preferível. Sem versão, objeto substituído ou removido deve
  falhar por ETag, sem processar conteúdo diferente.
- Lambdas devem respeitar cancelamento e deadlines em todas as operações.
- O envio em lote deve tentar novamente somente entradas falhas; erro permanente
  ou esgotamento deve devolver a mensagem de chunk ao ciclo SQS/DLQ.
- A outbox de conclusão deve sobreviver a falha entre transição terminal e envio.
  Falha após envio e antes do checkpoint pode duplicar o evento, com mesmo ID.

## 13. Observabilidade e operação

Alarmes mínimos:

- qualquer mensagem nas DLQs de intake, chunks e conclusão;
- idade da mensagem mais antiga nas filas principais;
- erros e throttles das Lambdas;
- duração próxima do timeout;
- concorrência do Worker.

O dashboard deve mostrar backlog, idade, erros, duração e concorrência. O
runbook deve explicar: triagem por `jobId`, inspeção do ledger, tratamento de
DLQ, redrive, replay completo ou seletivo, objeto alterado, configuração
inválida, registro grande, job parado e rollback de implantação.

## 14. Testes e critérios de aceite

### 14.1 Testes unitários obrigatórios

- Estados permitem somente transições descritas e terminais não regridem.
- IDs são determinísticos, distintos quando a identidade muda e hexadecimais.
- Parser `text`: LF, CR, CRLF, LFCR, mistos, vazio, linha vazia, terminador
  final, falta de terminador, UTF-8 inválido, limite exato e estouro.
- Parser `multi-line`: posições por byte, todos os campos, precedência, linha
  curta, cabeçalho/trailer, UTF-8 e fronteira entre chunks.
- Parser JSON: chave estrutural versus texto em valor, escapes, objeto completo,
  realinhamento e 10.000 elementos sem perda.
- Planner cria faixas balanceadas sem ler conteúdo e visa aproximadamente 100
  chunks quando não há alvo explícito.
- Chunks adjacentes não duplicam nem omitem registro, inclusive CRLF na borda.
- Validação rejeita tipos antigos, limites globais violados, prefixo/URL/layout
  inválidos e configuração numérica incoerente.
- Packer bundle respeita quantidade, bytes, ID estável e envelope grande.
- Batches respeitam 10 entradas e 1 MiB agregado.
- Concorrência 1, 2 e 4 não perde eventos e passa com detector de race.
- Retry preserva `eventId` e `sourceRecordId`.
- Falha parcial retorna somente IDs das mensagens SQS que falharam.
- Conclusão é enviada antes de marcar a intenção como entregue.

### 14.2 Integração com LocalStack

- ciclo completo de admissão, planejamento, agendamento, aquisição e conclusão;
- duas tentativas concorrentes adquirem um chunk no máximo uma vez;
- notificações diferentes do mesmo arquivo físico não criam execução normal
  duplicada;
- checkpoint de publicação permite retomar apenas chunks não confirmados;
- agregação concorrente termina o job uma única vez;
- contadores e razões são persistidos corretamente;
- SSM seleciona o prefixo mais específico e snapshot não muda após admissão;
- objeto não versionado alterado é detectado.

### 14.3 Cenários E2E mínimos

1. `text` com 1, 2, 1.000 e mais de 1.000 registros: exatamente N envelopes,
   números 1..N, nenhum duplicado e DLQs vazias.
2. arquivo `text` e `multi-line` vazio: zero eventos e job rejeitado.
3. array JSON vazio: zero eventos e job concluído com sucesso.
4. JSON com um, poucos e 10.000 elementos: exatamente N envelopes válidos.
5. `multi-line` com cabeçalho, registros de várias linhas e trailer: somente
   registros elegíveis e contagem de linhas ignoradas.
6. repetição da mesma mensagem/chunk: nenhuma republicação após checkpoint.
7. falha transitória de publicação: retry seletivo e conclusão consistente.
8. `bundle`: contagem lógica N, mensagens físicas menores ou iguais a N,
   bundles válidos e IDs estáveis.
9. configuração de prefixo mais específico e fila dedicada: eventos na rota
   correta.
10. conclusão para `COMPLETED`, `FAILED` e `REJECTED`, com duplicata dedutível.

O smoke test local deve gerar ao menos 20.000 registros e validar
`20.000/20.000`, nenhuma lacuna, nenhuma DLQ e nenhum erro. A suíte deve salvar
relatórios com timestamp local e offset: resumo JSON, mensagens de saída JSONL,
conclusões JSONL, DLQs JSONL e snapshot final do DynamoDB.

## 15. Automação e comandos esperados

Devem existir comandos equivalentes a:

```bash
# testes da aplicação
go test ./...
go test -race ./...

# ambiente e fluxo local
cd automacao
./subir-ambiente.sh
./executar-fluxo.sh
./validar-fluxo.sh
./parar-ambiente.sh

# suíte ponta a ponta
cd e2e
go test -count=1 -v -timeout 25m ./...
```

Os scripts devem usar `set -euo pipefail`, validar pré-requisitos, aceitar
endpoint/região por ambiente, não sobrescrever massa sem opção explícita e ser
seguros para repetição. O build deve gerar `organizer.zip` e `worker.zip` com
binário `bootstrap` para a arquitetura selecionada.

## 16. Plano de implementação para o agente

O agente deveria trabalhar em incrementos verificáveis:

1. **Fundação:** módulo Go, modelos, estados, IDs, parsers e testes unitários.
2. **Casos de uso puros:** interfaces, planner, Worker, envelopes, packer e
   testes com fakes em memória.
3. **AWS:** clientes S3/SQS/SSM/DynamoDB, handlers Lambda e falhas parciais.
4. **Ledger:** admissão, manifesto, aquisição, checkpoints, agregação, outbox e
   replay, todos com condições de concorrência.
5. **Infraestrutura:** Terraform, IAM, SSM, filas, Lambda, observabilidade.
6. **Ambiente local:** LocalStack, build e scripts idempotentes.
7. **E2E e hardening:** cenários, race detector, limites, segurança e runbooks.

Ao fim de cada incremento, o agente DEVE executar os testes aplicáveis e
registrar decisões que alterem esta especificação em ADR. Não deve marcar a
entrega como concluída com testes falhando, contratos não versionados, recursos
manuais ou TODOs no caminho crítico.

## 17. Definição de pronto

O projeto está pronto para validação quando:

- pode ser criado do zero por scripts documentados, sem arquivo secreto ou
  recurso AWS manual;
- testes unitários, integração e E2E passam de forma repetível;
- os três formatos funcionam em arquivo pequeno e dividido;
- retries não criam lacunas e IDs permanecem estáveis;
- ledger e conclusão refletem contagens verdadeiras;
- DLQs, alarmes, logs, retenção, criptografia e IAM estão definidos;
- contratos têm exemplos e schemas versionados;
- runbooks cobrem falha, redrive e replay;
- benchmark com massa representativa registra memória, duração, throughput,
  backlog e custo estimado;
- limites e SLO do ambiente de destino foram validados antes de produção.

Esta especificação descreve uma prova de conceito pronta para validação e
evolução. Homologação de produção exige teste de carga com dados reais,
modelagem de custo, revisão de segurança, definição de SLO, responsáveis por
alarmes/DLQs e confirmação da retenção das versões S3 durante toda a janela de
retry e replay.
