# Especificação — Envelope de Eventos do F2E

## 1. Objetivo

Implementar no **F2E — File-to-Events** um padrão único de envelope para todos os registros produzidos a partir de arquivos.

O objetivo é que cada registro publicado em Amazon SNS ou Amazon SQS carregue:

- identificação do schema;
- versão do schema;
- formato do evento;
- rastreabilidade completa até o arquivo de origem;
- identificação do job e chunk responsáveis pelo processamento;
- localização do registro dentro do arquivo;
- identificadores técnicos de correlação;
- payload produzido pelo parser e, quando aplicável, pela transformação específica da aplicação.

O conceito interno deve ser chamado de **File-to-Envelope**, pois cada registro lido pelo framework é transformado em um envelope padronizado.

Entretanto, o nome do produto, framework e capacidade arquitetural deve permanecer:

**F2E — File-to-Events**

A relação conceitual é:

```text
File-to-Events
      |
      +--> File
      |
      +--> Chunk
      |
      +--> Record
      |
      +--> File-to-Envelope
      |
      +--> SNS / SQS Event
```

O envelope é o contrato técnico utilizado para gerar o evento.

---

# 2. Princípio de design dos Message Attributes

Os Message Attributes de SNS/SQS são um recurso escasso e não devem ser utilizados como armazenamento genérico de metadados.

O F2E deve publicar, por padrão, apenas os seguintes atributos próprios:

```text
schema
format
```

A aplicação hospedeira poderá adicionar atributos corporativos obrigatórios, por exemplo:

```text
transactionId
correlationId
traceId
```

O framework não deve consumir desnecessariamente o limite disponível.

Todos os demais metadados do F2E devem estar no corpo do evento.

---

# 3. Attributes definidos pelo F2E

## 3.1 `schema`

Identifica simultaneamente o schema lógico e sua versão.

Formato recomendado:

```text
<schema-id>:<version>
```

Exemplos:

```text
customer-created:1
payment-transaction:3
legacy-account:2.1
```

Também pode ser utilizado um identificador corporativo mais formal:

```text
br.com.company.customer-created:v1
```

O formato exato deve ser padronizado pelo projeto, mas deve sempre permitir identificar:

```text
schema lógico + versão
```

em um único Message Attribute.

### Finalidade

Esse atributo existe principalmente para **roteamento e compatibilidade de consumidores**.

Exemplo conceitual de filtro SNS:

```text
schema = payment-transaction:3
```

Uma subscription poderá, portanto, receber somente eventos pertencentes a determinado contrato.

---

## 3.2 `format`

Identifica o formato de serialização do corpo.

Valores iniciais recomendados:

```text
json
```

Possíveis extensões futuras:

```text
json
avro
protobuf
```

Esse atributo não representa o formato do arquivo original.

Por exemplo:

```text
arquivo de origem: FIXED_WIDTH
evento publicado: JSON
```

Nesse caso:

```text
format = json
```

O formato físico do arquivo de origem pertence ao envelope, dentro de `source`.

---

# 4. Regra para SNS e SQS

O mesmo contrato deve ser utilizado independentemente do publisher.

```text
F2E Envelope
      |
      +--> SNS Publisher
      |
      +--> SQS Publisher
```

Os publishers não devem possuir contratos de evento diferentes.

O envelope é gerado antes da escolha do destino.

Exemplo:

```text
Record
  |
  v
Event Mapper
  |
  v
File-to-Envelope
  |
  +--> Attributes
  |
  +--> Body
  |
  v
Publisher
  |
  +--> SNS
  +--> SQS
```

---

# 5. Message Attributes esperados

Exemplo mínimo gerado pelo F2E:

```text
schema = customer-created:1
format = json
```

Exemplo com atributos corporativos adicionados pela aplicação:

```text
schema        = customer-created:1
format        = json
transactionId = 8e8c...
correlationId = 31ad...
traceId       = 492f...
```

O framework deve permitir que atributos externos sejam adicionados sem que o core precise conhecer seu significado funcional.

---

# 6. Regra de não duplicação nos atributos

Os seguintes dados NÃO devem virar Message Attributes por padrão:

```text
jobId
chunkId
eventId
bucket
key
versionId
etag
fileName
sourceSystem
recordNumber
byteOffset
byteLength
originalFormat
timestamp
```

Todos eles devem permanecer no body.

Mesmo que eventualmente algum desses campos possa ser útil para roteamento, não deve existir promoção automática para Message Attribute.

Caso uma aplicação específica necessite filtrar pelo campo no SNS, deverá fazer isso explicitamente por configuração/extensão ou avaliar filtragem pelo MessageBody.

---

# 7. Estrutura padrão do body

O body deve ser um envelope JSON com quatro blocos principais:

```text
metadata
source
processing
data
```

Estrutura conceitual:

```json
{
  "metadata": {},
  "source": {},
  "processing": {},
  "data": {}
}
```

---

# 8. `metadata`

Contém informações sobre o evento produzido.

Estrutura sugerida:

```json
{
  "metadata": {
    "eventId": "01K...",
    "schema": {
      "id": "customer-created",
      "version": "1"
    },
    "format": "json",
    "createdAt": "2026-08-31T12:00:00Z",
    "transactionId": "8e8c...",
    "correlationId": "31ad..."
  }
}
```

Campos:

### `eventId`

Identificador único da instância daquele evento.

Deve permitir rastreamento individual, retry e investigação.

---

### `schema.id`

Identificador lógico do contrato.

Exemplo:

```text
customer-created
```

---

### `schema.version`

Versão do contrato.

Exemplo:

```text
1
```

Embora `schema + version` também estejam representados no Message Attribute `schema`, devem permanecer no body.

Essa duplicação é intencional.

O atributo atende ao broker.

O body atende ao contrato do evento.

---

### `format`

Formato do corpo/evento.

Também é duplicado em Message Attribute pelo mesmo motivo.

---

### `createdAt`

Momento de criação do envelope F2E.

---

### Identificadores corporativos

Quando disponíveis, identificadores como:

```text
transactionId
correlationId
traceId
```

também devem existir no body, mesmo que sejam publicados como Message Attributes.

O body deve continuar autossuficiente quando persistido, enviado para DLQ, auditado ou replayado fora do contexto original do broker.

---

# 9. `source`

Representa a proveniência original do registro.

Exemplo:

```json
{
  "source": {
    "type": "s3",
    "system": "legacy-core",
    "bucket": "input-files",
    "key": "payments/2026/08/payments.dat",
    "versionId": "AbCd123",
    "etag": "d41d8cd98f...",
    "fileName": "payments.dat",
    "fileFormat": "fixed-width",
    "fileSize": 8246337201
  }
}
```

O objetivo do bloco `source` é permitir responder:

```text
De onde veio este registro?
```

---

# 10. URLs pré-assinadas

Quando o F2E receber uma URL pré-assinada como mecanismo de acesso ao arquivo, a URL não deve ser propagada para cada evento.

A URL é uma credencial temporária e deve permanecer restrita à etapa de ingestão/processamento.

O F2E deve resolver a URL para uma identidade lógica do objeto sempre que possível.

O envelope deve armazenar:

```text
bucket
key
versionId
etag
```

ou outro identificador estável equivalente.

Não armazenar:

```text
https://bucket.s3.amazonaws.com/file?X-Amz-Signature=...
```

no evento final.

---

# 11. `processing`

Representa como aquele registro passou pelo F2E.

Exemplo:

```json
{
  "processing": {
    "jobId": "job-01K...",
    "chunkId": "chunk-00042",
    "recordNumber": 1849234,
    "byteOffset": 5370021936,
    "byteLength": 384
  }
}
```

---

# 12. `jobId`

Identifica a execução lógica associada ao arquivo.

Todos os chunks e todos os eventos daquele processamento devem compartilhar o mesmo `jobId`.

Exemplo:

```text
arquivo
  |
  +--> jobId = J1
          |
          +--> chunk A
          +--> chunk B
          +--> chunk C
```

---

# 13. `chunkId`

Identifica a unidade de trabalho que produziu o registro.

Isso permite investigar:

```text
evento
  |
  v
chunk
  |
  v
job
  |
  v
arquivo
```

Também é útil para retry e futura implementação de checkpoints.

---

# 14. `recordNumber`

Quando o formato permitir determinar a posição lógica do registro no arquivo, armazenar seu número.

Exemplo:

```text
recordNumber = 1849234
```

Nem todos os formatos permitirão determinar isso de forma eficiente.

O campo deve ser opcional.

---

# 15. `byteOffset`

Posição inicial do registro dentro do arquivo original.

Exemplo:

```text
byteOffset = 5370021936
```

Esse campo deve ser preenchido sempre que o decoder conseguir determinar a posição física do registro.

Ele permite rastreabilidade extremamente precisa.

Exemplo:

```text
evento
  |
  v
byteOffset 5370021936
  |
  v
payments.dat
```

Em formatos fixed-width, essa informação é particularmente simples de determinar.

Em CSV, JSON, XML ou formatos binários, dependerá do decoder utilizado.

---

# 16. `byteLength`

Quantidade de bytes ocupada pelo registro original, quando conhecida.

Exemplo:

```text
byteLength = 384
```

Com:

```text
bucket
key
versionId
byteOffset
byteLength
```

é possível identificar precisamente o fragmento original que gerou o evento.

---

# 17. Identidade imutável do arquivo

Sempre que possível, não utilizar apenas:

```text
bucket + key
```

como identidade do arquivo.

Uma `key` do S3 pode posteriormente apontar para outro conteúdo.

Preferência:

```text
bucket
key
versionId
```

Quando versionamento não estiver disponível, utilizar também informações como:

```text
etag
fileSize
```

para melhorar a rastreabilidade.

---

# 18. `data`

Contém o dado funcional produzido pelo pipeline.

Exemplo:

```json
{
  "data": {
    "customerId": "123456",
    "account": "998877",
    "amount": 150.75
  }
}
```

O F2E não deve impor a estrutura interna de `data`.

Ela é definida pelo schema correspondente ao evento.

---

# 19. Pipeline File-to-Envelope

O pipeline conceitual deve ser:

```text
FILE
 |
 v
CHUNK
 |
 v
RECORD
 |
 v
DECODER
 |
 v
EVENT MAPPER
 |
 v
OPTIONAL APPLICATION PROCESSOR
 |
 v
FILE-TO-ENVELOPE
 |
 +--> metadata
 +--> source
 +--> processing
 +--> data
 |
 v
PUBLISHER
 |
 +--> SNS
 +--> SQS
```

O termo **File-to-Envelope** representa a construção técnica do envelope.

O produto continua sendo denominado:

```text
F2E — File-to-Events
```

---

# 20. Responsabilidade do framework

O F2E deve gerar automaticamente:

```text
eventId
createdAt
jobId
chunkId
source
recordNumber, quando disponível
byteOffset, quando disponível
byteLength, quando disponível
```

A aplicação deve fornecer ou configurar:

```text
schema.id
schema.version
data
```

E poderá fornecer:

```text
transactionId
correlationId
traceId
source.system
atributos corporativos
```

---

# 21. Interface conceitual em Go

Uma possível representação interna:

```go
type Envelope[T any] struct {
    Metadata   Metadata   `json:"metadata"`
    Source     Source     `json:"source"`
    Processing Processing `json:"processing"`
    Data       T          `json:"data"`
}

type Metadata struct {
    EventID       string `json:"eventId"`
    Schema        Schema `json:"schema"`
    Format        string `json:"format"`
    CreatedAt     string `json:"createdAt"`

    TransactionID string `json:"transactionId,omitempty"`
    CorrelationID string `json:"correlationId,omitempty"`
    TraceID       string `json:"traceId,omitempty"`
}

type Schema struct {
    ID      string `json:"id"`
    Version string `json:"version"`
}

type Source struct {
    Type       string `json:"type"`
    System     string `json:"system,omitempty"`
    Bucket     string `json:"bucket,omitempty"`
    Key        string `json:"key,omitempty"`
    VersionID  string `json:"versionId,omitempty"`
    ETag       string `json:"etag,omitempty"`
    FileName   string `json:"fileName,omitempty"`
    FileFormat string `json:"fileFormat,omitempty"`
    FileSize   int64  `json:"fileSize,omitempty"`
}

type Processing struct {
    JobID        string `json:"jobId"`
    ChunkID      string `json:"chunkId"`
    RecordNumber *int64 `json:"recordNumber,omitempty"`
    ByteOffset   *int64 `json:"byteOffset,omitempty"`
    ByteLength   *int64 `json:"byteLength,omitempty"`
}
```

O código é apenas uma referência conceitual. A implementação final pode adotar tipos específicos, UUID/ULID, `time.Time` e validações adicionais.

---

# 22. Construção dos Message Attributes

A criação dos attributes deve ocorrer a partir do envelope.

Exemplo conceitual:

```go
attributes := map[string]MessageAttribute{
    "schema": {
        Type:  "String",
        Value: envelope.Metadata.Schema.ID +
               ":" +
               envelope.Metadata.Schema.Version,
    },
    "format": {
        Type:  "String",
        Value: envelope.Metadata.Format,
    },
}
```

Em seguida, atributos corporativos configurados externamente podem ser mesclados.

```text
F2E attributes
      +
corporate attributes
      |
      v
validation
      |
      v
SNS / SQS
```

O framework deve validar antecipadamente a quantidade final de atributos e falhar de forma explícita caso ultrapasse o limite configurado para o publisher.

Nunca deve remover silenciosamente um atributo para caber no limite.

---

# 23. SNS e fan-out

Arquitetura esperada:

```text
                     +--> SQS Consumer A
                     |
F2E --> SNS Topic ---+--> SQS Consumer B
                     |
                     +--> SQS Consumer C
```

Os dois atributos do F2E permitem filtros simples.

Exemplo:

```text
schema = payment-created:2
```

ou:

```text
schema = customer-created:1
format = json
```

A subscription recebe apenas eventos cujo contrato seja compatível com seu consumo.

Os dados de proveniência permanecem exclusivamente no body porque não são responsabilidade do roteamento.

---

# 24. Filtragem por domínio

O `schema` deve representar suficientemente o tipo lógico do evento para evitar criação de múltiplos atributos como:

```text
eventType
domain
entity
operation
version
```

Exemplo:

Em vez de:

```text
domain      = payment
eventType   = created
version     = 2
```

utilizar:

```text
schema = payment-created:2
```

Isso economiza atributos e cria uma unidade única de versionamento e roteamento.

---

# 25. Raw Message Delivery

Quando utilizar SNS → SQS com Raw Message Delivery, o F2E deve assumir que existe restrição forte sobre a quantidade de Message Attributes.

Por isso, a especificação padrão utiliza somente:

```text
schema
format
```

como atributos próprios do F2E.

Isso preserva espaço para atributos organizacionais e evita acoplamento da arquitetura aos metadados internos do framework.

---

# 26. DLQ

Uma mensagem encaminhada para DLQ deve continuar contendo no body tudo o que for necessário para investigação.

Nenhuma operação crítica deve depender exclusivamente de Message Attributes.

Um operador deve conseguir examinar apenas o body e descobrir:

```text
qual evento era
qual schema era
de qual arquivo veio
qual job processou
qual chunk processou
qual registro originou
qual offset originou
qual correlação possuía
qual payload foi produzido
```

---

# 27. Replay

O mesmo princípio deve valer para replay.

Um evento armazenado fora do SNS/SQS original deve continuar sendo semanticamente completo.

Portanto:

```text
Body = contrato autossuficiente
Attributes = otimização de transporte/roteamento
```

Esse princípio deve ser tratado como regra arquitetural do F2E.

---

# 28. Idempotência

O `eventId` identifica a ocorrência do envelope, mas não necessariamente deve ser utilizado sozinho como chave de idempotência funcional.

Quando for necessário criar uma identidade determinística da origem, uma aplicação poderá derivá-la de:

```text
source identity
+
record position
+
schema
```

Por exemplo conceitualmente:

```text
hash(
  bucket +
  key +
  versionId +
  byteOffset +
  schemaId +
  schemaVersion
)
```

O framework pode futuramente disponibilizar essa informação como `sourceRecordId`.

Isso permitiria que o mesmo registro físico produzisse uma identidade estável entre retries.

---

# 29. Exemplo completo

### Message Attributes

```text
schema        = payment-created:2
format        = json

# adicionados pela plataforma/aplicação
transactionId = 9285...
correlationId = 1928...
```

### Body

```json
{
  "metadata": {
    "eventId": "01K4A...",
    "schema": {
      "id": "payment-created",
      "version": "2"
    },
    "format": "json",
    "createdAt": "2026-08-31T12:00:00Z",
    "transactionId": "9285...",
    "correlationId": "1928..."
  },
  "source": {
    "type": "s3",
    "system": "legacy-payments",
    "bucket": "payment-files",
    "key": "2026/08/31/payments.dat",
    "versionId": "J4x...",
    "etag": "71ab...",
    "fileName": "payments.dat",
    "fileFormat": "fixed-width",
    "fileSize": 8246337201
  },
  "processing": {
    "jobId": "01K4J...",
    "chunkId": "chunk-00042",
    "recordNumber": 1849234,
    "byteOffset": 5370021936,
    "byteLength": 384
  },
  "data": {
    "paymentId": "P123456",
    "account": "998877",
    "amount": 150.75,
    "currency": "BRL"
  }
}
```

---

# 30. Regra arquitetural final

O F2E deve seguir esta separação:

```text
MESSAGE ATTRIBUTES
------------------
schema
format
+
atributos exigidos pela organização

Responsabilidade:
roteamento e integração com o broker


MESSAGE BODY
------------
metadata
source
processing
data

Responsabilidade:
contrato, proveniência, rastreabilidade
e conteúdo do evento
```

Em outras palavras:

> **Attributes servem ao transporte. O Envelope serve ao evento.**

E:

> **File-to-Envelope é o mecanismo interno; File-to-Events continua sendo a capacidade arquitetural e o nome F2E.**