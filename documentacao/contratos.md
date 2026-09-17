# Contratos versionados

Todos os contratos usam `schemaVersion: "1"`.

## Entrada do Organizer

O Organizer recebe em SQS o contrato explícito abaixo. Cada arquivo declara o modo de delimitação e os layouts que devem ser propagados sem alteração para o `ChunkJob` de cada worker.

```json
{
  "schemaVersion": "1",
  "files": [{
    "bucket": "f2e-input",
    "key": "entrada/registros.txt",
    "dataType": "text",
    "maxRecordLengthBytes": 1048576
  }]
}
```

Os três modos suportados são `text`, `json` e `multi-line`. Tipos removidos (`fixed-width`, `jsonl`, `ndjson`, `csv`, `binary`) produzem erro explícito "unsupported data type". Nomes antigos não são aceitos como aliases.

### Modo `text`

Uma linha física terminada por CR (`\r`), LF (`\n`) ou CRLF (`\r\n`) é um registro lógico. CRLF é um único terminador; LFCR são dois. O terminador não integra `data.raw`. Linha vazia terminada é um registro vazio válido. Terminador final não gera registro extra. Última linha sem terminador é erro. Arquivo vazio é rejeitado. Bytes UTF-8 inválidos geram erro identificável. `maxRecordLengthBytes` é obrigatório para arquivos com mais de `F2E_RECORDS_PER_CHUNK` linhas.

### Modo `json`

Um objeto do array selecionado por `jsonArrayLayout.arrayPath` (vazio = root array). `firstFieldName` e `maxBytesPerElement` são obrigatórios. O Organizer não lê o corpo do objeto; Workers localizam o primeiro campo por chave JSON real, com janela limitada pelo máximo por elemento. O produtor deve garantir que esse campo seja o primeiro em todos os objetos selecionados e não ocorra fora deles, inclusive em valores/objetos aninhados. Arrays de primitivos não são suportados.

### Modo `multi-line`

Linhas físicas agrupadas por campos configurados em `multiLineLayout`:
- `breakFields` é obrigatório; `includeFields` e `ignoreFields` são opcionais.
- Cada conjunto tem de 1 a 99 campos (`startByte`, `lengthBytes`, `value`); todos devem corresponder por bytes, sem trim. Linha curta não corresponde.
- A precedência é quebra, inclusão e ignorar; linhas sem correspondência são descartadas.
- `lineSeparator`: separador entre linhas do registro (padrão: `\x1C`).
- `maxBytesPerRecord`: tamanho máximo do registro lógico completo (obrigatório).

Cabeçalhos e trailers (linhas que não correspondem a nenhum prefixo) são silenciosamente ignorados e contados como linhas físicas ignoradas em métrica separada. Terminadores físicos CR/LF/CRLF são suportados com as mesmas regras do modo `text`.

## Limites padrão

10 GiB por arquivo (`F2E_MAX_FILE_BYTES`), 64 MiB por chunk (`F2E_MAX_CHUNK_BYTES`), limite por registro/elemento declarado no layout, e 1.020 KiB para cada evento. Esse valor é 1 MiB menos 4 KiB, reservados para os Message Attributes e a codificação SQS. Configurações cujo chunk nominal ultrapasse o limite são rejeitadas no planejamento.

## Planejamento de chunks (text e multi-line)

Para `text` com `maxRecordLengthBytes > 0`, o organizer cria faixas nominais de `F2E_RECORDS_PER_CHUNK × maxRecordLengthBytes` bytes e estende cada faixa até o terminador que encerra o registro cruzando o fim nominal. O job informa `maxRecordLengthBytes` e a extensão exata em `trailingPaddingBytes`. O worker lê até `maxRecordLengthBytes` antes do início nominal, descarta o primeiro fragmento e publica somente registros cujo byte inicial esteja na faixa nominal.

CRLF que cruza a fronteira nominal é tratado corretamente: se o CR está no último byte nominal e o LF está no primeiro byte de extensão, o chunk inclui ambos e o leitor os reconhece como terminador único.

## Notificação S3

O envelope de notificação S3 aceita múltiplos `Records`; somente `ObjectCreated:*` é processado. O Organizer busca no SSM a configuração cuja combinação de bucket e prefixo seja a mais específica. A chave é decodificada conforme a codificação URL de eventos S3. Eventos de teste sem registros de objeto são reconhecidos e ignorados.

## ChunkJob

`ChunkJob` contém `bucket`, `key`, `versionId`, `etag`, `fileSize`, IDs SHA-256, `startByte`, `endByteInclusive`, `maxRecordLengthBytes`, `trailingPaddingBytes`, `dataType`, `multiLineLayout`, `jsonArrayLayout`, contexto corporativo e configuração. `VersionId` é usado quando presente; sem ele, toda leitura usa `If-Match` com o ETag.

## Contexto corporativo e identidades de mensagem

`transactionId`, `correlationId`, `traceId` e `sourceSystem` atravessam organizer e worker e são incorporados ao body final. Os Message Attributes são construídos após o `RecordProcessor` e refletem `schema` e `format` finais. Extensões não podem remover ou trocar IDs e localização técnica.

## Envelope v1

O envelope v1 contém `eventId`, `sourceRecordId`, identidade imutável da origem, IDs do job/chunk, posição e payload (`data.raw` para todos os três modos). `eventId` é estável durante retries do mesmo job e muda em replay explícito. Consumidores que deduplicam o registro físico devem usar `sourceRecordId`. O JSON Schema normativo está em `documentacao/schemas/envelope-v1.schema.json`.

## Migração de tipos removidos

| Tipo removido | Caminho | Perda |
|---|---|---|
| `fixed-width` (com LF) | Reconfigurar como `text` com `maxRecordLengthBytes = recordLengthBytes` | Validação de tamanho exato por registro |
| `jsonl` / `ndjson` | Reconfigurar como `text` | Validação estrutural JSON por linha |
| `csv` (coluna única / LF) | Reconfigurar como `text` | Parsing RFC 4180 |
| `csv` (campos multi-linha) | Sem equivalente | Requer parser externo |
| `binary` | Sem equivalente | Encapsulamento Base64; usar contrato de referência S3 |
