# Contratos versionados

Todos os contratos usam `schemaVersion: "1"`.

## Entrada do Organizer

O Organizer recebe em SQS o contrato explícito abaixo. Cada arquivo declara o formato e as opções que devem ser propagadas sem alteração para o `ChunkJob` de cada worker.

```json
{
  "schemaVersion": "1",
  "files": [{
    "bucket": "f2e-input",
    "key": "entrada/eventos.ndjson",
    "dataType": "ndjson",
    "maxRecordLengthBytes": 1048576,
    "options": { "bypassJsonValidation": false }
  }]
}
```

Os formatos de produção são `fixed-width`, `jsonl`, `ndjson` e `text`. `csv` e `json` array são preview e exigem `F2E_ENABLE_PREVIEW_FORMATS=true`; `binary` e `multi-line` são experimentais, exigem `F2E_ENABLE_EXPERIMENTAL_FORMATS=true` e são proibidos em produção. `bypassJsonValidation` só é aceito para `jsonl` e `ndjson`: quando `false` (padrão), cada linha é validada como um valor JSON; quando `true`, a linha é encaminhada como recebida, sem parse/validação. `text` é processado por linhas. CSV usa UTF-8, não remove header nem BOM, aceita quantidade variável de colunas e segue o parser RFC 4180 do Go, incluindo campos entre aspas, CRLF e quebras de linha internas.

JSON array aceita qualquer valor JSON válido (objeto, array, string, número,
booleano ou `null`) e usa parser consciente de strings, escapes e nesting. A
busca pelo array configurado em `arrayPath` é limitada por
`F2E_JSON_ARRAY_SEARCH_BYTES` (1 MiB por padrão); não localizar o path nessa
janela é erro explícito. Binário é não divisível e só é publicado se o envelope
Base64 completo couber em `F2E_MAX_EVENT_BYTES`; acima disso deve ser usado um
contrato de referência S3 pela aplicação hospedeira.

Os limites padrão são 10 GiB por arquivo (`F2E_MAX_FILE_BYTES`), 64 MiB por chunk (`F2E_MAX_CHUNK_BYTES`), o limite declarado por registro/layout e 256 KiB para body mais Message Attributes. Configurações cujo chunk nominal ultrapasse o limite são rejeitadas no planejamento.

Para arquivos de registros variáveis delimitados por LF (`jsonl`, `ndjson` e `text`), informe `maxRecordLengthBytes`. O organizer cria faixas nominais de `F2E_RECORDS_PER_CHUNK × maxRecordLengthBytes` e estende cada faixa até o LF que encerra o registro atravessando o fim nominal. O job informa o limite em `maxRecordLengthBytes` e a extensão exata em `trailingPaddingBytes` (a “gordura”). O worker lê até um tamanho máximo de registro antes do início nominal, descarta o primeiro fragmento/registo anterior e publica exclusivamente registros cujo byte inicial esteja na faixa nominal. Logo, registros completos da gordura inicial são desprezados e registros iniciados antes do fim nominal são processados pelo worker anterior, sem duplicação ou lacuna.

O envelope padrão de notificação S3 em SQS continua aceito por compatibilidade, incluindo múltiplos `Records`; somente `ObjectCreated:*` é processado. Ele sempre gera jobs `fixed-width` com as opções padrão. A chave é decodificada conforme a codificação URL de eventos S3.

`ChunkJob` contém `bucket`, `key`, `versionId`, `etag`, `fileSize`, IDs SHA-256, `startRecord`, `recordCount`, `recordLengthBytes`, `startByte`, `endByteInclusive`, `maxRecordLengthBytes`, `trailingPaddingBytes`, `dataType`, contexto corporativo e opções. `VersionId` é usado quando presente; sem ele, toda leitura usa `If-Match` com o ETag. ETag multipart é tratado somente como token opaco de condição, nunca como MD5. Para `fixed-width`, a largura inclui o LF; a massa é ASCII/UTF-8 e sempre usa LF.

`transactionId`, `correlationId`, `traceId` e `sourceSystem` atravessam organizer e worker e são incorporados ao body final. Os Message Attributes são construídos depois do `RecordProcessor`, a partir do envelope serializado, e portanto refletem `schema` e `format` finais. Extensões não podem remover ou trocar IDs e localização técnica. A aplicação hospedeira é responsável por validar o schema funcional de `data`.

O envelope v2 contém `eventId`, `sourceRecordId`, identidade imutável da origem,
IDs do job/chunk, posição e payload. `eventId` é estável durante retries do mesmo
job e muda em replay explícito. Consumidores que deduplicam o registro físico
devem usar `sourceRecordId`. O JSON Schema normativo está em
`documentacao/schemas/envelope-v2.schema.json`.
