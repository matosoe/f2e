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

Os valores aceitos para `dataType` são `fixed-width`, `jsonl`, `ndjson`, `csv`, `binary` e `text`. `bypassJsonValidation` só é aceito para `jsonl` e `ndjson`: quando `false` (padrão), cada linha é validada como um valor JSON; quando `true`, a linha é encaminhada como recebida, sem parse/validação. `text` é processado por linhas; `binary` é encaminhado como um único evento codificado em Base64; CSV é lido pelo parser CSV padrão, incluindo campos entre aspas e quebras de linha internas.

Para arquivos de registros variáveis delimitados por LF (`jsonl`, `ndjson` e `text`), informe `maxRecordLengthBytes`. O organizer cria faixas nominais de `F2E_RECORDS_PER_CHUNK × maxRecordLengthBytes` e estende cada faixa até o LF que encerra o registro atravessando o fim nominal. O job informa o limite em `maxRecordLengthBytes` e a extensão exata em `trailingPaddingBytes` (a “gordura”). O worker lê até um tamanho máximo de registro antes do início nominal, descarta o primeiro fragmento/registo anterior e publica exclusivamente registros cujo byte inicial esteja na faixa nominal. Logo, registros completos da gordura inicial são desprezados e registros iniciados antes do fim nominal são processados pelo worker anterior, sem duplicação ou lacuna.

O envelope padrão de notificação S3 em SQS continua aceito por compatibilidade, incluindo múltiplos `Records`; somente `ObjectCreated:*` é processado. Ele sempre gera jobs `fixed-width` com as opções padrão. A chave é decodificada conforme a codificação URL de eventos S3.

`ChunkJob` contém `bucket`, `key`, `etag`, IDs SHA-256 determinísticos, `startRecord`, `recordCount`, `recordLengthBytes`, `startByte`, `endByteInclusive`, `maxRecordLengthBytes`, `trailingPaddingBytes`, `dataType` e `options`. Portanto, o worker recebe sempre um contrato completo e explícito sobre o tipo de dado e as opções. Para `fixed-width`, a largura inclui o LF; a massa é ASCII UTF-8 e sempre usa LF.

`OutputEvent` contém `eventId` SHA-256 de `fileId/chunkId/recordNumber`, os IDs do job, `recordNumber`, `byteOffset`, `dataType` e o payload. `payload.raw` contém texto sem terminador de linha; `payload.fields` contém um registro CSV; e `payload.base64` contém dados binários. Entregas são at-least-once: consumidores devem deduplicar por `eventId`.
