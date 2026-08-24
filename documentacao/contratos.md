# Contratos versionados

Todos os contratos usam `schemaVersion: "1"`. O Organizer aceita o envelope padrão de notificação S3 em SQS, incluindo múltiplos `Records`; somente `ObjectCreated:*` é processado. A chave é decodificada conforme a codificação URL de eventos S3.

`ChunkJob` contém `bucket`, `key`, `etag`, IDs SHA-256 determinísticos, `startRecord`, `recordCount`, `recordLengthBytes`, `startByte` e `endByteInclusive`. A largura inclui o LF; a massa é ASCII UTF-8 e sempre usa LF.

`OutputEvent` contém `eventId` SHA-256 de `fileId/chunkId/recordNumber`, os IDs do job, `recordNumber`, `byteOffset` e `payload.raw` sem LF. Entregas são at-least-once: consumidores devem deduplicar por `eventId`.
