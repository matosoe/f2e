# ADR 0006 — Identidades, estados e máquina de jobs

**Status:** aceito em 2026-09-05

## Contexto

O ledger atual usa `JobID` e `ChunkID` sem separar formalmente a identidade do
recebimento físico, do arquivo imutável e do registro lógico. Isso dificulta
deduplicação de admissão, rastreabilidade de replay e auditoria de conciliação.

## Decisão

### Identidades

| Identificador | Semântica |
|---|---|
| `receiptId` | Ocorrência de recebimento (notificação S3 ou entrada explícita). Gerado na admissão; uma nova entrega da mesma notificação reutiliza o mesmo receiptId pelo SQS messageId. |
| `fileId` | Identidade imutável da origem física: `SHA-256(bucket/key/versionId/etag/size)`. Não muda com chunking ou configuração. |
| `jobId` | Execução de processamento vinculada a um `fileId`. Admissão normal cria um job; replay explícito cria outro job vinculado ao anterior. |
| `sourceRecordId` | Identidade imutável do registro lógico: `SHA-256(fileId/byteOffset/schemaVersion)`. Estável com mudança de chunking, agrupamento SQS ou configuração. |

Regras:
- Uma notificação repetida ou entrada explícita da mesma versão do arquivo não
  cria novo job normal; vincula ao job existente.
- Mudança de configuração SSM não autoriza reprocessar a mesma versão do arquivo.
- No perfil corporativo, exigir S3 versionado; `VersionId` preservado desde a
  notificação até `HEAD/GET`; fontes sem identidade imutável são rejeitadas.
- Replay explícito cria novo `jobId` e novos `eventId`s; `sourceRecordId`
  permanece igual; o job anterior é referenciado.

### Máquina de estados do job

```text
RECEIVED -> VALIDATING -> PLANNING -> PROCESSING -> COMPLETED
                |             |           |
                +-------------+-----------+--> FAILED (falha técnica definitiva)
                |
                +--> REJECTED (arquivo/configuração inválidos)
```

- `COMPLETED` pode ter resultado `WITH_REJECTIONS` quando há rejeições de
  registros; essas rejeições não são falhas técnicas.
- Terminais (`COMPLETED`, `FAILED`, `REJECTED`) não regridem.
- Tentativa antiga não sobrescreve nova tentativa; planejamento/agendamento
  tardio não sobrescreve conclusão.

### Máquina de estados do chunk

```text
PENDING -> RUNNING -> COMPLETED
                |
                +--> RETRY_PENDING -> RUNNING -> ...
                |
                +--> FAILED
```

### Invariantes

- Com manifesto selado:
  `expectedChunks = pendingChunks + runningChunks + retryPendingChunks + completedChunks + failedChunks`
- Em chunk concluído:
  `recordsRead = recordsPublished + recordsRejected + recordsIgnored`
- `messagesPublished` é métrica separada (depende do modo single/bundle).
- Reenvios não aumentam totais lógicos.
- Arquivo falho informa progresso parcial com `countsComplete=false`.

### Configuração fixada na admissão

- Capturar: nome do parâmetro SSM, versão, hash do conteúdo, instante de
  leitura, `responsible` declarado e versão dos limites globais usados.
- Distinguir `responsible` declarado de autor autenticado; correlação com
  CloudTrail para auditoria de escrita.
- Replay reutiliza snapshot original por padrão.

## Consequências

- O domínio ganha tipos `Receipt`, `Job`, `Chunk` com tabela de transições
  válidas (T07).
- A persistência condicional implementa as invariantes via controle de versão e
  token de posse (T08).
- A admissão idempotente usa a identidade `fileId` + `VersionId` (T09).
- `ADR 0001` é complementado (não substituído) por este ADR para a parte de
  identidades de eventos.
