# ADR 0007 — Modo de saída bundle (múltiplos envelopes por mensagem SQS)

**Status:** aceito em 2026-09-05

## Contexto

O F2E publica um envelope por mensagem SQS (`outputMode=single`). Para arquivos
com muitos registros pequenos, isso aumenta o número de chamadas `SendMessage`
e o custo SQS. Um modo `bundle` agrupa envelopes em uma única mensagem, mas
requer contrato externo versionado e opt-in explícito do consumidor.

## Decisão

### Modos

| Modo | Semântica |
|---|---|
| `single` | Um `Envelope v2` por mensagem SQS. Comportamento atual. Padrão. |
| `bundle` | Uma mensagem SQS contendo lista de `Envelope v2` completos, cada qual com seu próprio ID. Contrato externo versionado. |

### Contratos do bundle

- Schema/version próprios (`bundleSchemaVersion`) distinguem wrapper de envelopes
  internos.
- Cada envelope interno preserva `eventId` e `sourceRecordId` completos.
- `transactionId` heterogêneo (de envelopes de origens distintas) não é
  promovido a atributo da mensagem wrapper.
- Consumidores devem fazer opt-in explícito por configuração de prefixo
  (`outputMode=bundle`); consumidores do modo `single` não recebem bundles.

### Limites

Limits separados para:
- `maxEventBytes`: tamanho máximo do envelope individual serializado.
- `maxMessageBytes`: tamanho máximo da mensagem SQS (padrão conservador: 256 KiB;
  expansível até 1 MiB com validação completa e fila compatível).
- `maxEnvelopesPerMessage`: máximo de envelopes por mensagem bundle.
- 10 mensagens por chamada `SendMessageBatch` e 1 MiB na soma do lote (limites SQS).

Envelope maior que `maxEventBytes` é erro explícito; nunca truncado.
Contabilização inclui envelope externo, escaping UTF-8 e atributos de mensagem.

### Agrupamento

- Somente registros do mesmo chunk, job, prefixo e configuração.
- Fronteiras determinadas por ordem, quantidade e bytes; não por ordem de
  conclusão de goroutines.
- IDs preservados em retry; sem reordenação.

### Deduplicação pelo consumidor

- Consumidor deve dedupliar pelo `eventId` de cada envelope interno.
- Retry do bundle inteiro é válido; envelopes já processados são descartados
  pela deduplicação.

## Consequências

- Configuração `outputMode` adicionada ao `PrefixConfiguration` (T15).
- O publicador streaming determinístico é implementado em T16.
- A suíte E2E valida os mesmos IDs em modo single e bundle (T18).
- A documentação de migração declara que consumidores single não precisam mudar
  para continuar recebendo; bundle requer atualização explícita.
