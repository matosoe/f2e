# ADR 0001 — Identidade de eventos e registros de origem

**Status:** aceito em 2026-09-01

## Contexto

O contrato anterior usava `eventId` determinístico para deduplicação, enquanto a
especificação de envelope exige que ele identifique uma ocorrência do evento.
Essas semânticas são distintas em uma entrega at-least-once.

## Decisão

- `eventId` é um identificador único de cada envelope publicado;
- `sourceRecordId` é determinístico, calculado de identidade imutável do objeto,
  posição física do registro e schema/version;
- retries de publicação preservam o mesmo `eventId`; um replay explícito cria
  novo `eventId`, mas preserva `sourceRecordId`;
- consumidores que precisarem deduplicar o registro físico devem usar
  `sourceRecordId`, não `eventId`.

## Consequências

O envelope público ganha `metadata.sourceRecordId`. A regra anterior de
deduplicação por `eventId` deixa de ser válida e a documentação e os testes de
compatibilidade devem declarar essa mudança como contrato versão 2.
