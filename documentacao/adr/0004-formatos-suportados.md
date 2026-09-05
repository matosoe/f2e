# ADR 0004 — Status dos formatos

**Status:** supersedido em 2026-09-05 pelo [ADR 0005](0005-tres-modos-de-delimitacao.md)

## Decisão

Formatos de produção: `fixed-width`, `text`, `jsonl`, `ndjson`, `csv`, JSON
array, `multi-line` e `binary`.

Todos os formatos permanecem sujeitos aos limites explícitos de arquivo, chunk
e evento e aos testes de boundary definidos pelo contrato.

## Consequências

O organizer deve rejeitar cedo tipos desconhecidos e configurações que excedam
os limites ou não forneçam o layout obrigatório de JSON array e multi-line.
