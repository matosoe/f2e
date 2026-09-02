# ADR 0004 — Status dos formatos

**Status:** aceito em 2026-09-01

## Decisão

Formatos de produção: `fixed-width`, `text`, `jsonl` e `ndjson`.

Formatos em preview, sujeitos a limites explícitos e testes de boundary: `csv`
e JSON array. `multi-line` e `binary` permanecem não suportados para publicação
de produção até que framing/limites e contrato sejam homologados.

## Consequências

O organizer deve rejeitar cedo configurações que tentem usar os formatos não
suportados fora do modo explicitamente habilitado.
