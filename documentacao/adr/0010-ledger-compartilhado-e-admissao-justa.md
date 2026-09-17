# ADR 0010 — Ledger compartilhado e admissão justa por prefixo

**Status:** substituído em 2026-09-17

## Decisão

O DynamoDB permanece uma única tabela compartilhada, mas o controle de quota e
admissões em espera foi removido por não fazer parte do fluxo de produção.

## Consequências

O backlog é tratado pelas filas SQS e respectivas DLQs. A identidade do arquivo
e o protocolo de eventos não dependem de uma quota de admissão.
