# ADR 0007 — Ledger compartilhado e backlog via SQS

**Status:** aceito em 2026-09-17

## Decisão

O DynamoDB permanece uma única tabela compartilhada, mas o controle de quota e
admissões em espera foi removido por não fazer parte do fluxo de produção.

### Decisão anterior descartada

A reserva de vagas por prefixo, os itens `WAITING` e o agendador de admissão
justa foram descartados. As filas SQS e suas DLQs já absorvem o backlog; remover
esse fluxo reduz a complexidade operacional e de recuperação sem afetar a
identidade do arquivo ou o protocolo de eventos.

## Consequências

O backlog é tratado pelas filas SQS e respectivas DLQs. A identidade do arquivo
e o protocolo de eventos não dependem de uma quota de admissão.
