# ADR 0002 — Ledger de jobs e chunks

**Status:** aceito em 2026-09-01

## Decisão

O core expõe uma porta de ledger. A plataforma AWS a implementa com DynamoDB,
usando escritas condicionais e operações idempotentes. O core não depende do
SDK AWS nem de Step Functions.

## Consequências

O ledger registra planejamento, início, conclusão e falha de job/chunk, com
contador de tentativas e trilha de replay. A tabela, chave KMS, retenção e
alarms são parametrizados no Terraform; os valores corporativos finais ainda
dependem de definição de retenção e classificação de dados.
