# ADR 0010 — SSM dinâmico para operação e breaking glass

**Status:** aceito em 2026-09-16

## Decisão

O F2E mantém o AWS Systems Manager Parameter Store como fonte dinâmica de
configuração de prefixos e limites globais. Não será adotada configuração fixa
em variáveis de ambiente.

## Justificativa

Em incidentes de produção, operadores autorizados precisam reduzir limites,
alterar parâmetros de processamento ou bloquear uma configuração de modo
tempestivo. O SSM permite esse procedimento de *breaking glass* sem aguardar
um novo build ou deploy. Cada admissão registra parâmetro, versão e hash no
ledger, preservando auditoria e impedindo que uma alteração afete job já aceito.

## Consequências

O acesso ao SSM continua protegido por IAM, CloudTrail e processo operacional
de emergência. Mudanças de topologia permanecem no Terraform; SSM não fornece
URLs arbitrárias de recursos como mecanismo de provisionamento.
