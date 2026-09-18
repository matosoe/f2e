# ADR 0009 — Implantação simples com recursos compartilhados

**Status:** aceito em 2026-09-16

## Decisão

Para implantações com um único prefixo, volumes equivalentes ou sem SLA
diferenciado, o F2E usa uma fila de chunks, um Worker, uma fila de saída e uma
role IAM compartilhados. `prefixId` continua no job e nos envelopes para
rastreabilidade. Esta decisão prioriza simplicidade de implantação, operação e
custo: há menos recursos Terraform, alarmes e permissões a manter.

## Trade-off

Um pico de um prefixo pode atrasar os demais; não há garantia de capacidade
isolada. Portanto não é adequada quando há risco de monopolização, SLAs
diferentes ou necessidade de fronteira de segurança/capacidade.

Para um fluxo crítico, a alternativa é criar um F2E dedicado de ponta a ponta:
seu próprio intake, Worker, filas, ledger/configuração, monitoramento e ciclo
de deploy. Não se deve reintroduzir isolamento parcial dentro desta implantação
simples.

## Consequências

O módulo `f2e-prefix` e os recursos por prefixo são removidos. Esta decisão
descarta o modelo de recursos dedicados e seu catálogo Terraform de prefixos.
A implantação compartilhada foi escolhida por simplicidade operacional, menor
custo e menor superfície de Terraform, IAM e alarmes. O SSM permanece para
resolver e registrar o contrato por prefixo.
