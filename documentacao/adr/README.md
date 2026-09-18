# Índice de ADRs

Architecture Decision Records (ADRs) registram decisões duradouras do F2E,
seu contexto e suas consequências. Decisões substituídas foram consolidadas no
registro vigente; o histórico dos arquivos removidos permanece no Git.

| ADR | Status | Decisão |
|---|---|---|
| [0001](0001-identidade-de-eventos.md) | Aceito | Separar a identidade da entrega (`eventId`) da identidade do registro de origem (`sourceRecordId`). |
| [0002](0002-ledger-de-jobs.md) | Aceito | Usar DynamoDB como implementação AWS do ledger idempotente de jobs e chunks. |
| [0003](0003-url-pre-assinada.md) | Aceito | Tratar URL pré-assinada como credencial transitória de ingestão. |
| [0004](0004-tres-modos-de-delimitacao.md) | Aceito | Limitar o processamento a `text`, `json` e `multi-line`. |
| [0005](0005-identidades-e-estados.md) | Aceito | Definir identidades, estados e snapshots de configuração na admissão. |
| [0006](0006-modo-bundle.md) | Aceito | Permitir saída `single` ou `bundle` com opt-in explícito. |
| [0007](0007-ledger-compartilhado-e-backlog-sqs.md) | Aceito | Manter ledger compartilhado e tratar backlog pelas filas SQS e DLQs. |
| [0008](0008-modulo-go-e-automacao-canonica.md) | Aceito | Usar um módulo Go e entradas canônicas para build e automação. |
| [0009](0009-implantacao-simples-compartilhada.md) | Aceito | Usar recursos compartilhados para implantação simples; dedicar um F2E inteiro quando necessário. |
| [0010](0010-ssm-e-breaking-glass.md) | Aceito | Manter SSM dinâmico para ajustes rápidos e auditáveis em incidentes. |
