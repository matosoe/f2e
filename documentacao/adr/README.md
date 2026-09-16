# Índice de ADRs

Architecture Decision Records (ADRs) registram decisões duradouras do F2E,
seu contexto e suas consequências. Um ADR supersedido permanece neste índice
para preservar o histórico; mudanças de implementação que não alteram uma
decisão não criam ADR novo.

| ADR | Status | Decisão |
|---|---|---|
| [0001](0001-identidade-de-eventos.md) | Aceito | Separar a identidade da entrega (`eventId`) da identidade do registro de origem (`sourceRecordId`). |
| [0002](0002-ledger-de-jobs.md) | Aceito | Usar DynamoDB como implementação AWS do ledger idempotente de jobs e chunks. |
| [0003](0003-url-pre-assinada.md) | Aceito | Tratar URL pré-assinada como credencial transitória de ingestão. |
| [0004](0004-formatos-suportados.md) | Supersedido por 0005 | Catálogo anterior de formatos suportados. |
| [0005](0005-tres-modos-de-delimitacao.md) | Aceito | Limitar o processamento a `text`, `json` e `multi-line`. |
| [0006](0006-identidades-e-estados.md) | Aceito | Definir identidades, estados e snapshots de configuração na admissão. |
| [0007](0007-modo-bundle.md) | Aceito | Permitir saída `single` ou `bundle` com opt-in explícito. |
| [0008](0008-isolamento-por-prefixo.md) | Supersedido por 0013 | Modelo anterior de isolamento por prefixo. |
| [0010](0010-ledger-compartilhado-e-admissao-justa.md) | Aceito | Manter ledger compartilhado na PoC e admitir jobs de forma justa por prefixo. |
| [0011](0011-catalogo-terraform-de-prefixos.md) | Supersedido por 0013 | Catálogo da topologia dedicada por prefixo. |
| [0012](0012-modulo-go-e-automacao-canonica.md) | Aceito | Usar um módulo Go e entradas canônicas para build e automação. |
| [0013](0013-implantacao-simples-compartilhada.md) | Aceito | Usar recursos compartilhados para implantação simples; dedicar um F2E inteiro quando necessário. |
| [0014](0014-ssm-e-breaking-glass.md) | Aceito | Manter SSM dinâmico para ajustes rápidos e auditáveis em incidentes. |

O ADR 0009 não existe porque nenhum registro foi criado para essa numeração.
