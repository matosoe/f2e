# ADR 0008 — Isolamento e capacidade por prefixo

**Status:** aceito em 2026-09-05

## Contexto

Prefixos atualmente compartilham fila de chunks, Workers e filas de saída. Isso
impede garantir capacidade de processamento por cliente e cria risco de
interferência: um volume de A pode atrasar B. A autorização por campo da mensagem
(`prefixId` escrito pelo produtor) não prova a identidade do remetente.

## Decisão

### Recursos dedicados por prefixo

Cada prefixo protegido recebe recursos próprios:

| Recurso | Dedicado por prefixo |
|---|---|
| Fila de intake (entrada autorizada) | Sim |
| Fila de chunks e DLQ | Sim |
| Worker Lambda e role IAM | Sim |
| Fila de saída | Sim |
| Organizer Lambda (quando necessário para capacidade de admissão) | Sim |

Código binário compartilhado; configuração, IAM e filas separados.

### Autorização

- `prefixId` é identificador estável derivado da configuração, não valor aceito
  do body da mensagem.
- Permissões derivam de role, origem autenticada e fila de entrada vinculada ao
  prefixo.
- Uma mensagem SQS não prova a identidade do produtor por campo escrito por ele;
  produtores são restritos na queue policy/IAM.
- Bucket/key de cada mensagem são validados contra o prefixo vinculado na
  admissão.

### Sobreposição de prefixos

- Prefixos sobrepostos têm uma única regra canônica de roteamento, testada.
- Nenhuma entrada explícita pode selecionar outro prefixo ou URL de saída.

### Capacidade e quota

- `maxActiveJobs` por prefixo, com reserva atômica vinculada à admissão.
- Capacidade separada de quota de admissão (jobs simultâneos) e concorrência de
  execução (Workers).
- Admissão acima da capacidade aguarda com backoff controlado; sem redrive por
  saturação normal.
- Reserva/liberação de capacidade é idempotente e recuperável.
- Deduplicação (mesmo arquivo/versão) não consome nova vaga.
- Prefixos com recursos dedicados não bloqueiam uns aos outros.

### Isolamento do ledger

- Para isolamento estrito, usar tabela DynamoDB por prefixo ou comprovar
  políticas de chaves e acesso via IAM; `prefixId` no item sozinho não isola IAM.

### Terraform

- Módulo Terraform com `for_each` de prefixo provê intake, Organizer (opcional),
  chunks/DLQ, Worker/role e output.
- Migração com `terraform state mv` e preservação de filas antes de qualquer
  `replace` ou `destroy`.

## Consequências

- Custo adicional proporcional ao número de prefixos protegidos.
- Quotas regionais (Lambda, SQS, DynamoDB) permanecem globais e precisam de
  planejamento de capacidade explícito.
- Implementação incremental: T20 (autorização/roteamento), T21 (Terraform),
  T22 (quota de admissão).
