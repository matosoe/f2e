# Propostas de simplificação do F2E

Este documento lista propostas independentes para reduzir a complexidade
operacional e de infraestrutura do F2E sem remover nenhum requisito funcional:
conversão de arquivo em eventos, processamento paralelo por chunks, rastreio no
ledger, sinalização de conclusão e recuperação via retries e DLQs.

Cada proposta indica o que é eliminado, o que a substitui e quais trade-offs
precisam ser validados antes de adotar.

---

## P1 — Eliminar a Lambda Completion Publisher e o DynamoDB Streams

### Complexidade atual

- DynamoDB Streams ativado com `NEW_AND_OLD_IMAGES`.
- Lambda Completion Publisher disparada pelo Stream.
- Itens `COMPLETION_INTENT` com outbox (`intentPending`) no ledger.
- GSI `pending-intents-index` para recuperação de intents pendentes.
- EventBridge reprocessa intents não entregues a cada 1–5 min.

### Proposta

Ao encerrar o último chunk, o próprio Worker detecta atomicamente que todos os
chunks estão concluídos (usando a contagem já mantida no item de job) e publica
diretamente na fila `completion-events` como parte da mesma invocação.

Para garantir at-least-once na entrega do evento de conclusão, o Worker grava
um flag `completionPublished` no item de job antes de enviar para o SQS. Se a
Lambda for reexecutada, o flag impede dupla publicação; se o envio SQS falhar, o
chunk ainda está marcado como concluído e um retry da detecção pode reenviar.

### O que é removido

| Recurso | Terraform |
|---|---|
| Lambda `completion-publisher` | `aws_lambda_function.completion_publisher` |
| DynamoDB Streams | `stream_enabled`, `stream_view_type` |
| GSI `pending-intents-index` | atributo `intentPending` e índice |
| EventBridge rule de reprocessamento de intents | `aws_cloudwatch_event_rule` de publisher |
| Itens `COMPLETION_INTENT` do ledger | simplifica esquema DynamoDB |

### Requisito mantido

Sinalização de término em fila separada com resultado do job (COMPLETED,
WITH_REJECTIONS, FAILED, REJECTED).

### Trade-off

O envio da conclusão passa para o caminho crítico do Worker. Se o Worker
falhar após marcar todos os chunks mas antes de enviar ao SQS, um retry (via
DLQ ou replay explícito) precisa relançar a detecção. Isso é análogo ao
comportamento atual do outbox, mas sem a Lambda extra.

---

## P2 — Eliminar o mecanismo de WAITING e o EventBridge de admissão

### Complexidade atual

- Itens `WAITING#<fileId>` no DynamoDB quando `maxActiveJobs` está saturado.
- GSI `waiting-admissions-index` com `waitingAdmission` + `queuedAt`.
- EventBridge dispara o Organizer a cada 1 min para liberar admissões em espera.
- Organizer trata dois caminhos: admissão normal e dequeue de WAITING.

### Proposta

Remover o controle de `maxActiveJobs` por prefixo ou substituí-lo por um limite
de concorrência reservada na própria Lambda Worker (parâmetro já existente:
`reserved_concurrency`). Quando a quota da conta não suporta mais Workers
simultâneos, o SQS mantém os chunks em fila naturalmente; a mensagem de intake
permanece visível até que o Organizer conclua ou receba um retry via
`visibility_timeout`.

Para prefixos que precisam de back-pressure explícita, a alternativa é usar
`maxReceiveCount` + redrive DLQ com reprocessamento manual, em vez de manter
estado WAITING no DynamoDB.

### O que é removido

| Recurso | Onde |
|---|---|
| Itens `WAITING#<fileId>` | esquema DynamoDB |
| GSI `waiting-admissions-index` | `dynamodb.tf` |
| Atributos `waitingAdmission`, `queuedAt` | `dynamodb.tf` |
| EventBridge rule de liberação de WAITING | `monitoring.tf` / Terraform |
| Caminho de dequeue no Organizer | `internal/organizer/` |

### Requisito mantido

Recuperação de trabalho técnico; arquivos que não podem ser admitidos
imediatamente não são perdidos.

### Trade-off

Perde a garantia de FIFO entre prefixos na fila de espera. O back-pressure passa
a depender de configuração de concorrência Lambda em vez de controle explícito no
ledger. Para ambientes com muitos prefixos e picos coordenados, a perda de
visibilidade do estado de espera pode dificultar a observabilidade.

---

## P3 — Substituir SSM Parameter Store por configuração em variável de ambiente

### Complexidade atual

- Organizer resolve a configuração por prefixo consultando o SSM a cada
  admissão (maior correspondência de prefixo).
- SSM é um serviço adicional com latência de leitura e custo por parâmetro.
- Mudanças no SSM afetam apenas novas admissões — a Lambda precisa ser
  reiniciada para invalidar o cache.

### Proposta

Codificar o mapeamento de prefixos como um JSON único em uma variável de
ambiente da Lambda Organizer (ex.: `F2E_PREFIX_CONFIG`). O Organizer carrega
o JSON na inicialização e resolve o prefixo mais longo em memória, sem chamada
de rede.

Para configurações que precisam mudar sem redeploy, uma alternativa intermediária
é um único objeto JSON em S3 lido na inicialização (e cacheado pelo tempo de
vida do container Lambda), o que mantém a separação de configuração e código sem
a complexidade do SSM.

### O que é removido

| Recurso | Onde |
|---|---|
| `aws_ssm_parameter` de configuração por prefixo | `ssm.tf` |
| Permissão IAM `ssm:GetParameter` do Organizer | `iam.tf` |
| Chamada de rede ao SSM no caminho de admissão | `internal/organizer/` |

### Requisito mantido

Configuração por prefixo com snapshot imutável no job (o snapshot é gravado
no ledger na admissão, independentemente de onde a config é lida).

### Trade-off

Mudanças de configuração exigem um novo deploy da Lambda (ou atualização do
objeto S3 + restart do container). Para a maioria dos cenários o SSM já
requer restart para invalidar o cache in-process, então a diferença prática
é pequena. Não adequado se há necessidade de alterar configs com frequência
alta em produção sem CI/CD.

---

## P4 — Consolidar recursos compartilhados e remover o isolamento por prefixo para implantações simples

### Complexidade atual

- `prefix_modules.tf` instancia um módulo por prefixo com filas de chunks,
  filas de saída, Worker Lambda e IAM role dedicados.
- O Terraform cresce linearmente com cada novo prefixo.
- ADR 0008 justificou o isolamento para evitar interferência entre prefixos e
  garantir capacidade dedicada.

### Proposta (somente quando há um único prefixo ou prefixos com volumes equivalentes)

Manter um único Worker Lambda compartilhado com uma única fila de chunks e
uma única fila de saída. O `prefixId` continua no corpo das mensagens para
rastreio no ledger; o roteamento de saída distingue prefixos por atributo da
mensagem SQS, sem filas separadas.

Essa configuração é equivalente ao estado anterior à ADR 0008 e é adequada
quando:
- há apenas um prefixo ativo, ou
- os prefixos têm volumes similares e não há SLA diferenciado entre eles.

### O que é removido

| Recurso | Onde |
|---|---|
| Módulo `f2e-prefix` e `prefix_modules.tf` | Terraform |
| Filas de chunks e saída por prefixo | SQS |
| Workers e roles IAM por prefixo | Lambda + IAM |
| Configuração `prefix_worker_config` e `prefix_worker_effective` | `variables.tf`, `locals.tf` |

### Requisito mantido

Processamento paralelo por chunks, publicação de eventos por registro,
rastreio no ledger por prefixo.

### Trade-off

Perde o isolamento de capacidade entre prefixos. Um pico em um prefixo pode
atrasar outro. Não adequado quando há SLAs diferentes por cliente/prefixo
ou quando há risco de um prefixo monopolizar concorrência.

---

## P5 — Simplificar o esquema DynamoDB combinando P1 e P2

As propostas P1 e P2, quando combinadas, eliminam quatro dos cinco tipos de
item e dois dos três GSIs presentes na tabela atual.

### Esquema atual

| Tipo de item | PK | SK | GSI |
|---|---|---|---|
| JOB aggregate | `JOB#<jobId>` | `JOB` | `status-time-index` |
| CHUNK | `JOB#<jobId>` | `CHUNK#<n>` | — |
| QUOTA | `QUOTA#<prefixId>` | `QUOTA` | — |
| WAITING | `WAITING#<fileId>` | `WAITING` | `waiting-admissions-index` |
| COMPLETION_INTENT | `JOB#<jobId>` | `COMPLETION_INTENT#<n>` | `pending-intents-index` |

### Esquema simplificado (P1 + P2)

| Tipo de item | PK | SK | GSI |
|---|---|---|---|
| JOB aggregate | `JOB#<jobId>` | `JOB` | `status-time-index` |
| CHUNK | `JOB#<jobId>` | `CHUNK#<n>` | — |

DynamoDB Streams pode ser desabilitado. A tabela fica com um único GSI
(`status-time-index`) para consultas operacionais.

### Requisito mantido

Rastreio completo de admissão, chunks, contadores e estado terminal.

---

## P6 — Eliminar o EventBridge inteiramente (dependente de P1 e P2)

O EventBridge hoje serve dois papéis:

| Regra | Frequência | Propósito |
|---|---|---|
| Liberar WAITING | 1 min | Dequeue de admissões em espera (eliminado por P2) |
| Reprocessar intents | 1–5 min | Reenviar COMPLETION_INTENTs pendentes (eliminado por P1) |

Com P1 e P2 aplicados, nenhum dos dois papéis existe mais. O EventBridge pode
ser removido do módulo Terraform.

### O que é removido

- `aws_cloudwatch_event_rule` (ambas as regras)
- `aws_cloudwatch_event_target` e permissões Lambda associadas
- Lógica de handling de eventos EventBridge no Organizer e no Publisher

### Requisito mantido

Recuperação automática de trabalho perdido — passa a depender de retries SQS
e DLQs, que já são requisito de infraestrutura.

---

## Resumo de impacto por proposta

| Proposta | Lambdas removidas | Filas SQS removidas | DynamoDB | Terraform simplificado |
|---|---|---|---|---|
| P1 — sem Completion Publisher | 1 Lambda | — | −2 GSIs, −1 tipo de item, Streams off | sim |
| P2 — sem WAITING | — | — | −1 GSI, −1 tipo de item | sim |
| P3 — config em env var | — | — | — | remove SSM resources |
| P4 — sem isolamento por prefixo | N Workers → 1 | N×2 → 2 | — | remove prefix_modules.tf |
| P5 — esquema DynamoDB (P1+P2) | — | — | −3 tipos de item, −2 GSIs | sim |
| P6 — sem EventBridge (P1+P2) | — | — | — | remove event rules |

As propostas P1, P2, P5 e P6 formam um conjunto coeso: eliminam o Completion
Publisher, o mecanismo de back-pressure via ledger e o agendador periódico,
resultando em uma arquitetura com duas Lambdas operacionais (Organizer e Worker),
dois tipos de item no DynamoDB e zero dependência de Streams ou EventBridge.

P3 e P4 são independentes e podem ser adotadas separadamente conforme o
contexto operacional.
