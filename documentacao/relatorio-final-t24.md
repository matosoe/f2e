# Relatório final — Evolução F2E: três modos, operação e performance

Data: 2026-09-05. Elaborado ao término da sessão de implementação T12–T24.

---

## 1. Resumo executivo

Todas as 24 tarefas do plano `evolucao_f2e_tres_modos_operacao_e_performance.md`
foram implementadas localmente. O build compila sem erros, os testes unitários
passam, e os testes de integração com LocalStack estão prontos mas aguardam
infra disponível. Nenhum deploy AWS foi executado; nenhuma autorização de apply
foi solicitada nem concedida.

---

## 2. Concluído em código e testes locais

| Tarefa | Entregável principal |
|--------|----------------------|
| T01 | Tipos removidos (`fixed-width`, `csv`, `binary`, `jsonl`, `ndjson`); três modos (`text`, `json`, `multi-line`) com validação explícita |
| T02 | Remoção de `CRLF` do `data.raw`; terminador final não gera registro extra; `IsLastPhysical` e `IgnoredPhysicalLines` nos contadores |
| T03 | Suporte CRLF no `multi-line`; linhas físicas sem prefixo aceito são ignoradas e contadas; limite `maxBytesPerRecord` aplicado |
| T04 | Parser `json` array-path com limitador de busca `F2E_JSON_ARRAY_SEARCH_BYTES`; array vazio conclui com zero chunks |
| T05 | Variável de ambiente `F2E_DATA_TYPES_ALLOWED` (whitelist); erro explícito para tipos não suportados; remoção do `fixed-width` do Terraform |
| T06 | `ConfigSnapshot` com versão SSM, hash e `responsible`; campo `environment`; `ChunkJob.Configuration` propagado |
| T07 | Máquina de estados DynamoDB completa: `RECEIVED → VALIDATING → PLANNING → SCHEDULING → PROCESSING → COMPLETED/REJECTED/FAILED`; `advanceJob` com anti-regressão de terminal |
| T08 | Planejamento com `Plan`/`MarkScheduled`; selagem de manifesto (`manifestSealed`); `AcquireChunk`/`StartChunk`; `BeginValidation`/`BeginPlanning` |
| T09 | `RecordProcessor` com `RecordDecision` explícito (`publish`/`reject`/`ignore`); motivos obrigatórios; contadores por motivo |
| T10 | `Plan` write-by-chunk com selagem idempotente; reconciliação após checkpoint parcial; LocalStack `TestLedgerManifestScheduleCheckpoints` |
| T11 | Contagens `RecordsProduced/Rejected/Ignored` por chunk; agregação no job em transação DynamoDB; duplicatas não somam novamente |
| T12 | `CompletionIntent` outbox com `WriteCompletionIntent` (idempotente), `PendingCompletionIntents` (GSI esparso), `MarkIntentDelivered`; `eventId` SHA-256 determinístico; JSON Schema `completion-v1.schema.json` |
| T13 | Lambda `completion-publisher`: polling do GSI, `SendMessageBatch` para fila de conclusão, `MarkIntentDelivered` pós-confirmação; retry parcial idempotente |
| T14 | `FinalizeJob` + `RejectJob` gravam `CompletionIntent` no commit; `completion-publisher` é triggado via DynamoDB Streams |
| T15 | `F2E_BUNDLE_MAX_ENVELOPES` / `maxMessageBytes`; `bundlePacker` streaming determinístico; `bundleID` = sorted eventIds SHA-256; consumer E2E expande bundle |
| T16 | `bundlePacker` com limite por contagem e bytes; flush parcial e final; testes `TestBundlePackerSingleFlush/MultipleMessages/SizeLimit` |
| T17 | `F2E_PUBLISH_CONCURRENCY`; `concurrentSender` com semáforo e goroutines; `submit`/`wait`; testes de limite de concorrência |
| T18 | `ConsumeAllConcurrent` com pool de workers, long-polling, delete-batch com retry, deduplicação de `eventId` |
| T19 | Tags Gherkin `@smoke/@regression/@load`; `buildspec-e2e.yml` CodeBuild; `benchmark-matrix.sh` (memória × chunk size) |
| T20 | Campos `PrefixID`, `OutputQueueURL`, `AllowedSourceARNs` em `PrefixConfiguration`; `ValidatePrefixConfiguration` exportado; Worker aplica override de `OutputQueueURL` |
| T21 | Módulo Terraform `f2e-prefix`: fila SQS output+DLQ com TLS, IAM role/policy scoped, Lambda com `reserved_concurrent_executions`, `scaling_config`; `prefix_modules.tf` via `for_each` |
| T22 | `ReserveSlot`/`ReleaseSlot` atômicos em DynamoDB; `ErrQuotaExceeded` sem BatchItemFailure; `releaseQuotaIfNeeded` em `RejectJob`/`FinalizeJob`; 3 testes LocalStack |
| T23 | `MessageDispatcher` (roteamento por jobId); `Fork()` por cenário; `NewScenarioInitializerWithDispatcher`; `E2E_CONCURRENCY`; `BenchmarkComparison` em `RunReport` |
| T24 | Feature `failure_recovery.feature`; runbooks de quota saturation, outbox e rollback bundle-safe; contratos de conclusão e prefixo; exemplos SSM; este relatório |

---

## 3. Testes executados localmente

| Pacote | Resultado | Observação |
|--------|-----------|------------|
| `./internal/domain/f2e/...` | ✅ PASS | Todos os testes unitários |
| `./internal/domain/lineio/...` | ✅ PASS | |
| `./internal/domain/multiline/...` | ✅ PASS | |
| `./internal/application/organizer/...` | ✅ PASS | 27 testes incluindo T20 |
| `./internal/application/worker/...` | ✅ PASS | Inclui T17 concurrentSender |
| `./internal/adapter/inbound/s3event/...` | ✅ PASS | |
| `./internal/platform/config/...` | ✅ PASS | |
| `./internal/platform/aws/...` | ✅ PASS | Testes LocalStack skipped sem `F2E_TEST_AWS_ENDPOINT` |
| `./e2e/...` | ✅ PASS (build + strict-sanity) | `TestE2E` skipped sem LocalStack |
| `terraform validate` | ✅ PASS | `init -backend=false` |

---

## 4. Resultados AWS

Nenhuma execução AWS foi realizada nesta sessão. O plano proíbe deploy, apply,
destruição de recursos ou alterações em parâmetros remotos sem autorização
explícita. Os artefatos abaixo estão prontos para homologação quando autorizada:

- ZIPs de Lambda gerados por `automacao/package-lambdas.sh`
- Plano Terraform (`terraform plan -var-file=...`) deve ser revisado antes do apply
- E2E suite com `F2E_E2E_TARGET=aws` e tags `@smoke` e `@regression`
- `automacao/benchmark-matrix.sh` para medição de memória × chunk size

**Figura de 50 minutos reportada anteriormente:** não foi reproduzida nem medida
nesta sessão. As hipóteses são: drenagem sequencial SQS, ciclo de infraestrutura
Lambda e esperas de retry. A medição definitiva requer traces AWS com timestamps
do Worker.

---

## 5. Pendências externas

| Item | Motivo | Bloqueio |
|------|--------|----------|
| Testes de integração LocalStack | `F2E_TEST_AWS_ENDPOINT` não disponível neste ambiente | Requer LocalStack local ou CI com serviço LocalStack |
| Homologação AWS E2E | Plano proíbe deploy sem autorização | Aguarda aprovação e credenciais do ambiente de teste |
| Medição de throughput e latência AWS | Requer traces de execução real | Bloqueado por homologação AWS |
| Benchmarks de memória × chunk | `benchmark-matrix.sh` requer Lambda provisionada | Bloqueado por homologação AWS |
| Alarmes CloudWatch com ações | `terraform/cloudwatch.tf` tem alarmes sem SNS action | Requer ARN de tópico SNS da conta-alvo |
| Migração de parâmetros SSM legados | `prefixId`, `outputQueueUrl`, `maxActiveJobs` são opcionais | Coordenação com owners de prefixos existentes |
| Deduplicação financeira cross-arquivo | Fora do escopo deste plano | Requer requisitos de negócio separados |
| Autoria autenticada CloudTrail | Captura de autor SSM requer acesso a CloudTrail da conta | Requer permissões adicionais |

---

## 6. Contratos e schemas produzidos

| Artefato | Caminho |
|----------|---------|
| Envelope v2 (JSON Schema) | `documentacao/schemas/envelope-v2.schema.json` |
| Bundle v1 (JSON Schema) | `documentacao/schemas/bundle-v1.schema.json` |
| Completion v1 (JSON Schema) | `documentacao/schemas/completion-v1.schema.json` |
| Contratos versionados | `documentacao/contratos.md` |
| Runbooks operacionais | `documentacao/runbooks.md` |
| Operação AWS | `documentacao/operacao_aws.md` |
| Operação local | `documentacao/operacao_local.md` |

---

## 7. Decisões técnicas relevantes registradas

- **Três modos apenas**: `text`, `json`, `multi-line`. Tipos legados geram erro
  explícito "unsupported data type".
- **Deduplicação por versão S3**: mesma combinação `bucket+key+VersionId` retorna
  `AlreadyCompleted` — não cria novo job.
- **Quota via DynamoDB condicional**: `ReserveSlot` usa `attribute_not_exists(activeJobs) OR activeJobs < :max` para garantir atomicidade sem locks de aplicação.
- **Rollback bundle-safe**: downgrade de `F2E_BUNDLE_MAX_ENVELOPES` deve ser
  coordenado com consumidores; mensagens em trânsito persistem no formato bundle.
- **Outbox de conclusão**: `CompletionIntent` sobrevive a crashes do publisher;
  `eventId` SHA-256 é estável durante retries do mesmo job.
- **Paralelismo E2E via dispatcher**: `MessageDispatcher` roteia mensagens SQS
  por `jobId`, eliminando cross-contamination entre cenários paralelos.
