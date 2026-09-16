# Comandos para baseline E2E na AWS

Data: 2026-09-05. Parte do plano `evolucao_f2e_tres_modos_operacao_e_performance.md` — T02.

**Estes comandos não foram executados.** A medição de 50 minutos relatada anteriormente
não foi diagnosticada por traces e não é reproduzida aqui como baseline comprovado.
O baseline real será estabelecido na primeira rodada AWS autorizada (T19).

## Pré-requisitos

- Credenciais AWS com acesso aos recursos do ambiente de homologação.
- Runner CodeBuild (ou equivalente) na mesma região dos recursos (latência de rede próxima de zero para SQS/S3).
- Ambiente de homologação isolado, não-produção, com prefixos de teste dedicados.
- `E2E_METRICS_FILE` apontando para um caminho persistido entre steps do pipeline.

## Variáveis de ambiente necessárias

```bash
export F2E_E2E_TARGET=aws
export F2E_E2E_AWS_REGION=us-east-1          # substituir pela região real
export F2E_E2E_INPUT_BUCKET=f2e-input-hml    # bucket de homologação
export F2E_E2E_INTAKE_QUEUE_NAME=f2e-file-intake-hml
export F2E_E2E_OUTPUT_QUEUE_NAME=f2e-output-events-hml
export F2E_E2E_LEDGER_TABLE=f2e-job-ledger-hml
export F2E_E2E_INTAKE_DLQ_NAME=f2e-file-intake-dlq-hml
export F2E_E2E_CHUNK_DLQ_NAME=f2e-chunk-jobs-dlq-hml
export E2E_METRICS_FILE=/tmp/e2e-metrics-$(date +%Y%m%dT%H%M%S).json
```

## Rodada de smoke (cenários pequenos, sem @load)

```bash
# Executar da raiz do repositório com Git Bash:
# & 'C:\Program Files\Git\bin\bash.exe' -lc '...' no Windows

cd e2e
go test -v -timeout 30m -run TestE2E ./...
# Relatório gravado em $E2E_METRICS_FILE
```

## Rodada completa com carga

```bash
cd e2e
E2E_LOAD_TESTS=true go test -v -timeout 60m -run TestE2E ./...
```

## Rodada por tag (para isolar um tipo de arquivo)

```bash
cd e2e
E2E_TAGS="@text" go test -v -timeout 10m -run TestE2E ./...
```

## Coletar métricas de memória Lambda e concorrência (requer CloudWatch/CLI)

```bash
# Período da última rodada E2E (ajustar start/end com base no runId do relatório):
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=f2e-worker-hml \
  --start-time 2026-09-05T14:00:00Z \
  --end-time 2026-09-05T15:00:00Z \
  --period 60 \
  --statistics Average Maximum \
  --region us-east-1

aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name ConcurrentExecutions \
  --dimensions Name=FunctionName,Value=f2e-worker-hml \
  --start-time 2026-09-05T14:00:00Z \
  --end-time 2026-09-05T15:00:00Z \
  --period 60 \
  --statistics Maximum \
  --region us-east-1
```

## Coletar tempo de planejamento via X-Ray (se habilitado)

```bash
# Requer X-Ray habilitado no Lambda e permissões xray:GetTraceSummaries
aws xray get-trace-summaries \
  --start-time 2026-09-05T14:00:00 \
  --end-time 2026-09-05T15:00:00 \
  --filter-expression 'annotation.jobId = "JOBID_AQUI"' \
  --region us-east-1
```

## Observações

- **Relógio local vs. AWS**: os campos `waitMs`, `consumeMs`, etc. do relatório usam o
  relógio local do runner. Os campos `earliestEnvelopeCreatedAt` e `latestEnvelopeCreatedAt`
  usam o relógio do Worker Lambda. Não subtrair campos de clocks diferentes para inferir
  latência end-to-end — use X-Ray ou CloudWatch Logs Insights para isso.

- **Baseline**: o primeiro relatório produzido numa rodada AWS autorizada define o baseline.
  Otimizações (T17, T23) só são declaradas como comprovadas após comparação com o baseline.

- **Autorização**: execução com custo na AWS requer autorização explícita conforme o plano.
  Estes comandos são preparatórios para revisão; não há `apply`, `deploy` ou `destroy` aqui.
