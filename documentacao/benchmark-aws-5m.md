# Benchmark E2E AWS de 5 milhões de registros

Este benchmark é apartado da suíte funcional e precisa ser chamado
explicitamente. Seu perfil inicial representa o pior caso de empacotamento:
cada linha vira exatamente um envelope e uma mensagem física no SQS.

```bash
./automacao/benchmark-aws-5m.sh
```

O harness sobe um ambiente `development` descartável, aplica limite de
concorrência de **10 Workers** no poller SQS e executa os casos em sequência:

1. smoke test com 10.000 linhas;
2. somente se o smoke test for integralmente validado, teste com 5.000.000 de
   linhas.

Cada linha tem 100 bytes incluindo LF. O arquivo completo possui 500.000.000
bytes. O smoke usa 10 chunks de 1.000 registros; o caso completo usa 100 chunks
de 50.000 registros, mantendo dez Workers ocupados sem obrigar o Organizer a
fazer milhares de buscas de fronteira no S3. O envio usa `SendMessageBatch`
com até dez entradas, mas cada entrada continua sendo uma mensagem SQS
independente com apenas um envelope.

## Segurança e isolamento

O script recusa contas diferentes de `aws_account_id` e ambientes diferentes
de `development` no `aws.local.tfvars`. Como a topologia atual ainda mantém o
Worker legado e Workers por prefixo sobre a fila compartilhada de chunks, o
benchmark habilita apenas o Worker de `example-text` e desabilita os demais
event-source mappings durante a execução. Por padrão, chama
`parar-ambiente-aws.sh` ao terminar ou falhar.

Na conta da execução inicial, a cota regional de concorrência é 10 e a AWS
exige manter 10 unidades não reservadas. Por isso o Worker fica sem reserva
dedicada (`ReservedConcurrentExecutions = -1`) e o teto exato de 10 é imposto
por `ScalingConfig.MaximumConcurrency` no event-source mapping.

Para manter o ambiente temporariamente após o teste:

```bash
BENCHMARK_KEEP_ENVIRONMENT=1 ./automacao/benchmark-aws-5m.sh
```

## Métricas e artefatos

Os resultados são gravados em
`automacao/resultados/benchmark-aws-5m/<run-id>/` (diretório ignorado pelo Git):

- `report.json`: resultado consolidado;
- `10k/report.json` e `5m/report.json`: duração, throughput e invariantes;
- `worker-configuration.json`: memória, timeout, arquitetura, variáveis e
  concorrência reservada da Lambda;
- `event-source-mappings.json`: prova do limite de 10 Workers;
- `terraform-benchmark.tfvars.json`: overrides efetivamente aplicados depois
  do `aws.local.tfvars`;
- `*/cpu-summary.json`: CPU de processo por invocação, média, p95 e pico;
- `*/memory-summary.json`: memória máxima dos eventos `platform.report`, média,
  p95 e pico;
- `*/cloudwatch-*.json`: métricas nativas de invocações, concorrência, duração,
  erros e throttles;
- `*/cloudwatch-logs.json`: eventos brutos usados nos cálculos;
- `run.log`: trilha integral da execução.

A validação exige status `COMPLETED`, todos os chunks concluídos, zero registros
rejeitados/ignorados e igualdade entre linhas lidas, envelopes publicados e
mensagens publicadas. Esta última igualdade é a evidência principal de uma
linha por mensagem; a contagem visível da fila é registrada apenas como
aproximada, conforme a semântica do SQS.

## Configuração opcional

```bash
BENCHMARK_SMOKE_RECORDS_PER_CHUNK=1000 \
BENCHMARK_FULL_RECORDS_PER_CHUNK=50000 \
BENCHMARK_WORKER_CONCURRENCY=10 \
BENCHMARK_WORKER_MEMORY_MB=1024 \
BENCHMARK_LAMBDA_TIMEOUT=300 \
BENCHMARK_SQS_VISIBILITY_TIMEOUT=1800 \
BENCHMARK_PUBLISH_CONCURRENCY=1 \
BENCHMARK_TIMEOUT_SECONDS=14400 \
./automacao/benchmark-aws-5m.sh
```

O paralelismo de publicação dentro de cada Worker fica em 1 nesta primeira
versão; assim, o paralelismo medido vem dos dez ambientes Lambda. CPU é medida
no processo Linux via `getrusage`; memória vem do relatório da própria Lambda.

## Resultado da execução inicial

Execução válida: `20260908T004045Z`, conta `851779426177`, região
`us-east-1`. Os tempos abaixo são ponta a ponta a partir do início do upload
até o estado terminal do ledger. A massa já existia localmente, portanto a
geração levou apenas a validação do manifesto.

| Medida | 10 mil | 5 milhões |
|---|---:|---:|
| Tamanho do arquivo | 1.000.000 B | 500.000.000 B |
| Upload S3 | 2,281 s | 49,131 s |
| Tempo ponta a ponta | 11,501 s | 732,238 s |
| Vazão ponta a ponta | 869,49 msg/s | 6.828,38 msg/s |
| Chunks concluídos | 10/10 | 100/100 |
| Linhas lidas | 10.000 | 5.000.000 |
| Envelopes publicados | 10.000 | 5.000.000 |
| Mensagens físicas publicadas | 10.000 | 5.000.000 |
| Mensagens visíveis aproximadas no SQS | 10.000 | 5.000.000 |
| Registros rejeitados/ignorados | 0/0 | 0/0 |
| DLQs de intake/chunks | 0/0 | 0/0 |

### Configuração efetivamente medida

- Worker: `f2e-e2e-development-example-text-worker`;
- runtime/arquitetura: `provided.al2023`, `x86_64`;
- memória: 1.024 MB; armazenamento temporário: 512 MB;
- timeout observado no artefato desta execução: 60 s;
- concorrência reservada: nenhuma; teto do event-source mapping: 10;
- batch do mapping: 1 chunk por invocação;
- concorrência de publicação dentro de cada Worker: 1;
- saída: `single`, um envelope por mensagem física; chamadas em lotes de até
  dez mensagens via `SendMessageBatch`.

O máximo nativo de `ConcurrentExecutions` foi **10**. O CloudWatch registrou
zero `Errors` e zero `Throttles`. Os logs estruturados registraram exatamente
10 invocações no smoke e 100 no caso completo; a soma nativa de invocações do
caso completo retornou 98 porque duas amostras ficaram fora dos buckets de um
minuto delimitados pelo intervalo da consulta.

### CPU e memória do Worker

| Medida | 10 mil | 5 milhões |
|---|---:|---:|
| Invocações amostradas | 10 | 100 |
| CPU média por tempo de parede | 14,76% | 10,82% |
| CPU p95 | 17,77% | 11,86% |
| CPU pico | 18,89% | 12,34% |
| Memória máxima média | 43,5 MB | 44,73 MB |
| Memória p95/pico | 44/44 MB | 46/46 MB |
| Duração média da invocação | 1.499,23 ms | 55.145,67 ms |
| Duração máxima da invocação | 1.564,78 ms | 58.874,49 ms |
| Duração faturada acumulada | 16.077 ms | 5.514.890 ms |

A CPU é o tempo de processo (`getrusage`) dividido pelo tempo de parede de
cada invocação; não é uma métrica nativa de utilização da Lambda. A memória é
a métrica `maxMemoryUsedMB` dos eventos nativos `platform.report`.

### Conclusões e oportunidades

A cardinalidade foi preservada no pior cenário de envelope: para 5 milhões de
linhas, o ledger confirmou 5 milhões de envelopes e 5 milhões de mensagens
físicas. A fila também convergiu para a estimativa de 5 milhões e nenhuma DLQ
recebeu mensagens.

O Worker usou no pico 46 MB, apenas 4,5% dos 1.024 MB configurados. CPU também
ficou baixa, coerente com uma carga dominada pelas chamadas de rede ao SQS.
Há forte oportunidade de testar 512 MB e 256 MB, comparando duração e custo,
pois reduzir memória também reduz CPU disponível na Lambda. Não é seguro
concluir a configuração ótima usando somente este ponto.

Com chunks de 50 mil, a maior invocação levou 58,874 s e ficou perigosamente
próxima do timeout de 60 s observado. O harness foi endurecido depois da
medição para aplicar explicitamente timeout de 300 s e visibilidade SQS de
1.800 s por um var-file de maior precedência. Alternativamente, um próximo
cenário pode reduzir os chunks para 40 mil registros e manter timeout menor.

Durante a preparação, 5.000 chunks de 1.000 linhas fizeram o Organizer exceder
60 s realizando buscas de fronteira no S3. O perfil final de 100 chunks removeu
esse gargalo e ainda sustentou dez Workers. Também foi necessário corrigir a
conclusão concorrente do ledger: conflitos transacionais agora recebem backoff
e um token novo somente após cancelamento explícito, mantendo idempotência nos
retries de transporte.
