# P3.10 — right-sizing de memória do Worker

## Resultado

A matriz foi implantada e executada na AWS em 8 de setembro de 2026. Nenhuma
das memórias avaliadas — 128, 256, 512 ou 1.024 MiB — é elegível para promoção
com a configuração atual de chunks de aproximadamente 5 MiB, publicação
interna serial e timeout de 60 segundos.

O limitante observado não é falta de memória. No ensaio de capacidade com
1.024 MiB, o maior consumo foi 64 MB, mas as nove invocações do Worker
terminaram por timeout aos 60.000 ms. A decisão do P3.10 fica, portanto,
bloqueada até que o trabalho por invocação ou o caminho de publicação seja
reduzido. A configuração de memória existente não foi alterada.

## Ambiente e método

| Parâmetro | Valor |
|---|---:|
| Conta AWS | `851779426177` |
| Região | `us-east-1` |
| Runtime / arquitetura | `provided.al2023` / `x86_64` |
| Memórias | 128, 256, 512 e 1.024 MiB |
| Timeout do Worker | 60 s |
| Concorrência máxima de Workers | 10 |
| Concorrência de publicação por Worker | 1 |
| Modo de saída | Uma mensagem SQS por linha |
| Lote da API SQS | 10 mensagens |
| Tamanho alvo do chunk | 5 MiB (`5.242.880` bytes) |
| Massa de capacidade | 500.000 linhas de 100 bytes; 9 chunks |
| Margem operacional de elegibilidade | p99 de até 48 s |

O harness executa primeiro uma massa smoke de 10.000 linhas e projeta seu p99
para o tamanho nominal de 5 MiB. Uma projeção acima do timeout absoluto de 60
segundos é registrada como `PREFLIGHT_REJECTED`, evitando iniciar uma carga
que já se mostrou incompatível com o timeout. Os pontos que passaram pelo
preflight foram então exercitados com 500.000 linhas.

## Resultados do smoke e do preflight

| Memória | Repetições smoke | p99 mediano | Faixa do p99 | Projeção mediana para 5 MiB | Pico de memória | Init mediano | CPU mediana¹ | Ciclos GC medianos | Pausa GC mediana | Decisão do preflight |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 128 MiB | 3 | 19.911 ms | 19.808–20.833 ms | 104.392 ms | 46 MB | 138,260 ms | 6,93% | 72 | 371,458 ms | Rejeitado nas 3 repetições |
| 256 MiB | 3 | 12.236 ms | 12.182–12.825 ms | 64.149 ms | 45 MB | 138,426 ms | 11,38% | 75 | 42,721 ms | Rejeitado nas 3 repetições |
| 512 MiB | 2 | 8.914–11.802 ms² | 8.914–11.802 ms | 46.733–61.878 ms | 45 MB | 131,759–134,844 ms | 12,79%³ | 75–77 | 21,219–26,689 ms | Uma rejeição; uma aprovação |
| 1.024 MiB | 1 | 10.013 ms | 10.013 ms | 52.495 ms | 45 MB | 134,317 ms | 12,49% | 75 | 10,038 ms | Aprovado |

¹ A métrica é tempo de CPU do processo dividido pelo tempo de parede; ela não
representa diretamente a porcentagem da vCPU provisionada pela Lambda.

² Com duas amostras, a faixa é apresentada em vez de atribuir uma mediana
estatisticamente significativa.

³ Valor da repetição de 512 MiB que passou pelo preflight; a outra repetição
registrou telemetria compatível, sem mudar a conclusão.

## Ensaios de capacidade

| Memória | Execuções iniciadas | Chunks reconhecidos pelo ledger | Mensagens físicas observadas na saída | Evidência do Worker | Resultado |
|---:|---:|---:|---:|---|---|
| 512 MiB | 1 | 1 de 9; 55.555 de 500.000 registros | 494.765 | O job permaneceu incompleto após 137 s | Falhou; execução interrompida após confirmar ausência de progresso confiável |
| 1.024 MiB | 1 | 0 de 9; 0 de 500.000 registros | 477.780 | 9 de 9 invocações de capacidade em timeout exato de 60.000 ms; pico de 64 MB | Falhou deterministicamente por timeout |

As contagens físicas da fila de saída muito superiores aos registros
confirmados no ledger mostram publicação parcial antes do timeout. Além de
invalidar o benchmark, isso cria risco de reprocessamento e duplicidade: a
invocação publica parte do chunk, expira sem confirmá-lo e pode ser repetida
pelo event source mapping.

As três execuções completas por ponto previstas no plano não foram forçadas.
128 e 256 MiB foram rejeitados em três preflights cada; 512 MiB falhou na
primeira carga que passou pelo preflight; e a própria configuração de 1.024
MiB falhou nas nove invocações da primeira carga. Repetir cargas conhecidamente
inválidas só ampliaria timeouts, retries, mensagens parciais e custo, sem criar
uma configuração elegível.

## Teste adicional — bundle máximo em 128 MiB

Em 8 de setembro de 2026 foi executado um teste isolado para verificar se o
gargalo anterior era causado principalmente pela quantidade de mensagens
físicas enviadas ao SQS. O Worker foi configurado para preencher cada bundle
até o limite de bytes da aplicação:

| Parâmetro | Valor |
|---|---:|
| Execução | `20260908T115738Z` |
| Memória / timeout | 128 MiB / 60 s |
| `outputMode` | `bundle` |
| `maxEnvelopesPerMessage` | `0` — sem teto por quantidade |
| `maxMessageBytes` | 256.000 bytes — 250 KiB |
| Publicação por Worker | Concorrência 1; até 10 mensagens por chamada SQS |
| Smoke | 10.000 registros de 100 bytes em um chunk |
| Massa pretendida após o smoke | 5 milhões de registros |

O empacotador considera o body JSON e os atributos da mensagem e só fecha o
bundle antes de ultrapassar 256.000 bytes. Portanto, a configuração solicitou
o máximo de registros que coubesse em cada mensagem, sem impor limite de
quantidade.

### Resultado observado

| Métrica | Bundle máximo | Baseline 128 MiB, uma mensagem por registro |
|---|---:|---:|
| Resultado do smoke de 10 mil | Timeout | Concluído |
| Duração do Worker | 60.000 ms | p99 mediano de 19.911 ms |
| Memória máxima | 76 MB | 46 MB |
| Init duration | 151,270 ms | mediana de 138,260 ms |
| Registros confirmados | 0 de 10.000 | 10.000 de 10.000 |
| Mensagens físicas na saída | 0 | 10.000 |

A invocação expirou antes de publicar o primeiro bundle. O job continuava em
`SCHEDULED`, com `0/1` chunk concluído, após 120 segundos. O fluxo foi
interrompido nesse ponto para evitar retries; por isso, a massa de 5 milhões
não foi iniciada e não existe medição de throughput válida para ela.

O teste refuta a hipótese de que apenas reduzir a quantidade de chamadas ou
mensagens SQS resolveria o desempenho em 128 MiB com a implementação atual.
Por inspeção do código, a causa provável é o custo do próprio empacotador:
cada registro acrescentado chama `fits`, que serializa novamente todo o bundle
crescente, e `messageSize` recalcula o `bundleId`, ordenando e concatenando os
IDs. Esse trabalho repetido cresce de forma não linear e acontece antes do
primeiro envio, o que é compatível com a fila de saída vazia observada.

Para testar o ganho potencial do SQS de forma justa, o próximo passo é tornar
o cálculo de tamanho incremental e calcular o `bundleId` apenas no fechamento
do bundle. Depois dessa otimização, deve-se repetir primeiro o smoke de 10 mil
e, somente se ele ficar abaixo da margem de 48 segundos, executar os 5 milhões.

Os artefatos brutos desta tentativa estão em
`automacao/resultados/benchmark-p3-memory/bundle-128-20260908T115738Z/`. O
registro nativo `platform.report` confirma `status=timeout`, 60.152 ms
faturados e 76 MB de memória máxima; a fila de saída permaneceu vazia.

## Decisão

- Não promover 128, 256 ou 512 MiB.
- Não declarar 1.024 MiB como configuração validada: ela também não concluiu
  a massa de capacidade.
- Não comparar custo monetário entre candidatos, pois nenhum passou pelo gate
  de estabilidade e as cargas de capacidade foram interrompidas. Um custo
  calculado sobre execuções parciais seria enganoso.
- Corrigir primeiro a duração por chunk, priorizando o paralelismo/overhead de
  publicação e a atomicidade ou idempotência diante de timeout. Depois disso,
  repetir esta mesma matriz e somente então selecionar a memória por
  custo/latência.

## Artefatos e reprodutibilidade

O harness está em
[`automacao/benchmark-p3-memory.sh`](../../automacao/benchmark-p3-memory.sh) e
usa [`automacao/benchmark-aws-5m.sh`](../../automacao/benchmark-aws-5m.sh).
Os dados brutos locais das rodadas estão em:

- `automacao/resultados/benchmark-p3-memory/20260908T105638Z/` — 128, 256 e
  512 MiB;
- `automacao/resultados/benchmark-p3-memory/20260908T113127Z/` — 1.024 MiB.

O Worker também passou a registrar ciclos e pausas de GC, heap e
`GOMAXPROCS` por invocação. O harness coleta duração p95/p99, init, CPU,
memória, GC, erros, throttles e DLQs, e interrompe cedo se detectar a DLQ de
chunks.

Após os ensaios, o fluxo foi encerrado com `terraform destroy`: 107 recursos
foram destruídos, o estado Terraform ficou com zero recursos e não restaram
funções Lambda do ambiente de benchmark. Os logs locais de limpeza são
`.build/p3-destroy-512.log`, `.build/p3-destroy-1024.log` e
`.build/p3-destroy-128-bundle.log`.
