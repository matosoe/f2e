# Benchmark AWS — matriz de memória

Esta é a validação na AWS da matriz local de memória × CPU. Para cada perfil,
o harness executa primeiro um smoke de 10.000 linhas e só inicia a carga de
5.000.000 se as invariantes do ledger e das DLQs forem aprovadas.

```bash
./automacao/benchmark-aws-memory-matrix.sh
```

A matriz padrão é 128, 256, 512 e 1.024 MiB. Diferentemente do Docker local,
a CPU não é limitada artificialmente: cada execução recebe a CPU proporcional
à memória provisionada pela AWS Lambda. Isso faz desta a medição adequada para
decidir o tamanho de produção.

O perfil mantém a massa local de linhas de 100 bytes. Como a aplicação exige
chunks de ao menos 5 MiB e o Organizer redistribui o arquivo em partes iguais,
o alvo AWS é 6.250.000 bytes: 80 chunks exatos de 62.500 linhas na massa de 5
milhões. Isso preserva a fronteira das linhas. O benchmark usa até 10
Workers, publicação interna com concorrência 8, saída em bundles de até 250
KiB e sem limite de envelopes por mensagem. O timeout do Worker é 600 s e o
timeout de visibilidade do SQS é 3.600 s; o limite global por perfil é seis
horas. O timeout individual da Lambda não pode ultrapassar 900 s, uma
restrição da AWS.
Cada Worker usa `provided.al2023` e a
arquitetura configurada no `aws.local.tfvars`.

Os artefatos são gravados em
`automacao/resultados/benchmark-aws-memory-matrix/<run-id>/`. Cada perfil tem
os relatórios completos do harness AWS, inclusive telemetria CloudWatch de
CPU, memória, GC, duração, erros, throttles e DLQs; `summary.json` consolida
as etapas disponíveis.

O ambiente `development` é criado de forma descartável e destruído ao fim,
inclusive em falha. O harness subjacente também confirma a conta definida em
`aws.local.tfvars` antes de provisionar recursos.

## Resultado — execução `20260909T123015Z`

O `summary.json` desta execução contém resultados completos para 128, 256,
512 e 1.024 MiB. A unidade abaixo é **registro lógico**
(linha de entrada), e não mensagem SQS: cada mensagem de saída agrupou cerca
de 302 registros no teste de 5 milhões.

| Memória Lambda | Smoke 10k (fim a fim) | Capacidade média estimada por Worker | Projeção ideal para 5 milhões (10 Workers) | Capacidade média por Worker — 5 milhões | Resultado real — 5 milhões |
| --- | ---: | ---: | ---: | ---: | --- |
| **128 MiB** | 16,38 s | **~1.707 registros/s** | ~292,9 s | **~3.152 registros/s** | **Concluído em 255,21 s**; 19.592 registros/s no fluxo; 1 throttling |
| **256 MiB** | 16,87 s | **~3.590 registros/s** | ~139,3 s | **~6.283 registros/s** | **Concluído em 149,07 s**; 33.541 registros/s no fluxo |
| **512 MiB** | 8,59 s | **~7.225 registros/s** | ~69,2 s | **~12.782 registros/s** | **Concluído em 96,04 s**; 52.062 registros/s no fluxo |
| **1.024 MiB** | 8,51 s | **~13.133 registros/s** | ~38,1 s | **~24.517 registros/s** | **Concluído em 66,76 s**; 74.901 registros/s no fluxo |

A capacidade estimada por Worker é `10.000 / duração média da invocação
Worker no smoke`. A capacidade medida para 5 milhões é `62.500 / duração
média da invocação Worker nessa carga`. A projeção é um limite idealizado,
calculado com dez Workers ativos continuamente; ela não inclui o tempo de
orquestração, escalonamento e coordenação das filas. Por isso o resultado real
é a referência para decisão.

As quatro cargas completas terminaram sem erros, rejeições ou mensagens em DLQ.
O perfil de 128 MiB registrou um throttling isolado. Entre 256 e 512 MiB, a
capacidade do Worker cresceu 103% e o throughput do fluxo, 55%.

Os dados de origem estão em
`automacao/resultados/benchmark-aws-memory-matrix/20260909T123015Z/summary.json`.

## Personalização

```bash
BENCHMARK_AWS_MEMORY_MATRIX='256 512' \
BENCHMARK_AWS_PUBLISH_CONCURRENCY=4 \
BENCHMARK_AWS_RECORDS_PER_CHUNK=50000 \
./automacao/benchmark-aws-memory-matrix.sh
```

Se o DNS local direcionar o hostname regional do SSM para um endpoint privado
inacessível, use o endpoint dual-stack público da região:

```bash
BENCHMARK_AWS_SSM_ENDPOINT_URL='https://ssm.us-east-1.api.aws' \
./automacao/benchmark-aws-memory-matrix.sh
```

Para uma execução exploratória menor, ajuste explicitamente
`BENCHMARK_AWS_FULL_RECORDS`; esse resultado não deve substituir a decisão da
matriz de 5 milhões de linhas.
