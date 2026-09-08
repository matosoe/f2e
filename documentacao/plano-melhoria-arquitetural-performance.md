# Plano de melhoria arquitetural de performance

## Contexto e baseline

Este plano consolida as oportunidades identificadas durante o benchmark E2E
local com 5 milhões de registros de 100 bytes.

### Ambiente medido

| Recurso | Configuração |
|---|---:|
| CPU | Intel Core i7-13700H |
| Cores físicos | 14 |
| Threads | 20 |
| RAM do host | 32 GB |
| CPUs disponíveis no Docker Desktop | 20 |
| Memória disponível no Docker Desktop | 15,39 GiB |

### Resultado do benchmark

| Indicador | Resultado |
|---|---:|
| Registros | 5.000.000 |
| Tamanho do arquivo | 500.000.000 bytes |
| Chunks | 100/100 concluídos |
| Tempo de processamento | 197,79 segundos |
| Throughput | 25.279 registros/s |
| Mensagens físicas no SQS | 178.539 |
| Registros rejeitados | 0 |
| DLQs | 0 |
| CPU agregada média | equivalente a 0,9 CPU |
| Pico agregado de CPU | inferior a 2 CPUs |
| Pico agregado de memória | aproximadamente 5,3 GiB |

Apesar de o Docker ter acesso a 20 CPUs, somente um container Worker foi
observado durante a execução. A memória disponível não foi o fator limitante.

## P0 — Correções de integridade

### 1. Respeitar o limite agregado do `SendMessageBatch`

Atualmente cada bundle respeita individualmente o limite de 256 KiB, mas até
dez bundles podem ser enviados na mesma chamada. A soma dos bodies e atributos
pode ultrapassar o limite agregado da API, como demonstrado pelo erro
`BatchRequestTooLong` encontrado durante o ensaio.

Ações:

- montar lotes considerando o tamanho agregado dos bodies, atributos e
  overhead da requisição;
- encerrar o lote antes de ultrapassar o limite do SQS;
- manter uma margem explícita para diferenças de serialização;
- adicionar testes com mensagens próximas ao limite individual e agregado.

Critério de aceite: nenhuma chamada `SendMessageBatch` pode exceder o limite da
API, independentemente da combinação entre tamanho e quantidade de bundles.

### 2. Preservar o erro original no remetente concorrente

O erro `BatchRequestTooLong` foi apresentado pelo Worker apenas como
`context canceled`. O cancelamento das demais operações acabou ocultando a
causa raiz.

Ações:

- preservar o primeiro erro real que provocou o cancelamento;
- impedir que erros derivados de cancelamento substituam a causa original;
- incluir nos logs tentativa, quantidade de mensagens e tamanho do lote;
- classificar corretamente erros permanentes e transitórios.

Critério de aceite: o ledger e os logs devem apresentar a falha original da API,
com contexto suficiente para diagnóstico.

### 3. Corrigir a contabilização de mensagens físicas

O contador `messagesPublished` não é incrementado de forma confiável no fluxo
atual.

Ações:

- separar os contadores de registros lidos, eventos lógicos publicados,
  mensagens físicas publicadas e chamadas `SendMessageBatch`;
- incrementar mensagens físicas somente após confirmação da API;
- garantir que retries não dupliquem os totais lógicos;
- reconciliar os contadores por chunk e por job.

Critério de aceite: os totais do ledger devem coincidir com a saída observada e
permanecer corretos após retries parciais.

## P1 — Paralelismo e throughput

### 4. Alinhar o provisionamento local ao Terraform

O benchmark planejou 100 chunks, mas somente um container Worker foi criado. O
Terraform possui configuração de concorrência máxima, porém o provisionamento
direto do LocalStack não aplica explicitamente o mesmo limite.

Ações:

- configurar `MaximumConcurrency` no event-source mapping local;
- expor o valor como parâmetro do ambiente e do benchmark;
- iniciar os testes com 8 e 16 Workers;
- verificar nas métricas que múltiplas instâncias são realmente criadas;
- manter os padrões locais e Terraform documentados e consistentes.

Critério de aceite: com backlog e chunks suficientes, o ambiente deve utilizar
mais de um Worker até o limite configurado.

### 5. Coordenar as duas camadas de concorrência

Existem dois níveis independentes de paralelismo:

1. quantidade de Workers processando chunks;
2. quantidade de chamadas SQS simultâneas dentro de cada Worker.

Ações:

- configurar e medir os dois limites separadamente;
- impedir que `Workers × PublishConcurrency` produza pressão excessiva no SQS;
- iniciar com 8 Workers e concorrência de publicação 4;
- comparar posteriormente 16 × 4 e 8 × 8;
- aplicar backoff quando houver throttling ou aumento de latência.

Critério de aceite: aumentar a concorrência deve produzir ganho mensurável sem
elevar DLQs, falhas ou retries de forma desproporcional.

### 6. Separar tamanho alvo do chunk do limite máximo de registro

O planejamento atual deriva o tamanho nominal do chunk usando
`recordsPerChunk × maxRecordLengthBytes`. Como `maxRecordLengthBytes` é um teto
de segurança, e não o tamanho típico da linha, essa relação pode criar poucos
chunks excessivamente grandes.

Ações:

- introduzir uma configuração explícita `targetChunkBytes`;
- usar `maxRecordLengthBytes` somente para validação e tratamento de fronteiras;
- preservar `maxChunkBytes` como limite absoluto;
- registrar bytes e registros efetivos por chunk;
- avaliar chunks alvo de 1, 5 e 10 MiB.

O benchmark utilizou aproximadamente 5 MiB por chunk, produzindo 100 unidades
de trabalho e progresso linear.

Critério de aceite: arquivos grandes devem gerar paralelismo previsível sem
depender do maior tamanho possível de registro.

## P2 — Contrato e eficiência do SQS

### 7. Formalizar bundles para fluxos de alto volume

O benchmark publicou 5 milhões de eventos lógicos em 178.539 mensagens físicas,
uma média aproximada de 28 eventos por mensagem. No modo individual seriam 5
milhões de mensagens, aumentando armazenamento, custo e overhead operacional.

Ações:

- formalizar e versionar o contrato de bundle;
- documentar a expansão dos eventos pelo consumidor;
- exigir processamento idempotente por `eventId`;
- documentar que o retry de um bundle pode reenviar todos os seus eventos;
- manter o modo individual quando o consumidor não aceitar bundles.

Critério de aceite: consumidores compatíveis devem processar bundles sem perda
ou duplicação lógica, inclusive durante retries.

### 8. Tornar o empacotamento adaptativo

O limite temporário de 24.000 bytes por bundle evitou ultrapassar o limite
agregado de dez mensagens, mas reduz a utilização potencial de cada chamada.

Ações:

- calcular dinamicamente o orçamento restante do lote;
- preencher bundles e chamadas considerando os limites individual e agregado;
- contabilizar atributos e overhead de serialização;
- registrar tamanho médio, p95 e utilização percentual do limite;
- evitar depender permanentemente de um teto fixo conservador.

Critério de aceite: o empacotamento deve maximizar eventos por chamada sem
produzir requisições inválidas.

### 9. Aplicar backpressure

Ações:

- limitar lotes e mensagens em voo por Worker;
- reduzir temporariamente a concorrência após throttling;
- limitar a quantidade de envelopes serializados mantidos em memória;
- interromper rapidamente a produção quando o contexto for cancelado;
- expor métricas de fila interna e tempo de espera por slot.

Critério de aceite: o uso de memória deve permanecer limitado e previsível
mesmo quando o SQS responder lentamente.

## P3 — Observabilidade

### 10. Medir cada estágio do pipeline

Adicionar métricas para:

- planejamento do arquivo;
- espera na fila de chunks;
- leitura do S3 por chunk;
- parsing e serialização;
- empacotamento;
- publicação no SQS;
- atualização do ledger;
- duração p50, p95 e p99 dos chunks.

### 11. Medir saturação e utilização

Adicionar métricas para:

- Workers ativos e máximo configurado;
- chamadas SQS em voo;
- CPU e memória agregadas de todos os containers;
- tamanho médio e p95 dos bundles;
- eventos por mensagem física;
- erros, throttling e retries por API.

### 12. Separar métricas do produtor e do consumidor de teste

Ações:

- validar o término do processamento pelo ledger;
- validar a fila por contagem e amostragem de envelopes;
- medir separadamente o tempo necessário para consumir e excluir as mensagens;
- evitar que a drenagem de milhões de mensagens distorça o throughput do F2E.

## P4 — Estratégia de benchmark

Manter os testes de carga fora da suíte funcional padrão:

| Nível | Volume | Finalidade |
|---|---:|---|
| Smoke | 10 mil registros | Validar rapidamente o harness e o ambiente |
| Capacidade | 500 mil registros | Comparar parâmetros e detectar regressões |
| Completo | 5 milhões de registros | Validar throughput e estabilidade sustentada |

O teste completo deve ser manual ou agendado, nunca executado implicitamente
por `go test ./...`.

### Matriz inicial

Alterar um fator por vez em relação ao baseline:

| Fator | Valores sugeridos |
|---|---|
| Tamanho alvo do chunk | 1, 5 e 10 MiB |
| Concorrência de Workers | 4, 8 e 16 |
| Concorrência de publicação | 1, 4 e 8 |
| Tamanho máximo do bundle | 16, 24 e 32 KiB |

Cada configuração relevante deve ser executada ao menos três vezes. A mediana
deve ser usada para comparação, acompanhada da dispersão e dos percentis.

### Critérios mínimos

- 5.000.000 de registros lidos e publicados;
- todos os chunks concluídos;
- nenhuma rejeição inesperada;
- nenhuma mensagem em DLQ;
- nenhuma requisição acima dos limites do SQS;
- contadores do ledger reconciliados;
- ganho reproduzível em pelo menos três execuções;
- relatório contendo parâmetros, versões e configuração da máquina.

## Ordem recomendada de execução

1. Corrigir o limite agregado do SQS e a propagação de erros.
2. Corrigir os contadores do ledger.
3. Alinhar a concorrência do LocalStack ao Terraform.
4. Introduzir `targetChunkBytes` e validar a distribuição dos chunks.
5. Reexecutar smoke e capacidade com 8 Workers × 4 publicações.
6. Implementar empacotamento adaptativo e backpressure.
7. Executar a matriz controlada de parâmetros.
8. Repetir o benchmark completo três vezes.
9. Validar a configuração vencedora em uma conta AWS real.

## Limitações da medição local

O LocalStack é um simulador single-node e não representa integralmente a
capacidade, latência, escalabilidade ou limites operacionais da AWS. Os
resultados locais são adequados para detectar regressões e comparar mudanças
relativas.

A memória do Docker não precisa ser aumentada neste momento: o pico medido foi
aproximadamente 5,3 GiB dos 15,39 GiB disponíveis. A prioridade deve ser corrigir
o paralelismo efetivo e reduzir o overhead de publicação. A configuração final
de produção deve ser homologada em uma conta AWS real.
