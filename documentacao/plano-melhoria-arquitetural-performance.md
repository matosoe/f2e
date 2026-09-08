# Plano de melhoria arquitetural de performance

## Contexto e baseline

Este plano consolida as oportunidades identificadas nos benchmarks E2E local
e AWS com 5 milhões de registros de 100 bytes. O ensaio AWS exercitou o pior
caso de envelope: uma linha, um envelope e uma mensagem física no SQS.
Os dados e artefatos da execução estão descritos em
[`benchmark-aws-5m.md`](benchmark-aws-5m.md).

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

### Baseline AWS — envelope individual

| Indicador | Resultado |
|---|---:|
| Runtime/arquitetura | `provided.al2023` / `x86_64` |
| Memória configurada | 1.024 MB |
| Timeout medido | 60 segundos |
| Concorrência máxima observada | 10 Workers |
| Chunks | 100/100 concluídos |
| Tempo ponta a ponta | 732,238 segundos |
| Throughput | 6.828 mensagens/s |
| Mensagens físicas no SQS | 5.000.000 |
| CPU média/p95/pico do processo | 10,82% / 11,86% / 12,34% |
| Memória média/p95/pico | 44,73 / 46 / 46 MB |
| Duração média/máxima do Worker | 55,146 / 58,874 segundos |
| Erros, throttles e DLQs | 0 |

O baseline AWS comprovou a cardinalidade de uma mensagem por linha e que o
poller alcançou dez execuções concorrentes. Também mostrou duas restrições: a
conclusão concorrente dos chunks disputa o item agregado do job no DynamoDB, e
chunks de 50 mil registros ficam muito próximos do timeout de 60 segundos.
O timeout deve permanecer em **60 segundos**; o plano deve obter margem por
redução/controle do trabalho por invocação e por otimização, não reduzindo nem
ampliando esse valor como substituto para a correção.

## P0 — Correções de integridade

### 1. Respeitar o limite agregado do `SendMessageBatch`

O limite configurado de quantidade de envelopes não pode se sobrepor ao limite
de bytes. Uma mensagem física que agrupe muitos envelopes deve parar em
**250 KiB**, mesmo que ainda não tenha atingido `maxEnvelopesPerMessage`. Além
disso, até dez mensagens físicas podem participar de um `SendMessageBatch`; a
soma dos bodies, atributos e overhead estimado das entradas deve usar um
orçamento conservador de **240 KiB** para lotes com múltiplas mensagens. Isso
evita o `BatchRequestTooLong` encontrado durante o ensaio.

Ações:

- ao montar um bundle, serializar/incluir envelopes somente enquanto o body e
  seus atributos permanecerem em até 250 KiB;
- se um único envelope ultrapassar 250 KiB, rejeitá-lo com erro permanente e
  contexto no ledger; não truncar nem dividir silenciosamente o envelope;
- ao montar o `SendMessageBatch`, adicionar no máximo dez mensagens e somente
  enquanto a soma permanecer em até 240 KiB;
- se a próxima mensagem ultrapassar 240 KiB agregados, enviar imediatamente o
  lote atual com menos de dez entradas e iniciar outro;
- se uma única mensagem tiver mais de 240 KiB e no máximo 250 KiB, enviá-la
  sozinha; ela é válida como mensagem individual, mas não participa de lote
  com múltiplas entradas;
- calcular tamanhos sobre os bytes UTF-8 efetivamente serializados, incluindo
  nomes, tipos e valores dos atributos e margem para overhead;
- adicionar testes nos limites 240/250 KiB, com atributos, caracteres
  multibyte, dez mensagens pequenas e uma mensagem grande isolada.

Critério de aceite: nenhuma mensagem física excede 250 KiB; nenhum lote com
múltiplas entradas excede 240 KiB ou dez mensagens; e mensagens válidas entre
240 e 250 KiB são enviadas sozinhas sem perda ou truncamento.

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

Antes do benchmark AWS, o contador `messagesPublished` não era incrementado de
forma confiável no fluxo.

Ações:

- separar os contadores de registros lidos, eventos lógicos publicados,
  mensagens físicas publicadas e chamadas `SendMessageBatch`;
- incrementar mensagens físicas somente após confirmação da API;
- garantir que retries não dupliquem os totais lógicos;
- reconciliar os contadores por chunk e por job.

Critério de aceite: os totais do ledger devem coincidir com a saída observada e
permanecer corretos após retries parciais.

Estado: implementado no caminho `single` e `bundle` e validado no benchmark
AWS com 5 milhões de registros. Manter testes de regressão para retries e
falhas parciais.

### 3.1. Eliminar conflitos na conclusão concorrente do ledger

Com dez Workers, múltiplas transações tentaram atualizar simultaneamente o
mesmo item `JOB`. O primeiro ensaio terminou parcialmente por
`TransactionConflict`; reutilizar o mesmo token após uma transação
explicitamente cancelada também produziu `IdempotentParameterMismatch`.

Ações:

- manter retry com backoff somente para cancelamento por
  `TransactionConflict`;
- usar token novo no retry externo de uma transação comprovadamente cancelada;
- preservar o token nos retries internos de transporte, cujo resultado pode
  ser desconhecido;
- medir conflitos e quantidade de tentativas por conclusão;
- avaliar contadores distribuídos por chunk ou reconciliação assíncrona se o
  item `JOB` voltar a ser um hot key em concorrências maiores.

Estado: retry e escopo do token implementados; o benchmark final concluiu
100/100 chunks com dez Workers.

Critério de aceite: nenhuma perda de progresso ou dupla contabilização sob
concorrência, inclusive com falhas de transporte e conflitos induzidos.

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

### 4.1. Remover consumidores concorrentes da fila compartilhada

A topologia AWS atual possui o Worker legado e três Workers por prefixo
consumindo a mesma fila de chunks, sem roteamento exclusivo. O benchmark
precisou desabilitar os mappings não alvo para impedir que outro Worker
capturasse chunks de `example-text`.

Ações:

- preferir uma fila de chunks por Worker/prefixo; ou substituir os múltiplos
  consumidores por um único roteador com destino explícito;
- não aplicar filtros independentes sobre a mesma fila SQS sem comprovar a
  semântica de remoção das mensagens não correspondentes;
- tornar a associação prefixo → fila → Worker explícita no Terraform;
- testar simultaneamente dois prefixos e provar que não há roubo ou descarte
  cruzado de mensagens.

Critério de aceite: cada chunk deve possuir exatamente um consumidor elegível,
sem precisar desativar mappings para obter isolamento.

### 4.2. Validar cotas e concorrência antes da carga

A conta medida tinha cota regional de concorrência igual a 10 e exigia manter
as dez unidades no pool não reservado. Reservar dez unidades diretamente para
o Worker falhou; o benchmark usou ausência de reserva e
`ScalingConfig.MaximumConcurrency=10` no poller.

Ações:

- consultar Service Quotas e concorrência não reservada antes do deploy;
- distinguir capacidade reservada de limite máximo do event-source mapping;
- falhar cedo quando a configuração solicitada não couber na conta;
- registrar no relatório a cota, a reserva efetiva, o teto do mapping e o pico
  observado de `ConcurrentExecutions`.

Critério de aceite: o ambiente deve aplicar o teto solicitado sem erro de cota
e o relatório deve provar a concorrência efetivamente atingida.

### 5. Coordenar as duas camadas de concorrência

Existem dois níveis independentes de paralelismo:

1. quantidade de Workers processando chunks;
2. quantidade de chamadas SQS simultâneas dentro de cada Worker.

Ações:

- configurar e medir os dois limites separadamente;
- impedir que `Workers × PublishConcurrency` produza pressão excessiva no SQS;
- preservar o limite/backpressure já existente no `concurrentSender`;
- comparar, na AWS, 10 Workers com concorrência interna 1, 4, 8 e 10;
- manter retry seletivo somente das entradas que o `SendMessageBatch` marcou
  como falhas;
- aplicar backoff quando houver throttling ou aumento de latência.

Critério de aceite: aumentar a concorrência deve produzir ganho mensurável sem
elevar DLQs, falhas ou retries de forma desproporcional.

O baseline AWS usou concorrência interna 1 e atingiu 6.828 mensagens/s. Como a
carga é predominantemente I/O, goroutines limitadas podem sobrepor a espera de
rede sem exigir uma thread por chamada. `errgroup.SetLimit` é uma alternativa
de implementação, mas não é requisito: o sender atual com semáforo já oferece
limite de concorrência, cancelamento e backpressure equivalentes.

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
- limitar o tamanho alvo entre **5 MB e 100 MB**;
- fazer o Organizer buscar aproximadamente **100 chunks** por arquivo,
  respeitando os limites mínimo e máximo.

Para arquivos com pelo menos 5 MB, calcular a quantidade de chunks mais
próxima de 100 que satisfaça simultaneamente:

```text
ceil(fileSize / 100 MB) <= chunkCount <= floor(fileSize / 5 MB)
```

Depois, dividir o arquivo em intervalos nominais equilibrados por byte. Assim,
um arquivo de 500 MB gera 100 chunks próximos de 5 MB; um arquivo pequeno pode
gerar menos de 100; e um arquivo muito grande pode gerar mais de 100 para nunca
ultrapassar 100 MB. Arquivos menores que 5 MB formam um único chunk. O último
chunk não deve ficar artificialmente pequeno: quando possível, o restante deve
ser redistribuído entre os chunks anteriores.

O benchmark AWS utilizou aproximadamente 5 MB por chunk, produzindo 100
unidades de trabalho e progresso linear. Uma tentativa anterior com 5.000
chunks de 1.000 linhas excedeu 60 segundos no Organizer durante as leituras de
fronteira no S3. Já os chunks de 50 mil registros chegaram a 58,874 segundos
no Worker, margem insuficiente para o timeout fixo de 60 segundos.

Ações adicionais:

- definir uma margem operacional para o timeout, por exemplo duração p99 menor
  ou igual a 80% de 60 segundos;
- retirar do Organizer as leituras de descoberta de fronteira;
- não usar aumento de timeout para ocultar planejamento ou chunks grandes.

Critério de aceite: arquivos grandes devem gerar paralelismo previsível sem
depender do maior tamanho possível de registro, e nenhuma invocação deve se
aproximar do timeout de 60 segundos além da margem definida.

### 6.1. Transferir a descoberta de fronteiras para os Workers

Para texto delimitado por linha, o Organizer deve planejar apenas com os
metadados imutáveis do objeto (`size`, `versionId` e ETag). Ele cria intervalos
nominais contíguos, sem acessar o conteúdo para procurar quebras. Cada Worker
descobre localmente as fronteiras reais e publica somente os registros que
pertencem ao seu intervalo nominal.

Regra de propriedade: um registro pertence ao único chunk cujo intervalo
nominal contém o offset do primeiro byte desse registro. Pode haver sobreposição
de bytes na leitura física, mas nunca no processamento ou na publicação lógica.

Algoritmo de leitura para cada fronteira do chunk:

1. começar lendo **3 × `maxRecordLengthBytes`** antes do início nominal e
   depois do fim nominal, limitado ao início/fim do arquivo;
2. se não encontrar a quebra necessária, expandir mais **3 ×**;
3. se ainda não encontrar, fazer uma última expansão de mais **3 ×**;
4. o limite total pesquisado é, portanto, **9 × `maxRecordLengthBytes`** em
   cada lado;
5. o início do arquivo e o EOF são fronteiras válidas; um arquivo sem quebra
   final não deve falhar somente por terminar no meio da última janela;
6. validar as duas fronteiras antes de publicar qualquer mensagem do chunk;
7. publicar apenas registros cujo offset inicial esteja no intervalo nominal
   `[ownedStartByte, ownedEndByte]`.

Se uma quebra não for encontrada após as três janelas, o Worker deve falhar o
chunk e registrar no ledger um erro de contrato de dados contendo pelo menos:
`jobId`, `chunkId`, lado da fronteira, offset nominal,
`maxRecordLengthBytes`, bytes pesquisados e a informação de que os registros
são significativamente maiores que o limite informado. O erro não deve ser
tratado como transitório nem provocar publicação parcial do chunk.

O fator 9 × oferece tolerância quando o usuário subestima moderadamente o
tamanho máximo, mas muda a semântica prática da configuração: o valor informado
passa a ser uma estimativa com tolerância, enquanto o limite efetivamente
aceito pela busca é 9 ×. Essa tolerância e seu custo de leitura devem aparecer
nas métricas e na documentação do contrato.

A primeira implementação deve atender texto/JSON Lines delimitado por LF ou
CRLF. Multiline e arrays JSON precisam de estratégias específicas de fronteira
e não devem reutilizar automaticamente a regra de quebra de linha.

Critério de aceite: o Organizer não realiza `GetObject` para planejar texto
delimitado; e testes de propriedade comprovam que todos os registros são
publicados exatamente por um chunk para diferentes tamanhos de arquivo, chunk,
linha, LF/CRLF e ausência de quebra final. Arquivos com fronteira além de 9 ×
devem falhar no ledger antes de qualquer publicação daquele chunk.

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

O empacotamento deve respeitar simultaneamente quantidade e bytes. Quando
`maxEnvelopesPerMessage` estiver preenchido com valor positivo, ele é um teto
explícito do cliente: `1`, por exemplo, produz exatamente um envelope por
mensagem física. Quando o campo estiver vazio ou for `0`, não existe teto por
quantidade e o Worker deve preencher cada envelope até o máximo permitido por
bytes. Em ambos os casos, os tetos de segurança continuam sendo 250 KiB por
mensagem física, 240 KiB para um lote com múltiplas mensagens e dez entradas
por batch.

Ações:

- calcular dinamicamente o orçamento restante do bundle e do lote;
- tratar `maxEnvelopesPerMessage > 0` como limite configurado, sem tentar
  preenchê-lo além do teto de bytes;
- com `maxEnvelopesPerMessage` ausente ou `0`, continuar adicionando envelopes
  enquanto couberem no orçamento de bytes;
- fechar o bundle antes do envelope que faria a mensagem ultrapassar 250 KiB;
- fechar o batch antes da mensagem que faria o agregado ultrapassar 240 KiB,
  mesmo que o batch tenha menos de dez entradas;
- enviar isoladamente mensagens válidas na faixa acima de 240 até 250 KiB;
- contabilizar bytes UTF-8, atributos e overhead de serialização;
- registrar tamanho médio, p95 e utilização percentual do limite;
- registrar mensagens por lote, batches abaixo de dez entradas e mensagens
  grandes enviadas sozinhas.

Critério de aceite: o empacotamento deve respeitar a quantidade explicitamente
configurada pelo cliente; sem essa configuração, deve maximizar eventos por
mensagem até o limite de bytes, sem produzir requisições inválidas.

### 9. Aplicar backpressure

Ações:

- limitar lotes e mensagens em voo por Worker;
- reduzir temporariamente a concorrência após throttling;
- limitar a quantidade de envelopes serializados mantidos em memória;
- interromper rapidamente a produção quando o contexto for cancelado;
- expor métricas de fila interna e tempo de espera por slot.

Critério de aceite: o uso de memória deve permanecer limitado e previsível
mesmo quando o SQS responder lentamente.

## P3 — Otimização da Lambda Go

### 10. Fazer right-sizing de memória com medição

O Worker Go consumiu somente 46 MB no pico com 1.024 MB configurados. O
binário nativo, os buffers de streaming e o pool de conexões tornam 128 MB uma
hipótese testável, mas o corte de memória também reduz CPU e pode aumentar a
duração.

| Memória candidata | Hipótese a validar |
|---|---|
| 512 MB | Deve manter ampla folga; provável sobreprovisionamento |
| **256 MB** | Candidato inicial recomendado para custo/latência |
| **128 MB** | Viável para teste em Go; exige validar duração, GC e OOM |

Ações:

- executar AWS Lambda Power Tuning ou matriz equivalente em 128, 256, 512 e
  1.024 MB;
- manter 10 Workers, mesma massa, mesmos chunks e uma mensagem por linha;
- coletar duração, custo estimado, CPU, memória, init, GC, erros e throttles;
- executar cada ponto ao menos três vezes e comparar mediana e p95/p99;
- só promover 128 ou 256 MB se houver margem de memória e de timeout.

Critério de aceite: escolher a menor memória que preserve estabilidade e tenha
melhor resultado de custo/latência, mantendo p99 abaixo da margem operacional
do timeout de 60 segundos.

### 11. Enxugar o binário e comparar `arm64`

O runtime correto já está em uso: `provided.al2023`. O build atual usa
`GOARCH=amd64`, `-trimpath` e remove apenas o build ID. Avaliar um build de
produção como:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -tags lambda.norpc \
  -ldflags='-s -w -buildid=' -o bootstrap ./cmd/worker
```

Ações:

- parametrizar arquitetura no build e no Terraform para que binário e Lambda
  nunca divirjam;
- comparar `amd64` e `arm64` com a mesma memória e carga;
- medir tamanho do ZIP, `Init Duration`, duração quente e custo;
- validar todos os binários Lambda, não somente o Worker;
- manter `CGO_ENABLED=0` para artefato estático quando nenhuma dependência
  exigir CGO.

Não definir 30–60 ms como promessa: o baseline deve primeiro registrar a
distribuição real de cold starts. Não há SnapStart para este runtime; e
provisioned concurrency não se justifica sem evidência de que cold start viola
um SLO.

Critério de aceite: o artefato otimizado deve reduzir tamanho/init sem regressão
funcional, e `arm64` só deve virar padrão se melhorar o resultado medido.

### 12. Validar `GOMAXPROCS` e controlar o GC

O módulo usa Go 1.26, cujo comportamento deve ser verificado no cgroup da
Lambda em vez de presumido. Em memórias abaixo da faixa correspondente a uma
vCPU, comparar o ajuste automático com `GOMAXPROCS=1`; fixá-lo sem benchmark
pode prejudicar operações que se beneficiem de paralelismo de CPU.

Para reduzir risco de OOM durante o right-sizing, configurar `GOMEMLIMIT` por
perfil, próximo de 75–80% da memória da função. Exemplo para 256 MB:

```ini
GOMAXPROCS=1
GOMEMLIMIT=200MiB
```

`GOMEMLIMIT` é um soft limit do runtime, não um limite rígido nem garantia
contra OOM. Para 128 MB, deve ser usado um valor proporcional menor, reservando
memória para stacks, binário, buffers, TLS e componentes fora do heap Go.

Ações:

- registrar `runtime.GOMAXPROCS(0)`, limite de memória e métricas de GC uma vez
  por cold start;
- comparar `GOMAXPROCS` automático e 1 em 128/256/512 MB;
- definir `GOMEMLIMIT` específico para cada tamanho, não um valor global;
- acompanhar ciclos/pausas de GC, heap máximo, duração e memória RSS;
- preservar timeout em 60 segundos em toda a matriz.

Critério de aceite: ajustes de runtime devem reduzir custo ou duração sem elevar
p95/p99, OOMs ou throttling de GC.

### 13. Reduzir alocações somente após profiling

Avaliar `sync.Pool` para buffers de serialização e estruturas reutilizáveis do
caminho quente, inclusive lotes de `SendMessageBatchRequestEntry`. A
otimização deve vir depois de perfis de alocação, pois pools podem reter buffers
grandes, aumentar memória residente e introduzir erros de ownership quando um
slice é reciclado antes do término do envio assíncrono.

Ações:

- gerar perfil de alocação/heap em massa representativa;
- identificar os maiores allocators por bytes e objetos;
- reduzir cópias e pré-alocar slices antes de introduzir pools;
- limitar a capacidade máxima dos buffers devolvidos ao pool;
- adicionar teste de corrida e de integridade com publicação concorrente.

Critério de aceite: redução mensurável de bytes/op e ciclos de GC sem aumentar
RSS, complexidade de forma injustificada ou risco de corrupção.

### 14. Dimensionar o transporte HTTP para a concorrência

Keep-alive já é fornecido pelo transporte do AWS SDK for Go v2. Os clientes S3,
SQS, DynamoDB e SSM também já são criados uma vez no `init()` e reutilizados
durante a vida do ambiente Lambda; essas propriedades devem ser preservadas.

Ações:

- configurar e testar `MaxIdleConnsPerHost` pelo menos igual à concorrência
  interna de publicação;
- dimensionar `MaxIdleConns`, `IdleConnTimeout` e timeouts de conexão/TLS sem
  desabilitar keep-alive;
- medir reutilização de conexões, latência por chamada e handshakes;
- confirmar que o ganho de goroutines não é serializado pelo pool HTTP;
- manter uma única chamada a `config.LoadDefaultConfig` por cold start.

Critério de aceite: aumentar `PublishConcurrency` deve aumentar chamadas em voo
e throughput sem crescimento desproporcional de conexões ou latência.

### Configuração alvo da próxima rodada

Esta é uma hipótese inicial para a matriz, não uma configuração de produção já
aprovada:

- memória: **256 MB**, mantendo 128 MB como experimento subsequente;
- arquitetura: testar **`arm64`** contra o baseline `x86_64`;
- runtime: **`provided.al2023`**;
- timeout: **60 segundos**, sem redução para 45 segundos;
- `GOMAXPROCS=1` e `GOMEMLIMIT=200MiB` como variantes controladas, comparadas
  com o ajuste automático;
- `MaximumConcurrency=10` no mapping e reserva somente se a cota da conta
  permitir;
- concorrência interna de publicação inicialmente 1 e depois 4/8/10;
- sem SnapStart; sem provisioned concurrency enquanto cold start não violar um
  SLO.

## P4 — Observabilidade

### 15. Medir cada estágio do pipeline

Adicionar métricas para:

- planejamento do arquivo;
- espera na fila de chunks;
- leitura do S3 por chunk;
- parsing e serialização;
- empacotamento;
- publicação no SQS;
- atualização do ledger;
- duração p50, p95 e p99 dos chunks.

### 16. Medir saturação e utilização

Adicionar métricas para:

- Workers ativos e máximo configurado;
- chamadas SQS em voo;
- CPU e memória agregadas de todos os containers;
- tamanho médio e p95 dos bundles;
- eventos por mensagem física;
- erros, throttling e retries por API.

Já existe telemetria de CPU do processo via `getrusage` por invocação e memória
via `platform.report`. Preservar ambas e adicionar `Init Duration`, duração
faturada, arquitetura, memória configurada, `GOMAXPROCS`, `GOMEMLIMIT` e
métricas de GC ao relatório consolidado. A porcentagem de CPU atual representa
tempo de processo dividido pelo tempo de parede; não deve ser apresentada como
percentual nativo da capacidade de CPU provisionada pela Lambda.

Manter também `logging_config` explícito nos Workers por prefixo, apontando para
um log group criado e autorizado pelo Terraform. Sem esse vínculo, logs podem
ir para o grupo padrão ou falhar por permissão, inviabilizando a coleta de CPU
e memória usada pelo benchmark.

### 17. Separar métricas do produtor e do consumidor de teste

Ações:

- validar o término do processamento pelo ledger;
- validar a fila por contagem e amostragem de envelopes;
- medir separadamente o tempo necessário para consumir e excluir as mensagens;
- evitar que a drenagem de milhões de mensagens distorça o throughput do F2E.

## P5 — Estratégia de benchmark

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
| Memória do Worker | 128, 256, 512 e 1.024 MB |
| Arquitetura | `amd64` e `arm64` |
| Tamanho alvo do chunk | Automático para ~100 chunks, limitado a 5–100 MB |
| Concorrência de Workers | 4, 8, 10 e 16 |
| Concorrência de publicação | 1, 4, 8 e 10 |
| Limites de mensagem/lote | 250/240 KiB fixos; variar quantidade de envelopes |
| `GOMAXPROCS` | automático e 1 |
| `GOMEMLIMIT` | sem override e 75–80% por perfil de memória |

O timeout permanece fixo em **60 segundos** e não é fator da matriz. Para
comparações de memória, arquitetura ou runtime, usar chunks cuja duração p99
tenha margem suficiente para esse timeout.

Cada configuração relevante deve ser executada ao menos três vezes. A mediana
deve ser usada para comparação, acompanhada da dispersão e dos percentis.

### Critérios mínimos

- 5.000.000 de registros lidos e publicados;
- todos os chunks concluídos;
- nenhuma rejeição inesperada;
- nenhuma mensagem em DLQ;
- nenhuma requisição acima dos limites do SQS;
- contadores do ledger reconciliados;
- concorrência máxima efetivamente observada e registrada;
- p99 do Worker dentro da margem definida para o timeout de 60 segundos;
- memória sem OOM e com folga para heap e componentes fora do heap;
- ganho reproduzível em pelo menos três execuções;
- relatório contendo parâmetros, versões, arquitetura, configuração da Lambda,
  `GOMAXPROCS`, `GOMEMLIMIT` e configuração da máquina.

## Ordem recomendada de execução

1. Concluir os testes de regressão do contador físico e do retry transacional
   do ledger já implementados.
2. Corrigir o limite agregado do SQS e a propagação de erros.
3. Introduzir chunks automáticos de 5–100 MB, buscando aproximadamente 100 por
   arquivo, e transferir a descoberta incremental de fronteiras para o Worker.
4. Alinhar a concorrência do LocalStack ao Terraform.
5. Aplicar os flags de build enxuto e parametrizar `amd64`/`arm64`.
6. Executar Power Tuning em 128, 256, 512 e 1.024 MB, começando por 256 MB.
7. Comparar `GOMAXPROCS` automático/1 e definir `GOMEMLIMIT` por memória.
8. Dimensionar o pool HTTP e comparar 10 Workers com concorrência interna 1,
   4, 8 e 10.
9. Implementar empacotamento adaptativo; otimizar alocações apenas após
   profiling.
10. Executar a matriz controlada, repetindo cada configuração ao menos três
    vezes.
11. Repetir o benchmark completo na AWS e promover somente a configuração que
    cumprir integridade, margem de timeout e melhor custo/latência.

## Limitações da medição local

O LocalStack é um simulador single-node e não representa integralmente a
capacidade, latência, escalabilidade ou limites operacionais da AWS. Os
resultados locais são adequados para detectar regressões e comparar mudanças
relativas.

A memória do Docker não precisa ser aumentada neste momento: o pico medido foi
aproximadamente 5,3 GiB dos 15,39 GiB disponíveis. A prioridade deve ser corrigir
o paralelismo efetivo e reduzir o overhead de publicação.

O benchmark AWS é representativo dos serviços gerenciados, mas a execução
válida foi um único ponto por configuração. Os valores de 128/256 MB,
`arm64`, `GOMAXPROCS=1`, `GOMEMLIMIT` e maior concorrência interna são
hipóteses do plano, não resultados comprovados. A configuração final de
produção deve ser escolhida após repetições, análise de dispersão e custo.
