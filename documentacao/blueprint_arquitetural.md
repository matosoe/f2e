# Blueprint Arquitetural — F2E

## 1. Objetivo

Este documento consolida o blueprint do F2E (File-to-Events): uma interface
que transforma arquivos versionados no Amazon S3 em eventos de registros no
Amazon SQS. O foco são responsabilidades, garantias, recuperação, capacidade
e limites para operadores e consumidores.

Os critérios normativos de elegibilidade, escopo e limites estão em
[Requisitos e restrições](requisitos_e_restricoes.md). Este blueprint detalha
as decisões e os trade-offs da implementação atual.

---

## 2. Visão arquitetural

O F2E fica entre uma origem baseada em arquivos e consumidores orientados a
eventos. Ele admite uma identidade de arquivo — uma versão imutável quando
disponível, ou `ETag` condicional —, divide o trabalho em chunks, extrai
registros e os publica em uma fila de saída compartilhada ou dedicada ao
prefixo. A implantação possui uma fila de chunks e um Worker compartilhados.

~~~text
Arquivo versionado no S3
          |
          v
SQS file-intake
          |
          v
Lambda Organizer
          |
          v
SQS chunk-jobs (compartilhada)
          |
          v
Lambda Worker (compartilhada)
          |
          v
SQS output-events (padrão) ou fila dedicada de saída
          |
          v
Consumidores downstream
~~~

O DynamoDB mantém o ledger de admissão, plano, chunks e estados terminais. O
Worker entrega eventos técnicos de conclusão a partir da outbox durável.

~~~text
DynamoDB job-ledger -> Worker -> SQS completion-events
~~~

---

## 3. Limites de responsabilidade

O F2E é responsável por converter uma identidade de arquivo fixada na admissão
em eventos rastreáveis. Ele não executa regra de negócio downstream nem garante que cada
efeito de negócio seja aplicado uma única vez.

O escopo é inbound, File-to-Event. Event-to-File é deliberadamente externo ao
building block, e SNS não é um destino implementado por esta versão.

~~~text
ORIGEM                         F2E                              DESTINO
------                         ---                              -------
Produz arquivo          ->     Admite e planeja            ->   Consome evento
Define conteúdo         ->     Lê e interpreta registros   ->   Aplica negócio
Envia ao S3             ->     Publica Envelope v1         ->   Persiste estado
                           Registra execução e conclusão       Deduplica efeitos
~~~

### Responsabilidades do F2E

- Receber notificações S3 ou requisições explícitas na fila de intake.
- Selecionar a configuração de bucket e prefixo e registrar seu snapshot.
- Operar sobre a versão específica do objeto S3.
- Planejar, agendar e processar unidades de trabalho.
- Interpretar arquivos text, json em array e multi-line.
- Publicar Envelope v1 por registro, ou bundles quando configurado.
- Registrar estados, contadores, falhas e conclusão do job.
- Manter as DLQs provisionadas, logs, métricas e alarmes de infraestrutura.

### Responsabilidades fora do F2E

- Produzir o arquivo e validar sua semântica de negócio.
- Persistir efeitos de negócio e gerar novos eventos de domínio.
- Deduplicar efeitos nos consumidores.
- Decidir correção e reprocessamento de mensagens em DLQ.
- Governar retenção, permissões e capacidade dos consumidores.
- Impor ordenação de negócio entre registros.

---

## 4. Fluxo completo e posição do F2E

| Etapa | Responsabilidade | F2E |
|---|---|---|
| Geração | Sistema de origem cria o arquivo | Não |
| Armazenamento | S3 guarda uma versão do objeto | Infraestrutura AWS |
| Entrada | Notificação S3 ou requisição chega à fila | Sim |
| Configuração | Prefixo seleciona contrato e limites no SSM | Sim |
| Admissão | Arquivo é deduplicado e registrado | Sim |
| Planejamento | Arquivo se torna manifesto de chunks | Sim |
| Processamento | Worker lê intervalos e extrai registros | Sim |
| Publicação | Envelopes seguem para fila de saída | Sim |
| Consumo | Regra de negócio e persistência | Não |
| Conclusão | Resultado terminal segue para fila própria | Sim |
| Replay | Operador inicia a partir do ledger | Parcial |

---

## 5. Unidades de trabalho e paralelismo

O arquivo é a unidade de admissão e conclusão. O chunk é a unidade de retry,
agendamento e paralelismo. O registro é a unidade lógica entregue ao destino.

~~~text
1 arquivo (job)
     |
     +--> chunk 00000001 --> vários registros --> eventos de saída
     +--> chunk 00000002 --> vários registros --> eventos de saída
     +--> chunk 00000003 --> vários registros --> eventos de saída
~~~

O Organizer cria faixas nominais de bytes para texto; o Worker resolve as
fronteiras dos registros ao ler as faixas. JSON e multi-line usam suas regras
estruturais de delimitação. O Worker usa S3 Range GET para ler apenas o trecho
que precisa processar. Em `text`, `targetChunkBytes` pode orientar o número de
chunks; sem esse valor, o planejamento visa 100 chunks, respeitando os
limites mínimo e máximo de tamanho.

O paralelismo real é limitado por:

- Quantidade de chunks planejados.
- Mensagens na fila compartilhada de chunks.
- Concorrência global do mapeamento SQS para o Worker.
- Concorrência reservada, memória e timeout do Worker.
- Limites de conta e capacidade de S3, SQS e DynamoDB.
- Capacidade dos consumidores da fila de saída compartilhada ou dedicada.

Mais concorrência não aumenta throughput quando o arquivo gera um único chunk
ou quando o destino é o gargalo.

---

## 6. Configuração e roteamento por prefixo

Cada prefixo registrado possui uma configuração no SSM Parameter Store. Para
notificações S3, o Organizer escolhe a correspondência de prefixo mais longa e
anexa um snapshot ao job, incluindo a versão e o hash do parâmetro. Mudanças
posteriores afetam novas admissões, sem alterar jobs já admitidos. Limites
globais também vêm do SSM. Requisições explícitas usam a configuração do
contrato e os padrões da Lambda, sem seleção de prefixo no SSM. Os padrões de
`outputMode`, `maxEnvelopesPerMessage` e `maxMessageBytes` também entram na
configuração dos chunks dessas requisições.

~~~text
s3://bucket/financeiro/arquivo.txt  --> configuração financeiro/
s3://bucket/financeiro/diario/x.txt --> configuração financeiro/diario/
                                          (correspondência mais específica)
~~~

A infraestrutura cria uma fila de chunks com DLQ, um Worker e uma fila de saída
padrão. Um prefixo pode optar por uma fila de saída dedicada; o Terraform
provisiona essa fila e grava sua URL na configuração SSM. `prefixId` e a URL de
saída seguem no snapshot do job. O Worker publica na fila indicada pelo job ou,
na ausência dela, na fila padrão. O exemplo `example-json/` usa fila dedicada;
os exemplos `example-text/` e `example-multi-line/` usam a fila padrão.

Chunks de todos os prefixos disputam a mesma capacidade do Worker. A role IAM
do Worker pode publicar nas filas de saída provisionadas e na fila de conclusão;
não há isolamento de execução ou permissão por prefixo. O ledger DynamoDB
também é compartilhado e não isola itens por IAM. Quando uma carga exigir
capacidade ou fronteira de segurança própria, a decisão é implantar um F2E
dedicado de ponta a ponta, conforme o [ADR 0009](adr/0009-implantacao-simples-compartilhada.md).

---

## 7. Entrega e idempotência

O F2E oferece comportamento at-least-once. SQS pode reentregar mensagens, e
não existe transação distribuída entre enviar uma mensagem e gravar um
checkpoint no ledger.

~~~text
Worker publica evento no SQS
          |
          v
falha antes de confirmar o chunk no ledger
          |
          v
SQS reentrega o ChunkJob
          |
          v
possível nova publicação do mesmo registro
~~~

O ledger reduz duplicações técnicas, mas consumidores precisam ser idempotentes.

| Identificador | Uso |
|---|---|
| fileId | Identidade física do arquivo: versão imutável ou `ETag`/tamanho observados na admissão. |
| jobId | Execução; muda em um replay explícito. |
| chunkId | Unidade de processamento dentro do job. |
| eventId | Deduplicação de retries da mesma execução. |
| sourceRecordId | Deduplicação do mesmo registro físico entre replays. |
| IDs de correlação | Propagam transactionId, correlationId e traceId. |

Use eventId para retries da mesma execução e sourceRecordId para impedir
repetição entre replays. A persistência de uma chave de idempotência ou uma
operação de negócio naturalmente idempotente continua sendo responsabilidade
do consumidor.

---

## 8. Retry, DLQ e mensagens problemáticas

O retry ocorre em duas camadas: o código tenta novamente operações de
publicação em lote de forma limitada; caso a falha persista, a mensagem SQS
permanece disponível para uma nova tentativa. As Lambdas usam falhas parciais
de lote para não repetir mensagens que já terminaram com sucesso.

~~~text
Falha ao publicar
      |
      v
retry local limitado
      |
      +--> sucesso: checkpoint e continuidade
      |
      +--> falha: redelivery SQS
                         |
                         v
                  maxReceiveCount excedido
                         |
                         v
                        DLQ
~~~

| Caminho | Destino de falha |
|---|---|
| file-intake | DLQ de intake |
| chunk-jobs | DLQ de chunks compartilhada |
| saída | O F2E não provisiona DLQ de consumo para a fila padrão ou dedicada; o consumidor define seu redrive. |
| conclusão | DLQ de completion-events |

DLQ isola a mensagem problemática; não a corrige nem a reprocessa sozinha. A
operação deve investigar a causa e confirmar se o replay é seguro.

---

## 9. Back-pressure e quotas

As filas SQS desacoplam a velocidade da origem, processamento e consumo.

~~~text
S3 mais rápido que Organizer    --> backlog em file-intake
Organizer mais rápido que Worker --> backlog em chunk-jobs compartilhada
Worker mais rápido que destino   --> backlog na fila de saída usada pelo job
~~~

O dimensionamento de filas, concorrência, tamanho de arquivo e capacidade
downstream controla o backlog. Não há quota de admissão persistida no ledger.
Uma carga de um prefixo pode atrasar os demais na fila de chunks compartilhada.
Uma fila de saída dedicada separa apenas o backlog de consumo daquele prefixo.

---

## 10. Conclusão e outbox

Quando o job se torna terminal, o ledger grava uma intenção de conclusão na
mesma transição persistente. O Worker envia o evento para a fila de conclusão
e só então confirma a entrega no ledger.

~~~text
Job terminal + intenção pendente (DynamoDB)
                    |
                    v
                 Worker
                    |
                    v
          SQS completion-events
                    |
                    v
       marca a intenção como entregue
~~~

Uma nova entrega do chunk ou da mensagem de controle reconcilia intenções
pendentes. Isso cobre falha entre registrar a intenção e entregá-la. Se
a falha ocorrer depois do envio e antes da confirmação, pode haver duplicação;
o eventId de conclusão permanece estável.

O evento de conclusão não é barreira de consumo: ele pode chegar antes de os
consumidores terminarem de ler os eventos de registros.

---

## 11. Ordenação, contrato e replay

As filas provisionadas são SQS Standard. Não há ordenação global de eventos,
nem ordenação garantida entre chunks processados em paralelo. O Worker preserva
a montagem determinística dentro de um chunk, mas consumidores não devem tratar
isso como garantia de ordem na fila.

O contrato de registros é o Envelope v1, com origem, posição, identificadores
e versão de schema. No modo `single`, cada mensagem contém um envelope. No modo
`bundle`, uma mensagem contém múltiplos envelopes, até os limites configurados
de quantidade e bytes; o consumidor precisa suportar esse formato. O lote SQS
(`batchSize`) controla quantas mensagens físicas são enviadas por chamada, não
quantos registros cabem em cada bundle. O Worker pode enviar lotes em paralelo
conforme `F2E_PUBLISH_CONCURRENCY`, sem criar garantia de ordem na fila.

O ledger armazena o plano imutável e checkpoints de agendamento. Ele permite:

- Retomar chunks planejados cuja publicação não foi confirmada.
- Executar replay explícito de um job ou subconjunto de chunks com novo jobId.

Em um replay, sourceRecordId continua associado ao mesmo registro físico e
eventId muda. Antes de reprocessar, confirme a estratégia de deduplicação e a
disponibilidade da versão original no S3.

---

## 12. Observabilidade operacional

O Terraform provisiona logs em CloudWatch, dashboard e alarmes para Lambdas,
filas principais e DLQs de intake, chunks e conclusão. O monitoramento definido
em `terraform/monitoring.tf` não inclui as filas de saída dedicadas.
Acompanhe principalmente:

- Idade e quantidade de mensagens nas filas.
- Qualquer mensagem em DLQ.
- Erros, throttles, duração e concorrência das Lambdas.
- Contadores do ledger e IDs jobId, fileId, chunkId e eventId.
- IDs de correlação propagados no envelope.

Não há DynamoDB Stream no caminho de conclusão: o Worker reconcilia e publica
a intenção persistida no ledger.

---

## 13. Trade-offs e perguntas de evolução

| Decisão | Benefício | Consequência |
|---|---|---|
| S3 versionado (preferível) | Origem imutável e replay reproduzível | Exige retenção e permissões para versões. |
| S3 não versionado | Menor exigência para a origem | Usa `ETag` condicional; sobrescrita falha o job e inviabiliza replay do conteúdo original. |
| SQS Standard | Desacoplamento e escala simples | Duplicatas e ausência de ordenação. |
| Chunks paralelos | Throughput para arquivos grandes | Ordem entre chunks não é preservada. |
| Ledger DynamoDB | Auditoria, recuperação e replay | Estado operacional e custo de escrita. |
| Outbox no ledger + reconciliação pelo Worker | Conclusão recuperável após reentrega de chunk ou controle | Entrega continua at-least-once. |
| Worker e fila de chunks compartilhados | Menos recursos e operação mais simples | Prefixos disputam a mesma capacidade. |
| Fila de saída dedicada opcional | Separa o backlog de consumo de um prefixo | Exige provisionamento e operação da fila adicional. |

Antes de incluir um prefixo, formato ou consumidor, responda:

- Qual é o volume, tamanho máximo e perfil de pico dos arquivos?
- Quantos chunks serão criados e como a carga dividirá a capacidade compartilhada?
- O consumidor suporta duplicação, replays e, se aplicável, bundles?
- Existe requisito real de ordenação? Se sim, qual é a chave?
- Quem monitora as DLQs de intake, chunks e conclusão, e o redrive do consumidor?
- A versão do arquivo continua acessível durante a janela de recuperação?
- Quais identidades podem publicar requisições explícitas?
- Retenção, criptografia, dados sensíveis e alarmes têm responsáveis definidos?

---

## 14. Referências de implementação

- cmd/organizer: entrada, admissão e planejamento.
- cmd/worker: processamento, publicação de registros e conclusões.
- internal/application: casos de uso e portas.
- internal/domain/f2e: contratos, estados, envelopes e identificadores.
- terraform: recursos AWS compartilhados e fila de saída dedicada opcional.
- [Desenho arquitetural](arquitetura.md): topologia visual.
- [Operação na AWS](operacao_aws.md) e [runbooks](runbooks.md): operação.
