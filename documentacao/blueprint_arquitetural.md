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
eventos. Ele admite uma versão imutável do arquivo, divide o trabalho em
chunks, extrai registros e os publica em filas de saída.

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
Filas de chunks por prefixo
          |
          v
Lambdas Worker por prefixo
          |
          v
Filas de eventos por prefixo
          |
          v
Consumidores downstream
~~~

O DynamoDB mantém o ledger de admissão, plano, chunks e estados terminais. Um
fluxo separado entrega eventos técnicos de conclusão.

~~~text
DynamoDB job-ledger -> DynamoDB Streams -> Completion Publisher
                                            |
                                            v
                                    SQS completion-events
~~~

---

## 3. Limites de responsabilidade

O F2E é responsável por converter uma versão imutável de arquivo em eventos
rastreáveis. Ele não executa regra de negócio downstream nem garante que cada
efeito de negócio seja aplicado uma única vez.

O escopo é inbound, File-to-Event. Event-to-File é deliberadamente externo ao
building block, e SNS não é um destino implementado por esta versão.

~~~text
ORIGEM                         F2E                              DESTINO
------                         ---                              -------
Produz arquivo          ->     Admite e planeja            ->   Consome evento
Define conteúdo         ->     Lê e interpreta registros   ->   Aplica negócio
Envia ao S3             ->     Publica Envelope v2         ->   Persiste estado
                           Registra execução e conclusão       Deduplica efeitos
~~~

### Responsabilidades do F2E

- Receber notificações S3 ou requisições explícitas na fila de intake.
- Selecionar a configuração de bucket e prefixo e registrar seu snapshot.
- Operar sobre a versão específica do objeto S3.
- Planejar, agendar e processar unidades de trabalho.
- Interpretar arquivos text, json em array e multi-line.
- Publicar Envelope v2 por registro, ou bundles quando configurado.
- Registrar estados, contadores, falhas e conclusão do job.
- Manter DLQs, logs, métricas e alarmes de infraestrutura.

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

O Organizer cria faixas de bytes para texto e respeita fronteiras estruturais
para JSON e multi-line. O Worker usa S3 Range GET para ler apenas o trecho que
precisa processar.

O paralelismo real é limitado por:

- Quantidade de chunks planejados.
- Mensagens na fila de chunks do prefixo.
- Concorrência do mapeamento SQS para Lambda.
- Concorrência reservada, memória e timeout do Worker.
- Limites de conta e capacidade de S3, SQS e DynamoDB.
- Capacidade dos consumidores da fila de saída.

Mais concorrência não aumenta throughput quando o arquivo gera um único chunk
ou quando o destino é o gargalo.

---

## 6. Configuração e isolamento por prefixo

Cada prefixo registrado possui uma configuração no SSM Parameter Store. O
Organizer escolhe a correspondência de prefixo mais longa e anexa um snapshot
imutável ao job; mudanças posteriores não alteram jobs já admitidos.

~~~text
s3://bucket/financeiro/arquivo.txt  --> configuração financeiro/
s3://bucket/financeiro/diario/x.txt --> configuração financeiro/diario/
                                          (correspondência mais específica)
~~~

Para cada prefixo, a infraestrutura cria uma fila de chunks e DLQ, um Worker,
uma fila de saída e DLQ, e permissões para que o Worker leia o S3, atualize o
ledger e publique somente na fila de saída do próprio prefixo.

Isso impede que Workers de prefixos diferentes consumam os chunks uns dos
outros. O ledger DynamoDB é compartilhado: oferece rastreabilidade
centralizada, mas não isolamento de dados item a item por IAM.

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
| fileId | Versão física e imutável do arquivo. |
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
| chunks | DLQ de chunks do prefixo |
| saída | DLQ de saída do prefixo, caso o consumidor falhe |
| conclusão | DLQ de completion-events |
| DynamoDB Streams | DLQ de completion-events após retries |

DLQ isola a mensagem problemática; não a corrige nem a reprocessa sozinha. A
operação deve investigar a causa e confirmar se o replay é seguro.

---

## 9. Back-pressure e quotas

As filas SQS desacoplam a velocidade da origem, processamento e consumo.

~~~text
S3 mais rápido que Organizer    --> backlog em file-intake
Organizer mais rápido que Worker --> backlog em chunks
Worker mais rápido que destino   --> backlog na fila de saída
~~~

O limite opcional de jobs ativos por prefixo impede admissão ilimitada. Sem
vaga, o Organizer registra a admissão como WAITING no ledger. O EventBridge
tenta liberar periodicamente a admissão mais antiga por prefixo, sem deixar uma
Lambda aguardando vaga.

Essa quota limita arquivos ativos, não substitui o dimensionamento de filas,
concorrência, tamanho de arquivo ou capacidade downstream.

---

## 10. Conclusão e outbox

Quando o job se torna terminal, o ledger grava uma intenção de conclusão na
mesma transição persistente. O Completion Publisher recebe o Stream, envia o
evento para a fila de conclusão e confirma a entrega no ledger.

~~~text
Job terminal + intenção pendente (DynamoDB)
                    |
                    v
             DynamoDB Streams
                    |
                    v
          Completion Publisher
                    |
                    v
          SQS completion-events
                    |
                    v
       marca a intenção como entregue
~~~

Uma regra EventBridge consulta intenções pendentes a cada cinco minutos. Isso
cobre expiração do Stream ou falha entre registrar a intenção e entregá-la. Se
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

O contrato de registros é o Envelope v2, com origem, posição, identificadores
e versão de schema. O modo bundle muda a mensagem física para conter múltiplos
envelopes e requer suporte explícito no consumidor.

O ledger armazena o plano imutável e checkpoints de agendamento. Ele permite:

- Retomar chunks planejados cuja publicação não foi confirmada.
- Executar replay explícito de um job ou subconjunto de chunks com novo jobId.

Em um replay, sourceRecordId continua associado ao mesmo registro físico e
eventId muda. Antes de reprocessar, confirme a estratégia de deduplicação e a
disponibilidade da versão original no S3.

---

## 12. Observabilidade operacional

O Terraform provisiona logs estruturados em CloudWatch, métricas, dashboard e
alarmes para Lambdas, filas e DLQs. Acompanhe principalmente:

- Idade e quantidade de mensagens nas filas.
- Qualquer mensagem em DLQ.
- Erros, throttles, duração e concorrência das Lambdas.
- IteratorAge do Stream de conclusão.
- Contadores do ledger e IDs jobId, fileId, chunkId e eventId.
- IDs de correlação propagados no envelope.

O procedimento de atraso do Stream está em
[runbooks/streams-iterator-age.md](runbooks/streams-iterator-age.md).

---

## 13. Trade-offs e perguntas de evolução

| Decisão | Benefício | Consequência |
|---|---|---|
| S3 versionado | Origem imutável | Exige retenção e permissões para versões. |
| SQS Standard | Desacoplamento e escala simples | Duplicatas e ausência de ordenação. |
| Chunks paralelos | Throughput para arquivos grandes | Ordem entre chunks não é preservada. |
| Ledger DynamoDB | Auditoria, recuperação e replay | Estado operacional e custo de escrita. |
| Outbox + Stream | Conclusão resiliente a falhas | Entrega continua at-least-once. |
| Recursos por prefixo | Isolamento de capacidade e filas | Mais recursos para operar. |

Antes de incluir um prefixo, formato ou consumidor, responda:

- Qual é o volume, tamanho máximo e perfil de pico dos arquivos?
- Quantos chunks serão criados e onde estará o gargalo de throughput?
- O consumidor suporta duplicação, replays e, se aplicável, bundles?
- Existe requisito real de ordenação? Se sim, qual é a chave?
- Quem monitora cada DLQ e quem aprova seu reprocessamento?
- A versão do arquivo continua acessível durante a janela de recuperação?
- Quais identidades podem publicar requisições explícitas?
- Retenção, criptografia, dados sensíveis e alarmes têm responsáveis definidos?

---

## 14. Referências de implementação

- lambdas/cmd/organizer: entrada, admissão e planejamento.
- lambdas/cmd/worker: processamento e publicação de registros.
- lambdas/cmd/completion-publisher: entrega de conclusões.
- internal/application: casos de uso e portas.
- internal/domain/f2e: contratos, estados, envelopes e identificadores.
- terraform: recursos AWS e módulos por prefixo.
- [Desenho arquitetural](arquitetura.md): topologia visual.
- [Operação na AWS](operacao_aws.md) e [runbooks](runbooks.md): operação.
