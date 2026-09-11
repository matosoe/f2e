# Requisitos e restrições do building block F2E

## 1. Escopo

O F2E resolve exclusivamente o fluxo inbound File-to-Event:

~~~text
Arquivo recebido -> eventos de registros em SQS
~~~

A entrada normal é um objeto versionado no Amazon S3. Uma integração também pode
enviar uma requisição explícita para a fila de intake, desde que forneça o
contrato necessário.

O F2E não implementa o fluxo outbound Event-to-File:

~~~text
Eventos -> arquivo
~~~

Esse fluxo requer outro building block ou módulo, com contrato, persistência,
agrupamento, ordenação e estratégia de entrega próprios. A implementação atual
publica eventos em SQS; SNS não é um destino provisionado por este projeto.

## 2. Requisitos funcionais

| Requisito | Critério de atendimento |
|---|---|
| Converter arquivo em eventos | Cada registro lógico elegível gera um Envelope v2, ou integra um bundle quando esse modo estiver configurado. |
| Iniciar sob demanda | Um ObjectCreated do S3 ou uma requisição explícita entra na fila de intake e aciona o Organizer. |
| Processar arquivos grandes em paralelo | O Organizer produz chunks e Workers processam-nos concorrentemente. |
| Rastrear a execução | O ledger registra arquivo, job, chunks, contagens, falhas, configuração e estado terminal. |
| Sinalizar término | Um evento de conclusão é publicado em fila separada para jobs concluídos, falhos ou rejeitados. |
| Recuperar trabalho técnico | Checkpoints do ledger, retries SQS, DLQs, outbox e replay explícito permitem reconciliação. |

A janela de processamento é um requisito de adoção: ela deve ser definida pelo
caso de uso e validada em teste de carga. A configuração de chunks, concorrência
e recursos Lambda deve permitir que o prazo acordado seja atingido sem exceder
os limites do ambiente.

## 3. Condições de elegibilidade do arquivo

O F2E só é adequado quando a fronteira de cada registro lógico pode ser
determinada de forma previsível e limitada.

| Formato | Regra suportada | Limite necessário para paralelizar |
|---|---|---|
| text | Uma linha terminada por CR, LF ou CRLF. | maxRecordLengthBytes para arquivos divididos. |
| json | Um elemento de array no arrayPath configurado. | maxBytesPerElement e janela de busca do array. |
| multi-line | Linhas agrupadas por marcador de início e prefixes aceitos. | maxBytesPerRecord e regra de marcador. |

O arquivo deve respeitar os máximos configurados para tamanho total, chunk,
registro e mensagem. O Organizer rejeita contratos ou arquivos fora desses
limites; o Worker falha o chunk quando não consegue provar uma fronteira válida
dentro do limite declarado.

### Não elegíveis sem extensão específica

- Arquivos com hierarquia arbitrária ou registros cuja fronteira só pode ser
  descoberta após leitura integral e sem limite máximo conhecido.
- Arquivos que exigem quebra cega em bytes dentro de grupos hierárquicos.
- Formatos removidos ou não implementados, como CSV, XML, fixed-width, JSONL,
  NDJSON e binário.
- Registros que excedem o limite serializado da mensagem de destino.
- Casos cuja janela de processamento não caiba na capacidade e no timeout das
  Lambdas configuradas.

Nesses cenários, use outro processo de ingestão ou desenvolva um adaptador com
semântica de parsing e recursos compatíveis. Não basta aumentar concorrência:
sem fronteira previsível, dividir o arquivo pode corromper o registro lógico.

## 4. Restrições arquiteturais

### S3

- A origem é um bucket S3 com versionamento habilitado.
- O Organizer fixa bucket, key e VersionId antes de planejar o job.
- A retenção das versões precisa cobrir a janela de retry e replay.
- Notificações S3 apenas iniciam o fluxo; não são confirmação de processamento
  completo.

### SQS

- Intake, chunks, eventos de saída e conclusão são filas SQS.
- As filas são Standard: a entrega é at-least-once e não há ordenação global.
- Corpo, atributos e overhead serializado devem caber no máximo de mensagem.
- A redrive policy move mensagens que excedem as tentativas para DLQs.
- Consumidores downstream são responsáveis pela exclusão da mensagem, DLQ de
  saída e idempotência dos próprios efeitos.

### Lambda

- Organizer, Workers e Completion Publisher executam sob demanda.
- Tempo de execução, memória, armazenamento efêmero, concorrência e limites da
  conta restringem throughput e tamanho de trabalho por invocação.
- O Worker lê intervalos S3; ele não deve depender de carregar todo arquivo na
  memória para casos paralelizáveis.
- Dimensionamento precisa ser validado com arquivos representativos e SLOs
  definidos antes de produção.

### DynamoDB e SSM

- DynamoDB é o ledger técnico: não substitui a persistência de negócio dos
  consumidores.
- SSM guarda configuração por prefixo e limites globais.
- O snapshot de configuração entra no job; alterar SSM afeta somente novas
  admissões.

## 5. Ordem, entrega e conclusão

O paralelismo depende da independência entre registros. Não use o F2E quando a
ordem entre registros for requisito obrigatório, a menos que o consumidor
implemente uma estratégia própria de chave, reordenação ou persistência.

~~~text
at-least-once
      |
      v
duplicatas possíveis
      |
      v
consumidor idempotente
~~~

Use eventId para deduplicar retries da mesma execução e sourceRecordId para
deduplicar o mesmo registro físico entre replays. Não há transação distribuída
entre SQS e DynamoDB.

O job pode terminar em:

| Estado | Significado |
|---|---|
| COMPLETED / SUCCESS | Todos os registros tiveram o resultado esperado. |
| COMPLETED / WITH_REJECTIONS | A execução técnica terminou, mas há registros rejeitados; consultar contagens e motivos. |
| FAILED | Uma falha técnica impediu resultado completo; countsComplete pode ser falso. |
| REJECTED | Arquivo ou configuração não atende ao contrato antes da conclusão normal. |

O evento de conclusão não é barreira para consumo dos eventos de registros:
pode ser entregue antes de todos eles serem consumidos downstream.

## 6. Capacidade, back-pressure e isolamento

O arquivo é a unidade de admissão; o chunk é a unidade de paralelismo e retry;
o registro é a unidade lógica de saída.

~~~text
arquivo -> chunks -> Workers paralelos -> eventos
~~~

O throughput depende do número de chunks, concorrência SQS-Lambda, limites AWS
e capacidade de consumidores. Backlogs nas filas são o mecanismo de
desacoplamento e um sinal operacional de que uma etapa está mais lenta que a
anterior.

Cada prefixo registrado tem Worker e filas de chunks/saída próprios. Essa
separação evita que um prefixo consuma a fila de chunks de outro. O limite
opcional maxActiveJobs impede admitir arquivos ilimitadamente por prefixo; os
excedentes ficam em WAITING no ledger até a tentativa periódica de liberação.

## 7. Decisão de adoção

Antes de adotar o building block, confirme:

1. O problema é File-to-Event inbound, não Event-to-File.
2. Os registros são independentes ou suportam o tratamento de ordem no destino.
3. A regra de fronteira e o tamanho máximo do registro são conhecidos.
4. O arquivo, chunk, registro e mensagem cabem nos limites configurados.
5. A janela de processamento cabe na capacidade dimensionada de Lambda e SQS.
6. O consumidor é idempotente e suporta o contrato de saída escolhido.
7. Há responsável por alarmes, DLQs, retenção e autorização.
8. O replay é seguro para os efeitos de negócio e a versão do S3 será retida.

## 8. Referências

- [Arquitetura](arquitetura.md)
- [Blueprint arquitetural](blueprint_arquitetural.md)
- [Operação local](operacao_local.md)
- [Operação na AWS](operacao_aws.md)
- [Runbooks](runbooks.md)

