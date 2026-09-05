# Runbooks operacionais do F2E

Todo incidente deve registrar ambiente, `jobId`, `chunkId`, identidade física do
objeto, receive count, horário e ação adotada. Nunca registrar payload ou URL
pré-assinada.

## DLQ de intake ou chunks

1. suspender o event-source mapping se o volume estiver crescendo;
2. classificar a falha como transitória, permanente ou de contrato;
3. confirmar que bucket/key/VersionId/ETag ainda identificam o objeto original;
4. corrigir a causa e redirecionar somente mensagens aprovadas;
5. reconciliar `expectedChunks`, concluídos, falhos e pendentes no ledger.

## Falha de publicação

Inspecionar falhas parciais do `SendMessageBatch`, throttling e permissões/KMS.
Repetir somente os itens falhos, preservando IDs. Não republicar o arquivo.

## Job sem progresso

Consultar chunks `PENDING`, `RUNNING` e `FAILED`, idade da mensagem e concorrência
Lambda. Chunks presos devem ser reprocessados pela referência imutável registrada.

## Objeto alterado, removido ou URL expirada

Não substituir silenciosamente a origem. A leitura deve usar VersionId ou ETag
condicional. Se a versão não existir, encerrar o job com evidência. URL expirada
exige nova ingestão autorizada; a credencial antiga nunca entra no ledger.

## Replay

Replay cria novo `jobId` e `eventId`, preservando `fileId`, `sourceRecordId` e a
versão física. Registrar solicitante, motivo, horário, escopo e resultado.

## Suspensão emergencial

Definir a concorrência do event-source mapping como zero/desabilitá-lo, preservar
as filas e comunicar o owner. Retomar gradualmente após validar KMS, S3, SQS,
Lambda e DynamoDB.

## Rollback de implantação

1. suspender os event-source mappings se a versão nova estiver produzindo dados
   incorretos; não apagar filas nem o ledger;
2. identificar no workflow de CI o último `artifact_run_id` aprovado e confirmar
   a assinatura e o `SHA256SUMS` daquele artefato;
3. executar novamente o workflow de promoção para o mesmo ambiente usando esse
   artefato imutável; o Terraform publicará versões novas e moverá os aliases
   `live` de Organizer e Worker de forma controlada;
4. executar os smoke tests, reabilitar gradualmente os mappings e reconciliar os
   jobs que estavam `PENDING`, `RUNNING` ou `SCHEDULING_FAILED`;
5. registrar versão defeituosa, versão restaurada, janela do incidente e escopo
   de replay. Nunca reconstruir localmente um ZIP para rollback.

**Rollback com modo bundle ativo:** se a versão problemática introduziu bundle
(`F2E_BUNDLE_MAX_ENVELOPES > 1`) e o consumidor downstream aceita apenas
envelopes individuais, não basta restaurar o binário — é preciso também:

1. garantir que o `F2E_BUNDLE_MAX_ENVELOPES` do Worker seja redefinido para `1`
   nos parâmetros SSM antes da reativação dos mappings;
2. verificar que nenhum consumidor downstream exibe erros de parse de
   `schemaVersion: "f2e-bundle/1"` — mensagens em trânsito já publicadas
   continuam com o formato bundle e exigem tratamento no consumidor;
3. confirmar com o owner do consumidor que a mudança é reversível sem perda de
   dados e registrar o acordo antes de reativar.

**Rollback e jobs novos em binário legado:** o binário legado não reconhece
`PrefixID`, `OutputQueueURL` nem `MaxActiveJobs`; parâmetros SSM com esses
campos provocam validação com erro na versão legada. Antes de restaurar um
binário sem suporte a T20–T22, remover ou zerar esses campos em todos os
parâmetros afetados.

## Quota de prefixo esgotada (T22)

Quando um prefixo configurado com `maxActiveJobs > 0` atingiu sua cota, novas
mensagens de intake retornam ao SQS via visibility timeout sem entrar no DLQ.

Diagnóstico:
1. checar logs do Organizer por `ErrQuotaExceeded` com o `prefixId` afetado;
2. consultar o DynamoDB: `GetItem pk=QUOTA#<prefixId> sk=QUOTA` para ver o
   `activeJobs` atual;
3. listar jobs `RECEIVED`, `VALIDATING` ou `PLANNING` para esse `prefixId` que
   possam estar presos (lease expirado).

Recuperação:
1. se jobs estiverem presos com lease expirado, forçar `ReleaseSlot` manualmente:
   `UpdateItem` em `pk=QUOTA#<prefixId> sk=QUOTA` decrementando `activeJobs`;
2. se o contador estiver inconsistente (negativo ou maior que o real),
   recriar o item com `activeJobs = <jobs reais não terminais>`;
3. ajustar `maxActiveJobs` no parâmetro SSM do prefixo para um valor compatível
   com a carga atual, se a cota for muito baixa.

Prevenção:
- Monitorar `activeJobs` via CloudWatch Metrics ou DynamoDB Streams;
- configurar alarme quando `activeJobs ≥ maxActiveJobs * 0.9`;
- garantir que `FinalizeJob` e `RejectJob` sempre chegam a término — jobs que
  falham repetidamente sem chegar a terminal devem ser investigados pelo DLQ de
  chunks.

## Recuperação de intent de conclusão (outbox, T12–T13)

O intent de conclusão (`COMPLETION_INTENT#<version>`) sobrevive a crashes do
publisher. Se o publisher Lambda reiniciar antes de marcar o intent como
entregue, a mensagem pode ser reenviada. Consumidores devem deduplicar pelo
`eventId` determinístico.

Diagnóstico:
1. consultar GSI `pending-intents-index` com `intentPending = "1"`;
2. verificar se a mensagem foi entregue na fila de conclusão (receber sem deletar);
3. se entregue mas não marcada: chamar `MarkIntentDelivered` manualmente.

Recuperação automática: o publisher Lambda faz scan do GSI em cada ciclo e
republica intents pendentes; a duplicação é detectada pelo `eventId` idêntico.
Não alterar o intent no DynamoDB fora do fluxo normal — o campo `intentPending`
é a única fonte de verdade para o estado de entrega.

