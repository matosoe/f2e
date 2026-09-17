# Runbooks operacionais do F2E

Todo incidente deve registrar ambiente, `jobId`, `chunkId`, identidade física do
objeto, receive count, horário e ação adotada. Nunca registrar payload ou URL
pré-assinada.

Ao classificar um incidente, diferencie `COMPLETED/SUCCESS`,
`COMPLETED/WITH_REJECTIONS`, `FAILED` e `REJECTED`. Em `WITH_REJECTIONS`, a
execução técnica terminou, mas há registros rejeitados; em `FAILED`, a
contagem final pode ser parcial. Consulte [Requisitos e restrições](requisitos_e_restricoes.md)
antes de aprovar replay ou reenfileiramento.

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
Lambda. Chunks presos devem ser reprocessados pela referência registrada: uma
versão imutável quando houver `VersionId`, ou uma leitura condicional por `ETag`
em bucket não versionado.

## Objeto alterado, removido ou URL expirada

Não substituir silenciosamente a origem. A leitura deve usar VersionId ou ETag
condicional. Se a versão não existir, encerrar o job com evidência. URL expirada
exige nova ingestão autorizada; a credencial antiga nunca entra no ledger.

## Replay

Replay cria novo `jobId` e `eventId`, preservando `fileId` e `sourceRecordId`.
Em bucket não versionado, só o execute se o objeto ainda corresponder ao `ETag`
registrado; caso contrário, faça nova ingestão. Registrar solicitante, motivo,
horário, escopo e resultado.

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
`PrefixID` nem `OutputQueueURL`; parâmetros SSM com esses
campos provocam validação com erro na versão legada. Antes de restaurar um
binário sem suporte a T20–T22, remover ou zerar esses campos em todos os
parâmetros afetados.

## Recuperação de conclusão pendente

O Worker cria a intenção de conclusão junto da transição terminal e a entrega
para `completion-events`. Se a invocação falhar após o envio e antes da
marcação, o redrive da mensagem de chunk ou de controle pode reenviar o mesmo
`eventId`; consumidores devem deduplicá-lo. Se a mensagem esgotar as tentativas,
ela fica na DLQ de chunks: faça redrive após corrigir a causa e não altere a
intenção diretamente no DynamoDB.

