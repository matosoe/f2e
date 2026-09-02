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
