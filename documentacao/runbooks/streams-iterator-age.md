# Runbook: Completion-Publisher DynamoDB Streams Iterator Age

## Alarme

`{prefix}-{env}-completion-publisher-iterator-age`

Dispara quando o `IteratorAge` da Lambda `completion_publisher` ultrapassa **5 minutos** por 5
períodos consecutivos de 1 minuto (janela total: 5 minutos).

## Sintoma

A Lambda `completion_publisher` está atrasada na leitura do DynamoDB Stream da tabela de ledger.
Isso significa que eventos de conclusão de jobs não estão sendo publicados no SQS
`completion_events`, o que atrasa a entrega das notificações para os consumidores downstream.

## Passos de diagnóstico

### 1. Verificar se a Lambda está saudável

```sh
# Erros recentes (últimos 30 min)
aws logs filter-log-events \
  --log-group-name /aws/lambda/{prefix}-{env}-completion-publisher \
  --start-time $(date -d '-30 min' +%s000) \
  --filter-pattern ERROR
```

Se houver erros repetidos, veja a seção "Lambda com erros".

### 2. Verificar DLQ do Streams

O event-source-mapping não tem DLQ própria, mas a Lambda pode gravar erros em seu log.
Se mensagens foram descartadas após `bisect_batch_on_function_error`, verifique:

```sh
aws sqs get-queue-attributes \
  --queue-url $(aws sqs get-queue-url --queue-name {prefix}-{env}-completion-events-dlq --output text) \
  --attribute-names ApproximateNumberOfMessages
```

### 3. Verificar o IteratorAge atual

```sh
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name IteratorAge \
  --dimensions Name=FunctionName,Value={prefix}-{env}-completion-publisher \
  --start-time $(date -u -d '-1 hour' +%FT%TZ) \
  --end-time $(date -u +%FT%TZ) \
  --period 60 \
  --statistics Maximum
```

### 4. Verificar shards do Stream

```sh
aws dynamodbstreams describe-stream \
  --stream-arn $(aws dynamodb describe-table --table-name {prefix}-{env}-job-ledger \
    --query 'Table.LatestStreamArn' --output text) \
  --query 'StreamDescription.Shards[*].{id:ShardId,seq:SequenceNumberRange}'
```

Se houver shards antigos não fechados, pode ser necessário atualizar o event-source-mapping.

## Ações corretivas

### Lambda com erros

1. Verifique se a IAM role tem permissão para `dynamodb:GetRecords` / `dynamodb:GetShardIterator`.
2. Verifique se o `F2E_LEDGER_TABLE` e `F2E_COMPLETION_QUEUE_URL` estão configurados no Lambda.
3. Se for pânico (panic), abra issue e considere rollback do alias:
   ```sh
   aws lambda update-alias \
     --function-name {prefix}-{env}-completion-publisher \
     --name live \
     --function-version {versão_anterior}
   ```

### IteratorAge alto sem erros

O volume de inserts no ledger pode ter aumentado. Verifique o throughput e considere:
- Aumentar a concorrência do event-source-mapping (parâmetro `parallelization_factor`).
- Verificar throttles de DynamoDB Streams.

### RecoverPending manual

Se precisar forçar a re-publicação de intents com `delivered=false`:

```sh
# Invoke Lambda com evento sintético de RecoverPending
aws lambda invoke --function-name {prefix}-{env}-completion-publisher \
  --payload '{"recover":true}' /dev/stdout
```

> O handler detecta o campo `recover` e chama `RecoverPending` diretamente.

## Escalada

Se o alarme permanecer ativo por mais de 30 minutos sem resolução, escale para o time de plataforma.
