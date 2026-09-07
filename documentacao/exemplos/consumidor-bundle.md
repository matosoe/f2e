# Consumidor de Bundle F2E

Este documento mostra como consumir mensagens no modo `outputMode=bundle` (T15).

## Contrato

A fila de saída pode conter dois tipos de mensagem:

| Campo `schemaVersion` | Tipo           | Schema                   |
|-----------------------|----------------|--------------------------|
| ausente ou outro      | Envelope único | `envelope-v2.schema.json` |
| `f2e-bundle/1`        | Bundle         | `bundle-v1.schema.json`   |

**Consumidores existentes no modo `single` não precisam ser alterados** enquanto não optarem por bundle.

Para assinar a fila de bundle o operador deve:
1. Definir `outputMode=bundle` na configuração do prefixo no SSM.
2. Atualizar a função consumidora para inspecionar `schemaVersion` e processar `BundleEnvelope`.

## Modelo de retry

Um bundle retentado re-entrega **todos** os itens. O consumidor deve ser idempotente por `eventId`.

Estratégia recomendada: armazenar eventIds processados com TTL (ex: DynamoDB) e ignorar duplicatas na reprocessação.

```go
// pseudo-código Go — ilustrativo; adaptar ao framework do consumidor
func handleSQSMessage(body string) error {
    var probe struct{ SchemaVersion string `json:"schemaVersion"` }
    json.Unmarshal([]byte(body), &probe)

    if probe.SchemaVersion == f2e.BundleSchemaVersion {
        return handleBundle(body)
    }
    return handleSingleEnvelope(body)
}

func handleBundle(body string) error {
    var bundle f2e.BundleEnvelope
    if err := json.Unmarshal([]byte(body), &bundle); err != nil {
        return err
    }
    for _, item := range bundle.Items {
        if alreadyProcessed(item.Metadata.EventID) {
            continue // deduplicação por eventId
        }
        if err := processEnvelope(item); err != nil {
            // Retornar erro faz o SQS re-entregar o bundle inteiro após visibilityTimeout.
            // Todos os itens serão re-entregues; idempotência garante que itens já
            // processados sejam ignorados na próxima tentativa.
            return fmt.Errorf("bundle %s item %s: %w", bundle.BundleID, item.Metadata.EventID, err)
        }
        markProcessed(item.Metadata.EventID)
    }
    return nil
}
```

## Envelope único que excede MaxMessageBytes

Se um único envelope serializado superar `maxMessageBytes`, o pipeline emite um erro explícito
(`ErrEnvelopeTooLarge`) e **rejeita o registro** (não trunca, não particiona automaticamente).

O registro aparece nas contagens de `rejected` no evento de conclusão do job, com motivo
`OVERSIZED_BUNDLE_ITEM`. O operador deve aumentar `maxMessageBytes` ou dividir o arquivo.

## Configuração de exemplo (SSM PrefixConfiguration)

```json
{
  "bucket": "f2e-input",
  "prefix": "entrada/grandes/",
  "dataType": "text",
  "recordsPerChunk": 500,
  "batchSize": 10,
  "maxEventBytes": 262144,
  "maxFileBytes": 10737418240,
  "maxChunkBytes": 67108864,
  "jsonArraySearchBytes": 1048576,
  "eventSchemaId": "meu-sistema/evento-v1",
  "eventSchemaVersion": "1",
  "eventFormat": "application/json",
  "outputMode": "bundle",
  "maxEnvelopesPerMessage": 50,
  "maxMessageBytes": 240000
}
```

`maxMessageBytes` (240 000 bytes) é menor que `maxEventBytes` (256 KiB) — a diferença acomoda o
overhead do wrapper JSON do bundle.

## Migração de single → bundle

1. Definir `outputMode=bundle` + `maxEnvelopesPerMessage` + `maxMessageBytes` no SSM.
2. **Não alterar** a fila de saída; apenas o corpo da mensagem muda.
3. Atualizar o consumidor para tratar `schemaVersion=f2e-bundle/1` antes de ativar a nova config.
4. Testar com um arquivo pequeno em staging; validar contagens no evento de conclusão.
5. Documentar em `contratos.md` que este prefixo usa bundle.
