# T01 — Matriz de migração e estratégia de contratos

Data: 2026-09-05. Parte do plano `evolucao_f2e_tres_modos_operacao_e_performance.md`.

## 1. Campos e tipos removidos

### 1.1 `DataType` — valores removidos

| Campo antigo | Destino/remoção | Consumidores afetados | Procedimento |
|---|---|---|---|
| `fixed-width` | Removido | Prefixos SSM com `dataType: fixed-width`; Organizer; Worker; testes E2E | Reconfigurar como `text` com `maxRecordLengthBytes = recordLengthBytes`; exige que arquivo tenha LF ao fim de cada linha. |
| `jsonl` | Removido | Prefixos SSM com `dataType: jsonl`; Organizer; Worker; testes E2E | Reconfigurar como `text`. Perde validação JSON por linha (se `bypassJsonValidation=false`). |
| `ndjson` | Removido (alias de `jsonl`) | Idem `jsonl` | Idem `jsonl`. |
| `csv` (coluna única / LF) | Removido | Prefixos SSM com `dataType: csv` | Reconfigurar como `text` se cada linha CSV for um registro sem aspas e sem quebra de linha interna. |
| `csv` (campos multi-linha / RFC 4180) | Sem equivalente | Prefixos SSM com `dataType: csv` e campos com quebra interna | Parsing RFC 4180 deve ser feito pelo consumidor; F2E entrega o arquivo bruto ou o prefixo é migrado para processador externo. |
| `binary` | Removido | Prefixos SSM com `dataType: binary` | Não há equivalente nos três modos; chamador deve usar contrato de referência S3. |

### 1.2 Campos de `PrefixConfiguration` removidos

| Campo | Motivo da remoção | Substituto |
|---|---|---|
| `recordLengthBytes` | Exclusivo de `fixed-width`; substituído por `maxRecordLengthBytes` | `maxRecordLengthBytes` em todos os modos variáveis |
| `options.bypassJsonValidation` | Exclusivo de `jsonl`/`ndjson` | Não aplicável; `text` não valida JSON |

### 1.3 Campos de `RecordPayload` / `data` do envelope removidos

| Campo | Motivo | Modo afetado | Ação |
|---|---|---|---|
| `data.fields` | Gerado pelo parser CSV | `csv` | Removido junto com o modo |
| `data.base64` | Gerado pelo modo `binary` | `binary` | Removido junto com o modo |

### 1.4 Campos de `ProcessingOptions` removidos

| Campo | Motivo | Ação |
|---|---|---|
| `bypassJsonValidation` | Exclusivo de `jsonl`/`ndjson` | Removido; campo ignorado em `text` |

### 1.5 Fallback implícito removido

| Comportamento | Localização | Ação |
|---|---|---|
| `DataType == ""` → `fixed-width` (worker/service.go:83) | `internal/application/worker/service.go` | Remover; job sem `dataType` produz erro explícito |
| Validação de `BypassJSONValidation` em tipos não-JSON | `internal/application/organizer/service.go` e `worker/service.go` | Remover campo e validação |

## 2. Campos e contratos preservados

| Campo / Contrato | Status | Observação |
|---|---|---|
| `DataTypeText` | Preservado | Modo principal para linhas físicas |
| `DataTypeMultiLine` | Preservado | Modo para registros agrupados |
| `DataTypeJSON` | Preservado | Modo para arrays JSON estruturais |
| `maxRecordLengthBytes` | Preservado | Obrigatório em `text` e `multi-line` para planejamento de chunks |
| `MultiLineLayout` | Preservado | Sem alterações de campos nesta tarefa |
| `JSONArrayLayout` | Preservado | Sem alterações de campos nesta tarefa |
| `ChunkJob` core (jobId, chunkId, fileId, bucket, key...) | Preservado | Campos de identidade não mudam em T01 |
| Envelope v2 (`eventId`, `sourceRecordId`, `metadata`, `source`, `processing`, `data.raw`) | Preservado | `data.fields` e `data.base64` removidos em T04 |

## 3. Versões de contrato e estratégia para jobs legados

### 3.1 Versão atual

`schemaVersion: "1"` em `OrganizerRequest`, `ChunkJob` e envelope v2.

### 3.2 Estratégia de migração

Não há bump de `schemaVersion` neste plano: a remoção de campos é feita por
rejeição explícita de `dataType` não suportado, não por versão de envelope.

Estratégia para jobs legados em fila no momento da migração:

1. **Drenar** as filas de chunks (`fixed-width`, `csv`, `binary`, `jsonl`,
   `ndjson`) em ambiente controlado antes do deploy do Worker sem esses tipos.
2. **Manter executor antigo isolado** se a drenagem não for possível
   (ex.: fila com mensagens retidas por DLQ): operá-lo até esgotamento, depois
   desativar.
3. **Não reinterpretar** `ChunkJob` com `dataType: fixed-width` como `text`
   silenciosamente; produzir erro explícito e mover para DLQ para análise.

### 3.3 Jobs afetados por ambiente

| Ambiente | Ação recomendada |
|---|---|
| LocalStack / desenvolvimento | Apagar filas e recriar após deploy |
| Homologação AWS | Drenar com Worker antigo; confirmar filas vazias antes de trocar |
| Produção | Janela de manutenção; manter Worker antigo paralelo por período de retenção das mensagens (máximo SQS Standard: 14 dias) |

## 4. Consumidores e arquivos afetados

| Arquivo | Tipo de mudança | Tarefa |
|---|---|---|
| `internal/domain/f2e/messages.go` | Remover constantes de tipo; remover campos `Fields`, `Base64`, `BypassJSONValidation`, `RecordLengthBytes` | T04 |
| `internal/application/organizer/service.go` | Remover validações de tipos antigos; remover referência a `bypassJsonValidation` | T04 |
| `internal/application/worker/service.go` | Remover fallback `""→fixed-width`; remover validação de tipos antigos; remover `BypassJSONValidation` | T04 |
| `internal/domain/fixedwidth/reader.go` | Renomear pacote; manter somente lógica `multi-line` | T04 |
| `internal/platform/aws/prefix_configuration.go` | Remover `RecordLengthBytes`; ajustar validação de `GlobalLimits.InputTypes` | T04, T05 |
| `lambdas/cmd/organizer/main.go` | Validação de tipos suportados | T05 |
| `lambdas/cmd/worker/main_test.go` | Atualizar cenários de tipos removidos | T04, T05 |
| `e2e/internal/producers.go` | Atualizar geradores de requests | T05 |
| `e2e/internal/steps.go` | Atualizar asserções de cenários | T05 |
| `e2e/internal/types.go` | Remover campos `Fields`, `Base64` de `EnvData` | T04 |
| `terraform/locals.tf` | Remover variáveis de tipos removidos | T05 |
| `documentacao/contratos.md` | Atualizar lista de tipos, campos e exemplos | T05 |
| `documentacao/adr/0004-formatos-suportados.md` | Marcar supersedido por ADR 0005 | T01 (já registrado) |
| `documentacao/schemas/envelope-v2.schema.json` | Remover `fields` e `base64` de `data` | T04 |

## 5. Fixtures de contratos

Fixtures estão em `documentacao/schemas/fixtures/`:

- `organizer-request-legado.json` — Exemplo de entrada com tipo removido (referência histórica).
- `organizer-request-text.json` — Entrada com `text` (novo padrão).
- `organizer-request-json.json` — Entrada com `json`.
- `organizer-request-multi-line.json` — Entrada com `multi-line`.
- `chunk-job-text.json` — `ChunkJob` com `text`.
- `chunk-job-json.json` — `ChunkJob` com `json`.
- `chunk-job-multi-line.json` — `ChunkJob` com `multi-line`.
- `envelope-v2-text.json` — Envelope de saída com `data.raw` de registro `text`.
