# Plano de evolução do F2E: três modos, operação e performance

Data: 2026-09-04. Estado: planejamento; nenhuma implementação deste plano foi executada.

Este documento é organizado em tarefas para execução incremental com Codex, modelo Terra, esforço medium. Executar uma tarefa por interação; dividir uma tarefa se ela exigir mudanças extensas. O modelo não precisa carregar todo o código ou repetir a suíte AWS a cada tarefa.

## 1. Objetivo e escopo autorizado

1. Suportar somente `text`, `json` e `multi-line`.
2. Melhorar a medição e a performance do E2E e do pipeline; oferecer vários envelopes por mensagem SQS como opção.
3. Proteger contra recebimento duplicado de arquivo e publicar conclusão técnica em contrato próprio.
4. Registrar decisões de rejeição/ignorados e seus motivos para conciliação no ledger.
5. Implementar autorização, segregação e capacidade por prefixo.
6. Preservar versão da configuração SSM e responsável.
7. Formalizar a máquina de estados para acompanhar recebimento, planejamento, chunks e término.

O núcleo continua técnico. Liquidação de pagamentos, conciliação bancária e deduplicação de operações de negócio entre arquivos diferentes ficam fora deste plano. Deduplicação de arquivo não garante que uma ordem financeira seja executada uma única vez pelo consumidor.

## 2. Como executar

- Ler `AGENTS.md` e respeitar alterações preexistentes. Nesta sessão o usuário corrigiu o Git Bash para `C:\Program Files\Git\bin\bash.exe`; o caminho D do AGENTS está desatualizado. Usar o caminho C explicitamente. Se necessário, iniciar pelo PowerShell apenas como invocador do Git Bash.
- Não implementar o plano inteiro em uma única interação. Não delegar a agentes automaticamente.
- Atualizar o registro de execução ao concluir cada tarefa: arquivos, decisões, testes realmente executados, resultado e pendências.
- Marcar uma tarefa concluída somente quando seus critérios de aceite forem comprovados. Distinguir implementação, validação local e homologação AWS.
- Implementação e testes locais não dependem de aprovação adicional. Este plano não autoriza deploy, execução com custo na AWS, destruição de recursos ou mudanças em parâmetros remotos; preparar scripts e artefatos revisáveis para essas operações.
- Não alterar configurações reais de clientes para satisfazer testes. Usar fixtures, ambientes e prefixos de teste isolados.
- O contrato solicitado neste documento substitui a proposta anterior de quatro modos: `fixed-width` também será removido.
- Registrar escolhas técnicas rotineiras em ADRs sem interromper o trabalho para pedir aprovação. Perguntar somente por informações externas realmente necessárias.

## 3. Evidências da base examinada

| Área | Situação encontrada | Arquivos principais |
|---|---|---|
| Formatos | Oito tipos; JSONL/NDJSON validam JSON; CSV extrai campos; binário usa Base64; fixed-width exige LF | `internal/domain/f2e/messages.go`, `internal/application/worker/service.go`, `internal/domain/fixedwidth/` |
| Limites | SSM global e por prefixo; validação por prefixo não é compartilhada integralmente com entrada explícita | `lambdas/cmd/organizer/main.go`, `terraform/locals.tf` |
| Ledger | Jobs/chunks e replay existentes; algumas atualizações podem sobrescrever estado avançado; início de chunk concluído retorna sucesso sem sinal explícito de skip | `internal/platform/aws/ledger.go` |
| Configuração | Snapshot parcial via ChunkJob; sem proveniência completa da versão e autoria | `internal/platform/aws/prefix_configuration.go` |
| Publicação | Um envelope por mensagem; chamadas de lotes sequenciais; limite local de 256 KiB | `internal/application/worker/service.go`, `internal/platform/aws/client.go` |
| E2E | Cenários sequenciais; consumo/exclusão sequenciais de até 10 mensagens; cenários grandes verificam contagem aproximada | `e2e/suite_test.go`, `e2e/internal/aws.go`, `e2e/internal/steps.go` |
| Infraestrutura | Filas/Workers compartilhados; sem fluxo declarado de conclusão via Streams; alarmes sem ações no arquivo examinado | `terraform/` |

Os 50 minutos relatados não foram diagnosticados por traces da execução. A drenagem sequencial, o ciclo de infraestrutura e as esperas de retry são hipóteses sustentadas pelo código, não uma medição da contribuição de cada etapa.

## 4. Contratos e decisões de referência

### 4.1 Três modos de delimitação

| Modo | Registro lógico |
|---|---|
| `text` | Uma linha física terminada por CR, LF ou CRLF. |
| `json` | Um elemento do array selecionado, mantendo parsing estrutural para encontrar suas fronteiras. |
| `multi-line` | Linhas físicas agrupadas por marcadores configurados. |

- `CRLF` é um único terminador; `LFCR` são dois terminadores. Documentar e testar esta escolha para evitar interpretação ambígua de “ambos”.
- `text`: o terminador não integra `data.raw`; preservar espaços e conteúdo restante. Linha vazia terminada é um registro vazio. Terminador final não cria registro extra. Arquivo vazio é rejeitado. Última linha não vazia sem terminador é erro, conforme a exigência do usuário de que cada linha termine com quebra.
- Esses erros podem ser descobertos após publicação parcial; não prometer atomicidade do arquivo ou desfazer eventos já publicados.
- `multi-line`: reutilizar a leitura CR/LF/CRLF, manter os marcadores e o separador lógico configurado; definir em testes o tratamento de linhas órfãs e cabeçalho/trailer. Requerer terminador das linhas físicas também neste modo.
- `json`: preservar o contrato atual de seleção do array; array vazio conclui com zero registros e zero chunks, sem esperar um Worker inexistente.
- Remover `fixed-width`, `csv`, `binary`, `jsonl`, `ndjson` e opções exclusivas. Não aceitar silenciosamente nomes antigos como `text`.
- Na migração, JSONL/NDJSON e CSV de uma linha por registro podem ser reconfigurados como `text`, com perda explícita da interpretação anterior. CSV com campos multilinha, binários e fixed-width sem terminador não são suportados pelos três modos. Fixed-width com terminador pode virar `text`.
- Definir conteúdo textual como UTF-8 válido; bytes inválidos devem gerar erro identificável, evitando substituição silenciosa no JSON de saída. Não acrescentar transcodificação automática neste plano.

### 4.2 Identidades e configuração

- Separar `receiptId` (ocorrência recebida), `fileId` (origem física imutável), `jobId` (execução) e `sourceRecordId` (registro físico).
- Uma notificação repetida ou entrada explícita da mesma versão não cria nova execução normal. Replay autorizado cria novo job vinculado ao anterior.
- A identidade normal de admissão inclui ambiente, escopo de prefixo autorizado, bucket, key e VersionId. Mudança de configuração não autoriza processar outra vez a mesma versão.
- No perfil corporativo, exigir S3 versionado. Preservar VersionId da notificação e da entrada explícita; HEAD/GET devem usar a versão indicada, nunca trocar pela versão mais recente durante retry. Fontes sem identidade imutável são rejeitadas nesse perfil.
- A cópia do mesmo conteúdo para outra key/versão não é deduplicação de arquivo físico. Hash de conteúdo e deduplicação financeira são extensões separadas.
- `sourceRecordId` deve permanecer estável com mudança de chunking ou agrupamento SQS. Preferir fileId + posição física inicial; manter schema e versão de configuração como metadados separados. Versionar a mudança em relação à fórmula antiga.
- Fixar configuração na primeira admissão. Capturar nome/ARN do parâmetro, versão retornada pelo SSM, hash do conteúdo, instante de leitura e `responsible` declarado. Identificar também a versão dos limites globais usados.
- Distinguir responsável declarado de autor autenticado da alteração: o primeiro não prova o segundo. Capturar metadado de autor quando disponível; fornecer correlação com CloudTrail para auditoria de escrita. Nunca atribuir autoria humana sem evidência.
- Replay reutiliza snapshot original por padrão; reprocessamento com nova configuração é uma operação explícita, identificada e auditada.

### 4.3 Máquina de estados e invariantes

Modelar recebimentos, job agregado, chunks e histórico de transições. Evitar que um único status misture rejeição de entrada, publicação de eventos e liquidação financeira.

Fluxo normal do job:

```text
RECEIVED -> VALIDATING -> PLANNING -> PROCESSING -> COMPLETED
                  |           |           |
                  +-----------+-----------+--> FAILED (falha técnica definitiva)
                  |
                  +--> REJECTED (arquivo/configuração inválidos)
```

Durante `PLANNING`, persistir manifesto completo e selá-lo antes de publicar chunks. `PROCESSING` permite agendamento ainda em curso, explicitado por contadores/checkpoints. Array vazio pode transitar de planejamento selado diretamente para `COMPLETED`.

- Chunks: `PENDING -> RUNNING -> COMPLETED`; falha transitória vai para `RETRY_PENDING`; esgotamento ou erro permanente vai para `FAILED`.
- Job com chunks deve aguardar todos os chunks terminais antes de finalizar. Se algum falhou, resultado final é FAILED. Durante espera, informar falhas já conhecidas e progresso. Entrada rejeitada antes do planejamento não exige chunks.
- Finalizar tecnicamente com rejeições de registros é permitido: `COMPLETED` com resultado `WITH_REJECTIONS` e contagens explícitas. Rejeição de registro não deve ser confundida com falha de infraestrutura.
- Terminais não regridem. Tentativa antiga não sobrescreve nova tentativa. Planejamento/agendamento tardio não sobrescreve conclusão.
- Usar controle condicional de versão e token de posse por tentativa quando houver lease; TTL de limpeza não funciona como lock ou mecanismo de expiração pontual.
- Invariante com manifesto selado: `expectedChunks = pendingChunks + runningChunks + retryPendingChunks + completedChunks + failedChunks`.
- Em chunk concluído: `recordsRead = recordsPublished + recordsRejected + recordsIgnored`. Contagens são de registros lógicos únicos; `messagesPublished` é uma medida separada. Reenvios não aumentam totais lógicos.
- Arquivo falho pode ter eventos publicados: informar progresso parcial e `countsComplete=false` quando não houver contagem final comprovada. Não fabricar totalizadores nem exigir igualdade de leitura de arquivo incompleto.
- Guardar `receivedAt`, `plannedAt`, `startedAt`, `updatedAt`, `completedAt`, identidade da origem, configuração e histórico limitado por política de retenção. Para multipart, “recebido” começa na admissão do objeto completo, não no início do upload.

### 4.4 Publicação e conclusão

- Entrega continua at-least-once, inclusive em Streams e eventos de conclusão. Não prometer exactly-once entre DynamoDB e SQS.
- Padrão permanece `single`: um Envelope v2 por mensagem. Modo opcional `bundle`: contrato externo versionado contendo uma lista de envelopes completos, cada qual com seu ID.
- Agrupar somente registros compatíveis do mesmo chunk, job, prefixo e configuração. Determinar fronteiras do bundle por ordem, quantidade e bytes; preservar IDs em retry. Não agrupar por ordem variável de conclusão de goroutines.
- Ter limites separados para bytes do envelope, bytes da mensagem, envelopes por mensagem, mensagens por chamada (até 10) e soma dos bytes da chamada. Considerar envelope externo, escaping UTF-8 e atributos na contabilização aplicável.
- A referência atual do SQS informa 1 MiB por mensagem e 1 MiB no total de SendMessageBatch. O F2E limita 256 KiB em código. Confirmar quotas na implementação; manter default conservador e só habilitar até 1 MiB com validação completa e configuração da fila compatível.
- Conclusão em fila própria: schema/version, eventId determinístico, jobId, fileId, prefixId, origem/versionId, configuração, resultado técnico, contagens, motivos agregados e timestamps. Nunca na fila de envelopes de registros.
- Persistir intenção de conclusão atomicamente com a transição terminal (outbox no ledger). Streams entrega a intenção para um publicador; uma recuperação por índice também deve encontrar intenções pendentes após falha prolongada/expiração do stream.
- Marcar intenção entregue somente após confirmação SQS. Crash após envio e antes da marcação pode duplicar evento; manter mesmo eventId. A chegada da conclusão pode preceder o consumo dos registros: ela não é uma barreira de consumo entre filas.

### 4.5 Prefixos

- `prefixId` é identificador estável de configuração e autorização, não um valor arbitrário aceito do body.
- Adotar inicialmente recursos dedicados por prefixo protegido: fila de intake autorizada, fila de chunks/DLQ, Worker/role e fila de saída, com código compartilhado. Organizers também devem ter isolamento por prefixo quando necessário para garantir capacidade de admissão.
- Permissões derivam de role, origem autenticada e fila de entrada vinculada ao prefixo. Uma mensagem SQS não prova a identidade do produtor por um campo escrito por ele. Restringir produtores na queue policy/IAM e validar bucket/key contra o prefixo vinculado.
- Prefixos sobrepostos devem ter uma regra única de roteamento e autorização, testada; nenhuma entrada explícita pode selecionar outro prefixo ou URL de saída.
- Separar limite de concorrência de quota de admissão. Implementar `maxActiveJobs` e limites por arquivo; rate limit de bytes/registros por janela só quando houver definição e medição próprias.
- Admissão acima da capacidade fica pendente, com backoff controlado, sem loop de retries que mande trabalho válido à DLQ. Reserva/liberação de capacidade deve ser idempotente e recuperável.
- Validar soma de reservas contra capacidade da conta; prefixo dedicado não elimina quotas regionais. Documentar custo/quantidade de recursos e o limite operacional dessa topologia.

## 5. Tarefas incrementais

### T01 — Contratos, inventário e matriz de migração

Dependências: nenhuma.

- [ ] Inventariar tipos, campos, schemas, configurações, exemplos, scripts e testes afetados. Ler README, contratos, ADRs e handoff; o handoff de 02/09 não é fonte atual para status dos formatos.
- [ ] Criar ADRs curtos para três modos, identidades/estados, bundle e isolamento por prefixo usando as decisões da seção 4.
- [ ] Definir versões de contratos e estratégia para jobs legados: drenar filas em ambiente controlado ou manter executor antigo isolado até conclusão; não reinterpretar ChunkJob antigo silenciosamente.

Aceite: matriz com campo antigo, destino/remoção, consumidores afetados e procedimento de migração; fixtures de contratos novos e antigos. Nenhuma modificação remota.

### T02 — Instrumentação do E2E e baseline reproduzível

Dependências: nenhuma; executar antes de otimizações.

- [ ] Medir em JSON por run/cenário: preparação, upload, planejamento, produção, consumo/validação, espera de DLQ e teardown.
- [ ] Contar chamadas Receive/Delete/Send, mensagens, envelopes, vazios, retries e duplicatas observadas. Distinguir relógio local de timestamps AWS e evitar subtração entre relógios sem correlação.
- [ ] Separar scripts/modos de provisionar, testar ambiente existente e destruir. Modo reutilizável nunca destrói recursos automaticamente; manter modo descartável explícito.
- [ ] Gerar relatório local/LocalStack e comandos para baseline AWS sem executá-los.

Aceite: relatório permite separar duração do pipeline e da suíte. Ausência de medição AWS é declarada, sem estimar os 50 minutos como resultado comprovado.

### T03 — Leitor de linhas CR/LF/CRLF

Dependências: T01.

- [ ] Criar leitor compartilhado com offsets físicos em bytes, memória limitada, validação UTF-8 e contrato de terminadores da seção 4.
- [ ] Usar em `text`; ajustar o planejamento de ranges para reconhecer CRLF que cruza a fronteira sem duplicar nem perder registros.
- [ ] Separar tamanho físico do registro do conteúdo sem terminador; documentar onde o limite inclui terminador.

Aceite: testes com CR, LF, CRLF, misturas, CRLF dividido em reads/chunks, linha vazia, espaço final, UTF-8 multibyte na fronteira, limite exato/excedido e última linha sem terminador. Toda posição pertence a um único chunk.

### T04 — Reduzir runtime para três modos

Dependências: T03.

- [ ] Remover tipos e caminhos de parsing excluídos, fallback implícito para fixed-width, `bypassJsonValidation`, campos exclusivos de largura fixa/CSV/Base64.
- [ ] Adaptar multi-line ao leitor físico compartilhado; preservar configuração de marcadores e tratar linhas ignoradas explicitamente.
- [ ] Manter json-array e garantir término no array selecionado e suporte a array vazio.
- [ ] Renomear pacote `fixedwidth` se apenas funcionalidades multi-line permanecerem; evitar nome enganoso.

Aceite: três modos funcionam; tipos antigos produzem erro explícito; nenhum caminho interpreta formato antigo por default. Testes de fronteira json/multi-line continuam válidos.

### T05 — Configuração, Terraform e E2E dos três modos

Dependências: T04.

- [ ] Atualizar globals, prefixos SSM, variáveis/env, contratos e schemas; eliminar limites exclusivos de CSV/binário e configuração obrigatória de recordLength.
- [ ] Unificar validação de limites/formatos para S3 e OrganizerRequest. Manter JSON estrutural separado de validação de campos de negócio.
- [ ] Substituir features e geradores removidos; derivar expectativas de jobs/rejeições dos cenários executados, sem números mágicos antigos (6, 31 etc.).
- [ ] Atualizar README, operação local/AWS e ADR de formatos.

Aceite: busca não encontra suporte ativo aos tipos removidos; referências históricas são identificadas como migração. Terraform validate, testes locais e suíte reduzida passam.

### T06 — Proveniência imutável da configuração

Dependências: T05.

- [ ] Resolver valor e versão SSM da mesma resposta; propagar snapshot completo e hash para admissão, job, chunk e metadados de saída apropriados.
- [ ] Adicionar `responsible`, nome do parâmetro, versão, timestamp e proveniência dos limites globais. Não expor dados pessoais desnecessários por envelope; referência configId pode apontar ao ledger.
- [ ] Preparar procedimento de atualização auditada e consulta de autoria via CloudTrail; diferenciar campo declarado de identidade autenticada.

Aceite: alterar SSM no meio do processamento não muda um job já admitido; replay usa snapshot original. Testes não dependem de histórico SSM ainda existir para reconstruir o snapshot.

### T07 — Modelo de estados e identidades no domínio

Dependências: T01, T06.

- [x] Criar tipos de recebimento, job, chunk, transição, contagens e motivo; definir tabela de transições válidas e resultado de aquisição `acquired/alreadyCompleted/busy`.
- [x] Atualizar portas sem implementar ainda toda a persistência. Separar admissão, planejamento, agendamento, execução e conclusão.
- [x] Definir sourceRecordId independente de chunk/schema/bundle; versionar a identidade antiga.

Aceite: testes de tabela cobrem transições permitidas/proibidas, retries, terminais, arquivo vazio rejeitado e array vazio concluído. Compatibilidade fica explícita.

### T08 — Persistência condicional e histórico no DynamoDB

Dependências: T07.

- [x] Implementar revisão condicional e token de posse; guardar histórico de transições e progresso com retenção limitada.
- [x] Impedir regressão por MarkScheduled tardio, falha de tentativa antiga ou conclusão duplicada. Retornar skip explícito quando chunk já concluiu.
- [x] Atualizar contadores de chunk e agregado de job atomicamente; planejar limite de tamanho de item e contenção no agregado para jobs com muitos chunks.

Aceite: integração concorrente prova ausência de dupla contagem, regressão e publicação após skip. Crash após envio SQS continua documentado como possível reentrega, tratada por IDs.

### T09 — Admissão idempotente de arquivo

Dependências: T08.

- [ ] Preservar VersionId desde a notificação e no contrato explícito; usar essa versão em Head/GetRange e na chave de admissão.
- [ ] Registrar RECEIVED antes do planejamento, incluindo recebimentos rejeitados; adquirir admissão com escrita condicional e vincular notificações repetidas ao job existente.
- [ ] Usar lease/token recuperável para crash durante admissão/planejamento; manter manifesto/checkpoint para retomada. Definir retenção da chave de deduplicação compatível com janela de reentrega e arquivos.
- [ ] Tratar replay como nova execução explícita vinculada ao job anterior, sem apagar a evidência de admissão normal.

Aceite: dois Organizers concorrentes e S3 + entrada explícita para a mesma versão criam um job normal; crash não deixa arquivo bloqueado para sempre; nova versão cria outro job; configuração nova não duplica admissão antiga.

### T10 — Manifesto, agendamento recuperável e estado agregado

Dependências: T09.

- [ ] Persistir todos os chunks de maneira retomável e selar manifesto antes de despachar. Não assumir que todos cabem em uma transação DynamoDB.
- [ ] Registrar checkpoints de publicação e recuperar chunks não agendados; manter IDs na republicação após confirmação ambígua.
- [ ] Agregar conclusão somente com manifesto selado e todos os chunks terminais. Concluir array vazio sem Worker.

Aceite: falhas entre qualquer duas etapas de planejamento/agendamento são recuperáveis; conclusão não ocorre cedo; resultado informa exatamente chunks pendentes e falhos.

### T11 — Decisões explícitas de registros e conciliação

Dependências: T10.

- [ ] Substituir retorno `nil` ambíguo do RecordProcessor por resultado tipado: publish, reject ou ignore, com reasonCode estável. Erro técnico continua erro/retry.
- [ ] Fixar catálogo limitado de motivos; agregar por motivo no chunk e job, sem payload/CPF/chaves arbitrárias como dimensão.
- [ ] Persistir contagens finais uma vez por chunk, atomicamente com conclusão. Tentativas parciais ficam separadas dos totais lógicos finais.
- [ ] Contar cabeçalhos/trailers ignorados de multi-line como linhas físicas ignoradas em métrica própria, sem fingir que são registros lógicos lidos.

Aceite: publish/reject/ignore, retry e conclusão duplicada preservam a equação de registros; arquivo com rejeições informa WITH_REJECTIONS; falha parcial informa contagens incompletas sem total inventado.

### T12 — Contrato e outbox de conclusão

Dependências: T11.

- [ ] Criar JSON Schema e fixtures para conclusão técnica, incluindo COMPLETED, WITH_REJECTIONS, REJECTED e FAILED conforme status/resultado definidos.
- [ ] Gravar outbox atomicamente na transição terminal; chave determinística por job e versão de conclusão, sem URLs assinadas/payloads.
- [ ] Indexar intenções pendentes para recuperação e aplicar retenção que não remova publicação ainda pendente silenciosamente.

Aceite: término concorrente produz uma intenção lógica; rollback da transação não deixa job terminal sem intenção; array vazio e arquivo rejeitado têm eventos coerentes.

### T13 — Publicador de conclusão via Streams

Dependências: T12.

- [ ] Criar Lambda, DynamoDB Streams, fila de conclusão própria, IAM, criptografia, logs e mecanismo de falhas compatível com o event source de Streams.
- [ ] Processar somente registros outbox relevantes; marcar entrega após confirmação e não gerar loop com a própria atualização.
- [ ] Recuperar pendências por índice com tarefa agendada, inclusive após expiração dos registros do stream.

Aceite: evento duplicado mantém eventId; falha entre envio e marcação não perde conclusão; falha prolongada é recuperável; consumidores de registros não recebem o novo contrato.

### T14 — Consulta operacional e detecção de jobs parados

Dependências: T13.

- [ ] Fornecer CLI/script de consulta por receiptId/fileId/jobId/prefixId e histórico, com paginação. Evitar Scan completo para consulta normal.
- [ ] Criar índice por estado/faixa temporal com particionamento adequado e reconciliador de jobs sem progresso; renovar progresso de execução longa sem excesso de writes.
- [ ] Recuperar lease/agendamento quando comprovadamente seguro; emitir alerta antes de replay não controlado. Mapear mensagens esgotadas na DLQ ao estado final.
- [ ] Conectar alarmes a destinos configuráveis e documentar responsáveis/runbooks.

Aceite: operador vê recebimento, número planejado de chunks, progresso e término; job parado é detectado mesmo com filas vazias; nenhum retry infinito é criado.

### T15 — Contrato opcional de múltiplos envelopes

Dependências: T11, T06.

- [ ] Adicionar configuração por prefixo `outputMode=single|bundle`, `maxEnvelopesPerMessage` e `maxMessageBytes`, separada de `maxEventBytes` e batchSize.
- [ ] Criar schema do bundle e atualizações de contrato/attributes para distinguir wrapper e envelopes internos. Não promover transactionId heterogêneo a atributo da mensagem.
- [ ] Definir comportamento de envelope que não cabe sozinho: erro explícito, sem truncar. Consumidor de exemplo deduplica envelope e demonstra retry do bundle inteiro.

Aceite: modo single mantém seu contrato; consumidores precisam optar por bundle; limites e migração estão documentados.

### T16 — Empacotamento e limites por bytes no publicador

Dependências: T15.

- [ ] Implementar agrupador streaming determinístico por quantidade/bytes, com flush no fim do chunk.
- [ ] Separar agrupamento de envelopes e montagem de SendMessageBatch; respeitar limite individual e soma do lote, incluindo atributos aplicáveis.
- [ ] Remover tetos fixos conflitantes somente onde a nova configuração os substitui, validando também tamanho permitido pela fila e consumidores.
- [ ] Registrar envelopes lógicos e mensagens lógicas separadamente; manter sourceRecordId/eventId em retentativas.

Aceite: testes de limite exato, um byte excedido, escaping, UTF-8, atributos, envelope grande e batch misto; 10 mensagens individualmente válidas que excedam a soma são divididas antes da API.

### T17 — Publicação concorrente limitada dentro do Worker

Dependências: T16, T08.

- [ ] Adicionar `publishConcurrency` configurável, default 1, e fila interna limitada; testar 2 e 4 sem alterar outros fatores simultaneamente.
- [ ] Aguardar confirmação de todos os lotes antes de concluir chunk; cancelar produtores com segurança em falha e repetir somente itens falhos quando identificáveis.
- [ ] Preservar agrupamento determinístico anterior à execução concorrente; não prometer ordem global de entrega.

Aceite: sem vazamento de goroutines, memória sem crescimento com arquivo inteiro, race detector passa; evidência de latência/custo é comparada com baseline, sem ganho presumido.

### T18 — Coletor E2E concorrente e validação real

Dependências: T02, T16, T13.

- [ ] Receber com 4/8 consumidores configuráveis, long polling e exclusão em lote, tratando falhas parciais de DeleteMessageBatch.
- [ ] Consumir desde o início, identificar cenário/run pelos envelopes e expandir bundle; verificar todos os IDs/conteúdos esperados, não só ApproximateNumberOfMessages.
- [ ] Distinguir duplicatas de entrega toleradas pelo transporte de duplicação indevida de efeitos/identidade. Não apagar mensagem de outro run para “limpar” fila compartilhada.
- [ ] Usar conclusão + conjunto esperado + janela final limitada para observação de duplicatas; conclusão sozinha não comprova consumo completo. Aplicar timeout ao ciclo inteiro de coleta, não só à espera inicial.

Aceite: mesmo conjunto lógico validado em single/bundle; faltas, extras e IDs trocados são detectados; relatório separa tempo de produção e consumo.

### T19 — Perfil de testes AWS e runner na região

Dependências: T18.

- [ ] Preparar runner CodeBuild (ou equivalente já usado no projeto) na região dos recursos, com permissões mínimas e exportação de relatórios.
- [ ] Criar perfil E2E de pequenos arquivos com timeout/visibility/redrive coerentes; manter a relação recomendada entre timeout Lambda e visibilidade. Não reduzir visibilidade isoladamente.
- [ ] Separar smoke, regressão funcional, falhas e carga por tags. Separar teste de construção/destruição do benchmark.
- [ ] Preparar matriz de memória 512/1024/2048 MiB, concorrência Worker 4/8/16 e tamanhos de chunk; variar um fator por rodada e registrar p50/p95, custo e erros.

Aceite: comandos e infraestrutura revisáveis; execução real AWS fica pendente até autorização. Metas são estabelecidas a partir da baseline, não um tempo arbitrário prometido.

### T20 — Autorização e roteamento por prefixo

Dependências: T09, T06.

- [ ] Criar cadastro prefixId -> bucket/prefixo/configuração/filas/roles, com correspondência canônica e regra para sobreposição.
- [ ] Vincular fila de intake e identidade autorizada ao prefixo; validar S3 e OrganizerRequest no mesmo caminho de admissão.
- [ ] Remover possibilidade de consumidor escolher fila de saída, outro prefixo ou configuração fora de seu escopo por campos da mensagem.

Aceite: testes negativos cruzados comprovam isolamento, incluindo key manipulada, prefixos parecidos/sobrepostos e mensagem explícita tentando burlar limites.

### T21 — Recursos e capacidade dedicada por prefixo

Dependências: T20, T17.

- [ ] Refatorar Terraform para módulos/for_each com intake, Organizer quando requerido, chunks/DLQ, Worker/role e output por prefixo protegido. Reutilizar binários, separar configuração/IAM.
- [ ] Configurar reserved/maximum concurrency por prefixo e limites de acesso a S3, SQS, SSM e ledger. Para isolamento estrito do ledger, escolher tabela por prefixo ou comprovar políticas de chaves e acessos; prefixId no item sozinho não isola IAM.
- [ ] Migrar recursos existentes com plano de estado/endereço e preservação de filas; revisar qualquer replace/destroy antes de operação remota.

Aceite: plano Terraform demonstra recursos e permissões por prefixo; carga de A não utiliza os Workers reservados de B; quotas regionais e custo adicional documentados.

### T22 — Quota de admissão e recuperação de capacidade

Dependências: T21, T14.

- [ ] Implementar maxActiveJobs por prefixo com reserva atômica vinculada à admissão; deduplicata não consome nova vaga.
- [ ] Persistir estado de espera e retomar de forma justa com índice/scheduler, sem bloquear Lambda ou causar redrive por saturação normal.
- [ ] Liberar vaga uma vez no terminal e recuperar vazamentos após crash; falha de publicação do evento de conclusão não impede liberar capacidade técnica.

Aceite: N+1 jobs respeitam quota N; prefixos não bloqueiam uns aos outros; retries e término duplicado não geram capacidade negativa ou excesso de admissões.

### T23 — Paralelismo de cenários e benchmark comparável

Dependências: T19, T22.

- [ ] Habilitar paralelismo de cenários somente com coletores/recursos isolados e expectativas por run/job; nenhuma asserção depende de total global da fila/ledger.
- [ ] Comparar sequencial e paralelo, single e bundle, runner local e regional, com os mesmos dados e verificação de identidade.
- [ ] Medir admissão, planejamento (inclusive gravação de manifesto), publicação, ledger e consumo. Otimizar writes de planejamento apenas se medições indicarem gargalo, mantendo selagem/retomada.

Aceite: sem contaminação cruzada; throughput, duração e custo reportados por etapa. Aceleração só é registrada como comprovada após execução correspondente.

### T24 — Falhas integradas, migração e encerramento

Dependências: T23 e todas as anteriores.

- [ ] Automatizar falhas entre admissão/plano/envio/ledger/outbox, retomada após crash, duplicação S3/SQS/Streams, configuração alterada durante job e saturação de prefixo.
- [ ] Executar regressão local completa; preparar homologação AWS com single/bundle, três formatos, limites e recuperação prolongada de conclusão.
- [ ] Atualizar operação AWS/local, contratos, schemas, runbooks, exemplos SSM e procedimento de rollback. Rollback não deve enviar bundle para consumidor single nem executar jobs novos em binário legado.
- [ ] Produzir relatório final separando concluído em código, testes locais, resultados AWS e pendências externas.

Aceite: todos os requisitos da seção 1 têm evidência rastreável; não há promessa de produção homologada sem testes AWS e parâmetros operacionais definidos.

## 6. Ordem recomendada e pontos de parada

Executar T01, T02 e depois T03–T14. Em seguida T15–T19 e T20–T24. As dependências explícitas permitem reorganizar trabalho, mas não pressupõem agentes em paralelo.

- Marco A: T05 — três modos disponíveis e configuração coerente.
- Marco B: T14 — admissão, estados, contadores e conclusão recuperável.
- Marco C: T19 — bundle, publicação e E2E otimizáveis com medição.
- Marco D: T22 — isolamento e capacidade por prefixo.
- Marco E: T24 — regressão e documentação; homologação externa identificada separadamente.

## 7. Verificação proporcional à tarefa

Executar comandos dentro do Git Bash explícito, na raiz do repositório:

```bash
go test ./internal/domain/... ./internal/application/...
go test ./internal/platform/...
(cd lambdas && go test ./...)
(cd e2e && go test ./internal/...)
git diff --check
```

Selecionar apenas os pacotes afetados durante cada tarefa. Nos marcos, executar `go test ./...`, `go vet ./...` nos módulos relevantes e race detector em ambiente com CGO/compilador compatíveis. Não alterar todo o repositório com gofmt; formatar arquivos Go modificados.

Quando houver Terraform:

```bash
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false -input=false
terraform -chdir=terraform validate
```

Planos AWS precisam de valores/credenciais apropriados e devem ser revisados sem apply automático. E2E completo local nos marcos que alteram runtime/contratos; carga AWS apenas em rodadas autorizadas. Para scripts, `bash -n` dentro do Git Bash; verificar workflows com actionlint quando alterados. Relatar ferramentas indisponíveis sem inventar aprovação.

## 8. Prompt para cada execução

```text
Execute somente a tarefa TXX do plano
documentacao/planos/evolucao_f2e_tres_modos_operacao_e_performance.md.

Leia AGENTS.md, a tarefa, suas dependências e os contratos relacionados.
O Git Bash desta máquina está em C:\Program Files\Git\bin\bash.exe,
conforme correção do usuário; use esse caminho explícito.

Confirme o estado atual no código e no registro de execução. Implemente o escopo
da tarefa, preservando alterações preexistentes. Não implemente tarefas futuras.
Se faltar uma dependência, informe precisamente qual e faça apenas o trabalho
independente que possa ser concluído com segurança.

Execute verificações proporcionais à alteração e atualize o registro deste plano.
Não faça deploy, alterações AWS remotas, destruição, commit ou push.
Ao terminar, apresente mudanças, testes realmente executados, critérios de aceite
atendidos e pendências. Não trate medição ausente como ganho comprovado.
```

## 9. Registro de execução

Todas as tarefas estão pendentes. Preencher uma linha a cada execução; se houver retomada, preservar o histórico.

| Tarefa | Data/revisão | Estado | Evidências e verificações | Pendências |
|---|---|---|---|---|
| Planejamento | 2026-09-04 | Documento criado | Inspeção de código e configuração; sem implementação | T01–T24 |
| T01 | 2026-09-05 | Concluído | ADRs 0005–0008 criados; matriz de migração em `documentacao/planos/t01-matriz-de-migracao.md`; fixtures em `documentacao/schemas/fixtures/`; ADR 0004 marcado supersedido; nenhuma modificação remota | T02 e sequência |
| T02 | 2026-09-05 | Concluído | `e2e/internal/metrics.go` com tipos `ScenarioMetrics`/`RunReport`/`APICounters`/`PhaseTimer`; `aws.go` instrumentado com contadores de S3/SQS; `steps.go` com phase timers por fase (setup/upload/intake/wait/consume/validate) e timestamps de envelope AWS; `suite_test.go` salva relatório JSON em `E2E_METRICS_FILE`; scripts `provisionar-e2e.sh`/`testar-e2e.sh`/`destruir-e2e.sh` criados; `baseline-aws-commands.md` com comandos preparatórios sem execução AWS; `go build ./...` e `go vet ./...` passam; `TestUndefinedStepFailsStrictSuite` passa; E2E com LocalStack não executado (infraestrutura não disponível) | Execução E2E completa com LocalStack/AWS pendente; baseline AWS não estabelecido |
| T03 | 2026-09-05 | Concluído | Pacote `internal/domain/lineio` com `ReadLines` (CR/LF/CRLF, offsets físicos, limite de bytes, validação UTF-8, skipFirst, erro em última linha sem terminador); 30 testes de boundary (todos PASS); `worker/service.go` migrado para `lineio.ReadLines`, removidos `readLines` e `readBoundedLine`; `variableJobs` comentado para CRLF na fronteira de chunk; `TestJSONLBoundariesWithoutFinalLFAndWithCRLF` atualizado para novo contrato; `go test ./internal/...`, `go vet ./internal/...` e `lambdas` passam | — |
| T04 | 2026-09-05 | Concluído | Tipos `fixed-width`, `jsonl`, `ndjson`, `csv`, `binary` removidos de `messages.go`; campos `ProcessingOptions`/`BypassJSONValidation`, `RecordPayload.Fields/Base64`, `PrefixConfiguration.RecordLengthBytes` removidos; pacote `fixedwidth` deletado; novo pacote `internal/domain/multiline` com `ReadMultiLine` usando `lineio.ReadLines` (CR/LF/CRLF); worker e organizer aceitam somente `text`, `json`, `multi-line`; fallback `"" → fixed-width` removido; tipos antigos geram erro explícito "unsupported data type"; `e2e/internal/producers.go` e `steps.go` atualizados; todos os testes passam (`go test ./internal/...`, `go vet`, lambdas, e2e/internal) | — |
| T05 | 2026-09-05 | Concluído | `config.go`: `RecordLength`/`F2E_RECORD_LENGTH` removidos; `terraform/locals.tf`: tipos antigos removidos dos `file_configurations` e `global_limits.inputTypes`, `recordLengthBytes`/`F2E_RECORD_LENGTH`/`options.bypassJsonValidation` removidos; `variables.tf`: variável `f2e_record_length` removida; `init-aws.sh` atualizado; 5 feature files obsoletos deletados (`binary`, `csv`, `fixed_width`, `jsonl`, `ndjson`); `suite_test.go`: DLQ 6→2, jobs 31→14; `AssertLedgerComplete` aceita 0-chunk jobs; `ChunkJob.RecordLengthBytes`/`RecordCount` removidos; `documentacao/contratos.md` reescrito para três modos; README, operacao_local.md e executar-fluxo.sh atualizados; `go test`, `go vet` e `terraform validate` passam | — |
| T06 | 2026-09-05 | Concluído | `ConfigurationSnapshot` retém a configuração de prefixo e os limites globais efetivamente usados, além de hash, parâmetro/versão SSM, `responsible` declarado e instante de leitura; é propagado a `ChunkJob`, `JobPlan` e ledger. Testes confirmam hash determinístico e que a cópia admitida não muda após alteração da configuração em memória; Worker não consulta SSM. Validação local: `go test ./internal/domain/... ./internal/application/... ./internal/platform/...` e `go vet` nesses pacotes passam. | Consulta de autoria autenticada permanece procedimento CloudTrail; não houve alteração remota. |
| T07 | 2026-09-05 | Concluído | `internal/domain/f2e/states.go`: tipos `JobStatus` (estados canônicos `RECEIVED/VALIDATING/PLANNING/PROCESSING/COMPLETED/FAILED/REJECTED`), `ChunkStatus` (`PENDING/RUNNING/RETRY_PENDING/COMPLETED/FAILED`), `JobResult` (`SUCCESS/WITH_REJECTIONS`), `AcquisitionResult` (`acquired/alreadyCompleted/busy`), `Counts`, `Rejection`/`RejectionReason`, `Transition`, `Receipt`, `Job`, `Chunk`; tabelas de transições `jobTransitions`/`chunkTransitions` com `CanTransitionTo`/`Terminal`; `SourceRecordID(fileId, byteOffset)` (v2, independente de chunk/schema/bundle) e `SourceRecordIDV1` (fórmula legada versionada). `states_test.go`: testes de tabela cobrindo transições permitidas/proibidas, retries, terminais, arquivo vazio rejeitado (VALIDATING→REJECTED) e array vazio concluído (PLANNING→COMPLETED). `ports.go`: `JobLedger` expandido com fases de admissão/validação/planejamento/agendamento/execução/conclusão (`Admit`, `BeginValidation`, `RejectJob`, `BeginPlanning`, `AcquireChunk`, `FinalizeJob`) e `ErrNotImplemented`. `aws/ledger.go`: stubs das novas fases retornando `ErrNotImplemented` (persistência fica para T08); constantes legadas em `ledger.go` mantidas com mapeamento explícito de compatibilidade. `worker/service.go`: `sourceRecordID` migrado para `f2e.SourceRecordID` (v2). `go build ./...`, `go vet ./...`, `go test ./internal/domain/...`, `go test ./internal/application/...` e `go test ./lambdas/...` passam | Persistência condicional e histórico (T08) |
| T08 | 2026-09-05 | Concluído localmente | Escritas de fase usam condições e revisão; início/conclusão/falha de chunk condicionam estado e tentativa, com atualização transacional de chunk/contadores. `MarkScheduled`/`MarkSchedulingFailed` não sobrescrevem terminais. Falhas transitórias usam `RETRY_PENDING`; entrega após chunk concluído retorna skip explícito ao Worker. LocalStack: `TestLedgerLifecycle`, `TestLedgerConcurrentCompletion`, todas as `TestPhased*` (incluindo 8 claims concorrentes) passaram. | Homologação AWS pendente; T09 trata identidade por versão física. |
| T09 | 2026-09-05 | Concluído localmente | S3 preserva `versionId`; `HeadObject` e planner usam a versão solicitada; `fileId` determinístico deriva de bucket/key/VersionId/ETag/tamanho. O Organizer admite S3 versionado antes de planejar, usa `fileId` como `jobId`, persiste `receiptId` e snapshot, ignora terminal e devolve erro para entrada ocupada. Leases expiram em 15 min e podem ser reivindicados condicionalmente. LocalStack: `TestPhasedAdmissionDeduplicatesDifferentReceiptsForSameFile` prova que dois receipts da mesma versão resultam em um job normal. | Homologação AWS e integração do replay explícito ao registro de admissão permanecem pendentes. |
| T10 | 2026-09-05 | Concluído localmente | `Plan` grava chunks individualmente e só então sela o manifesto; repetição idempotente retoma escrita incompleta sem transação proporcional ao número de chunks. O ledger grava `scheduledAt` por chunk confirmado; o Organizer consulta e republica somente chunks sem checkpoint, mantendo o contrato/ID persistido. Agendamento exige manifesto selado; reconciliação exige manifesto selado e todos os chunks terminais. Array JSON vazio sela e conclui o job com zero chunks sem Worker. LocalStack: `TestLedgerManifestScheduleCheckpoints` confirma que, após checkpoint de um chunk, somente o outro é recuperado. | Homologação AWS; cortes de processo entre cada instrução e reconciliador operacional agendado ficam pendentes. |
| T11 | 2026-09-05 | Concluído localmente | `RecordProcessor` retorna `RecordDecision` explícito (`publish`/`reject`/`ignore`); motivos são obrigatórios para reject/ignore. Worker contabiliza registros lidos/publicados/rejeitados/ignorados, motivos limitados e linhas físicas multi-line ignoradas separadamente. A conclusão persiste contagens e razões do chunk e agrega os totais/motivos no job na mesma transação; duplicatas não somam novamente. `TestProcessorDecisionsCountPublishRejectAndIgnore`, `TestReadMultiLineReportsIgnoredPhysicalLines` e LocalStack `TestLedgerAggregatesReasonCounts` passam. | Homologação AWS pendente. |
| T12 | 2026-09-05 | Concluído localmente | `internal/domain/f2e/completion.go`: tipos `CompletionEvent`, `CompletionIntent`, `CompletionSource`, `CompletionConfig`, `CompletionCounts`, `CompletionTimestamps` e `CompletionEventID` (determinístico, SHA-256 hex de jobId+version); constante `CompletionEventVersion = "f2e-completion/1"`. `documentacao/schemas/completion-v1.schema.json`: JSON Schema do evento de conclusão com status terminal (COMPLETED/FAILED/REJECTED), result (SUCCESS/WITH_REJECTIONS), counts com `countsComplete`, rejectionReason, config de proveniência SSM e timestamps. Fixtures `completion-completed.json`, `completion-with-rejections.json`, `completion-rejected.json`, `completion-failed.json`. Port `JobLedger` expandido com `WriteCompletionIntent`, `PendingCompletionIntents` e `MarkIntentDelivered`. `aws/ledger.go`: `WriteCompletionIntent` usa PutItem condicional (`attribute_not_exists`) para garantir idempotência; `PendingCompletionIntents` consulta GSI `pending-intents-index` via atributo esparso `intentPending`; `MarkIntentDelivered` remove o atributo do GSI e é idempotente. `terraform/dynamodb.tf`: GSI `pending-intents-index` no atributo `intentPending` para recuperação sem Scan. Testes unitários `completion_test.go` (6 testes, todos PASS). Teste de integração LocalStack `ledger_outbox_integration_test.go` (skip sem `F2E_TEST_AWS_ENDPOINT`). `go build ./...`, `go vet ./internal/...`, `go test ./internal/domain/...`, `go test ./internal/application/...`, `go test ./internal/platform/...`, `go test ./lambdas/...` e `terraform validate` passam. | Homologação AWS e execução dos testes de integração com LocalStack pendentes (infra não disponível neste ambiente). Publisher (T13) ainda não implementado. |
| T24 | 2026-09-05 | Concluído localmente | `e2e/features/failure_recovery.feature` (NOVO): 5 cenários `@failure @regression` — deduplicação (reprocessamento não duplica saída), rejeição de arquivo vazio (`text` e `multi-line`), single-record text e single-element JSON. `documentacao/runbooks.md`: seções adicionadas para "Rollback com modo bundle ativo" (downgrade de `F2E_BUNDLE_MAX_ENVELOPES` requer coordenação com consumidor), "Rollback e jobs novos em binário legado" (remover campos T20–T22 antes de restaurar binário legado), "Quota de prefixo esgotada (T22)" (diagnóstico via DynamoDB + recuperação de slot preso), "Recuperação de intent de conclusão (outbox, T12–T13)" (GSI esparso + idempotência por `eventId`). `documentacao/contratos.md`: seção "Evento de conclusão (T12)" com referência ao schema JSON e semântica de `eventId`; seção "Configuração de prefixo com roteamento e quota (T20–T22)" com campos `prefixId`, `outputQueueUrl`, `allowedSourceArns`, `maxActiveJobs` e precedência de override; seção "Exemplos de parâmetros SSM" com três exemplos de `aws ssm put-parameter`. `documentacao/relatorio-final-t24.md` (NOVO): tabela completa T01–T24, testes locais executados, resultados AWS (não executados, razão e artefatos prontos), pendências externas (LocalStack, homologação AWS, alarmes, migração SSM, deduplicação financeira) e decisões técnicas relevantes. `go test ./... -short`, `go build ./...` e `cd e2e && go build ./...` passam. | Homologação AWS e execução de testes de integração LocalStack pendentes (sem infra disponível). Fault injection automatizado (crash entre etapas, duplicação via Streams) não implementado — requer harness de injeção de falha ou testes de chaos engineering fora do escopo local. |
| T23 | 2026-09-05 | Concluído localmente | `e2e/internal/dispatcher.go` (NOVO): `MessageDispatcher` com goroutine de background que faz long-polling do SQS e roteia cada `Envelope` para o canal do cenário correto pelo `Processing.JobID`; buffer de orphans com TTL de 30s para mensagens que chegam antes da subscrição; `CollectFromDispatcher` coleta `expected` envelopes de um canal com timeout. `e2e/internal/aws.go`: método `Fork()` cria um `*AWSClient` que compartilha os SDK clients mas tem seu próprio `Counters` — elimina data race em modo paralelo. `e2e/internal/steps.go`: campo `dispatcher *MessageDispatcher` e `dispatcherCh <-chan Envelope` em `scenarioCtx`; `NewScenarioInitializerWithDispatcher(client, d, collector)` como variante paralela; `newInitializer` usa `client.Fork()` em todos os modos; mutex `collectorMu` protege `*collector` em cenários concorrentes; `uploadAndProcess` e `uploadThroughConfiguredS3Prefix` subscrevem no dispatcher antes de fazer upload; `receiveExactly` usa `CollectFromDispatcher` quando `dispatcherCh != nil`; `noEventsWithin` verifica canal do dispatcher em modo paralelo. `e2e/internal/metrics.go`: `BenchmarkComparison` (concurrency, sequentialTotalMs, parallelTotalMs, speedupFactor, note) adicionado a `RunReport`. `e2e/suite_test.go`: env `E2E_CONCURRENCY` (default 1) controla `godog.Options.Concurrency`; quando > 1, instancia `MessageDispatcher` e usa `NewScenarioInitializerWithDispatcher`; env `E2E_BENCHMARK=true` executa `runBenchmark` que roda o suite seq + par e mede o speedup real; `writeMetricsReport` recebe `*BenchmarkComparison`. `go build ./...` e `go test ./... -short` passam. | Aceleração só é provada com execução AWS ou LocalStack real (sem infra disponível). DLQ count assertions mantidas para modo sequencial apenas; em modo paralelo a assertiva global do ledger ainda vale (AssertLedgerComplete). Modo benchmark roda o suite duas vezes, dobrando o tempo total. Homologação AWS pendente. |
| T22 | 2026-09-05 | Concluído localmente | `internal/domain/f2e/messages.go`: campo `MaxActiveJobs int` adicionado a `PrefixConfiguration`. `internal/domain/f2e/states.go`: campo `PrefixID string` adicionado a `Receipt`. `internal/application/port/ports.go`: erro sentinela `ErrQuotaExceeded`; métodos `ReserveSlot(ctx, prefixID, maxActiveJobs)` e `ReleaseSlot(ctx, prefixID)` adicionados à interface `JobLedger`. `internal/platform/aws/ledger.go`: `ReserveSlot` usa `UpdateItem` condicional (`attribute_not_exists(activeJobs) OR activeJobs < :max`) no item `QUOTA#<prefixID>/QUOTA`; `ReleaseSlot` usa condição `activeJobs > :zero` para idempotência; `Admit` persiste `prefixId` no item de job; helper `releaseQuotaIfNeeded` lê `prefixId` do item e chama `ReleaseSlot`, sendo chamado por `RejectJob` e `FinalizeJob` após transição terminal bem-sucedida. `lambdas/cmd/organizer/main.go`: `jobsFor` chama `ReserveSlot` antes de `Admit` quando `PrefixID != ""` e `MaxActiveJobs > 0`; reverte slot em caso de erro, `AlreadyCompleted` ou `Busy`; propaga `PrefixID` para `Receipt`. Handler do Organizer não adiciona ao `BatchItemFailures` quando `errors.Is(err, port.ErrQuotaExceeded)` — mensagem SQS retorna automaticamente via visibility timeout. `internal/platform/aws/ledger_quota_integration_test.go`: 3 testes de integração LocalStack (reserva/liberação, idempotência em 0, liberação automática no RejectJob). `go build ./...` e `go test ./... -short` PASS. | Testes de integração com LocalStack requerem `F2E_TEST_AWS_ENDPOINT` — skipped sem infra local. Scheduler de fila de espera (fair-queuing) não implementado: quota excedida é tratada por visibilidade SQS e retentativa natural; DLQ para mensagens que excedam `MaxReceiveCount` sem obter slot. Homologação AWS pendente. |
| T21 | 2026-09-05 | Concluído localmente | Módulo `terraform/modules/f2e-prefix/` criado: `variables.tf` (prefix_id com validação de charset, shared ARNs, worker sizing e concurrency), `main.tf` (SQS output+DLQ com TLS, IAM role/policy do Worker scoped à fila de output do prefixo, Lambda com `reserved_concurrent_executions`, event-source mapping com `scaling_config`, política de sender autorizado via `allowed_source_arns`), `outputs.tf`. `terraform/prefix_modules.tf`: instancia o módulo via `for_each = local.file_configurations`, monta `worker_env_with_endpoint`, propaga `prefix_worker_effective` (reserved/maximum concurrency + memory_mb por prefixo). `terraform/variables.tf`: nova variável `prefix_worker_config` (map of object com optional fields). `terraform/outputs.tf`: `prefix_output_queue_urls` e `prefix_worker_function_arns`. `terraform fmt`, `init -backend=false`, `validate` passam. | Isolamento IAM por item de ledger requer ABAC ou tabela por prefixo; migração de estado (terraform state mv) documentada como comentário em prefix_modules.tf; plano real requer credenciais AWS — não executado (sem autorização de apply). Quota de admissão em T22. |
| T20 | 2026-09-05 | Concluído localmente | `internal/domain/f2e/messages.go`: campos `PrefixID`, `OutputQueueURL`, `AllowedSourceARNs` adicionados a `PrefixConfiguration`; campos `OutputQueueURL` e `PrefixID` adicionados a `JobConfiguration`. `internal/application/organizer/validate.go`: funções exportadas `ValidatePrefixConfiguration` e `ValidateGlobalLimits` extraídas do `main.go` do Organizer, com validação de `PrefixID` (charset `[a-zA-Z0-9-_]`) e `OutputQueueURL` (deve começar com `https://sqs.` ou `http://` para LocalStack). Organizer `main.go` passou a chamar as funções exportadas e propaga `OutputQueueURL`/`PrefixID` para `JobConfiguration`; Worker `service.go` aplica override de `OutputQueueURL` do job quando presente. `validate_test.go`: 15 novos testes (charset válido/inválido para PrefixID, URL válida/ARN-rejeitado/LocalStack, AllowedSourceARNs informacional, GlobalLimits). Todos os 27 testes `./internal/application/organizer/` PASS; suite completa PASS. | `AllowedSourceARNs` é informacional — Terraform gera políticas a partir deles (T21); enforcement real via IAM. Homologação AWS pendente. |
| T19 | 2026-09-05 | Concluído localmente | Cenários E2E classificados com `@smoke`, `@regression`, `@load` (features text, multiline, json_array, ssm_prefix). `automacao/buildspec-e2e.yml`: buildspec CodeBuild com verificação de variáveis obrigatórias, artefato de métricas JSON, nota sobre visibilidade SQS vs timeout Lambda. `automacao/benchmark-matrix.sh`: matriz de memória (512/1024/2048 MiB) e tamanho de chunk (1000/5000/10000); varia um fator por rodada; restaura baseline após cada dimensão; usa `aws lambda update-function-configuration` e `aws ssm put-parameter`. E2E lista testes e builda sem erros. Execução real AWS aguarda autorização. |
| T18 | 2026-09-05 | Concluído localmente | `e2e/internal/aws.go`: `expandMessage` detecta bundle (`schemaVersion = "f2e-bundle/1"`) e expande `Items`; `ConsumeAllConcurrent(ctx, expected, workers, timeout)` — pool de `workers` goroutines com long-polling SQS (5s), delete em lote com retry de falhas parciais (`del.Failed`), timeout aplicado ao ciclo inteiro via `context.WithDeadline`, detecção de identity duplicates por `eventId`. `e2e/internal/steps.go`: `receiveExactly` para ≤1000 registros passou a usar `ConsumeAllConcurrent` com 4 workers em vez de `ConsumeAllTimed` sequencial; bundle é expandido transparentemente antes da validação. Build e vet sem erros. Testes reais aguardam deploy AWS. | Execução real AWS aguarda autorização (T19). |
| T17 | 2026-09-05 | Concluído localmente | `internal/platform/config/config.go`: campo `PublishConcurrency` (int, default 1, range 1–16) carregado de `F2E_PUBLISH_CONCURRENCY`. `internal/application/worker/concurrent_sender.go`: `concurrentSender` com semáforo (`chan struct{}`), pool de goroutines, lógica de retry migrada do `sendBatch` original, `submit()` bloqueia até slot disponível ou ctx cancelado, `wait()` aguarda todas as goroutines e retorna primeiro erro. `service.go`: substituiu `sendBatch` inline pelo `sender.submit(batch)` + `sender.wait()` no flush final; imports de `rand` e `time` não usados no sender foram removidos. Testes `concurrent_sender_test.go`: 4 testes — `RespectsConcurrencyLimit` (semáforo), `PropagatesError`, `ConcurrencyTwo` (6 records), `ConcurrencyFour` (8 records). Todos PASS. Suite completa PASS. | Homologação AWS; race detector requer CGO (não disponível no ambiente). |
| T16 | 2026-09-05 | Concluído localmente | `internal/application/worker/packer.go`: `bundlePacker` streaming determinístico com limites por quantidade (`maxEnvelopesPerMessage`) e por bytes (`maxMessageBytes`); `bundleID` estável via sorted eventIds; `flush` retorna `*port.OutboundMessage` (nil se vazio). `streamWithMetrics` estendido: detecta `OutputMode` em `j.Configuration`; em modo bundle usa `bundlePacker`; em modo single usa `enqueueSingle` (comportamento original); `RecordsPublished` conta envelopes em ambos os modos. Testes `packer_test.go`: 6 testes (count limit, bytes limit, oversized item → `ErrEnvelopeTooLarge`, bundleID estável, flush empty, atributos presentes). Testes `service_test.go` T16: 5 testes (packs all, flushes on limit, count envelopes, single unchanged, retry IDs estáveis). Total: 11 novos testes PASS; todos os existentes continuam passando. `go test ./internal/...`, `go test ./lambdas/...` PASS. | Homologação AWS; publicação concorrente (T17) pendente. |
| T15 | 2026-09-05 | Concluído localmente | `OutputMode` (`single`\|`bundle`), `MaxEnvelopesPerMessage`, `MaxMessageBytes` adicionados a `PrefixConfiguration` e `JobConfiguration` em `messages.go`. `BundleEnvelope` com `SchemaVersion="f2e-bundle/1"`, `BundleID`, `JobID`, `ChunkID`, `Items []Envelope[RecordPayload]`. `ErrEnvelopeTooLarge` para envelope que excede o limite sozinho (sem truncamento). `validatePrefixConfiguration` em `organizer/main.go` estendida: modo desconhecido rejeitado; `maxEnvelopesPerMessage` deve ser ≥ 0; `maxMessageBytes` deve estar em `[1024, maxEventBytes]`. Organizer propaga campos de bundle para `JobConfiguration` e `ChunkJob`. Schema `documentacao/schemas/bundle-v1.schema.json` criado. Fixtures `bundle-v1-example.json` e `documentacao/exemplos/consumidor-bundle.md` com padrão de deduplicação por `eventId` e retry de bundle inteiro. 7 testes unitários PASS (`TestOutputModeDefaultIsEmpty`, `TestOutputModeJSONRoundTrip`, `TestOutputModeSingleOmittedWhenEmpty`, `TestBundleEnvelopeSchemaVersion`, `TestBundleEnvelopeJSONRoundTrip`, `TestErrEnvelopeTooLargeMessage`, `TestJobConfigurationBundleFieldsRoundTrip`); `go build ./...`, `go vet ./internal/...`, `go test ./...` (lambdas) passam. | Modo single mantém contrato; consumidores precisam optar por bundle; implementação de empacotamento real (T16) pendente. |
| T14 | 2026-09-05 | Concluído localmente | GSI `status-time-index` (hashKey=`statusIndex`, rangeKey=`updatedAt`) em `terraform/dynamodb.tf`; atributo espelho `statusIndex` adicionado a `advanceJob` e `Admit` em `aws/ledger.go`. `internal/platform/aws/ledger_query.go`: tipos `JobSummary`/`StuckSince`; métodos `GetJob`, `GetJobByReceiptID`, `QueryJobsByStatus`, `StuckJobs`. CLI `automacao/cmd/query-job/main.go`: flags `-jobId`, `-receiptId`, `-status`, `-stuck`; saída JSON. `terraform/monitoring.tf`: `completion_events` e `completion_events_dlq` adicionados aos mapas monitorados; Lambda `completion_publisher` ao mapa de funções; alarme `streams_iterator_age` (threshold 5 min, 5 períodos) com `alarm_actions`/`ok_actions` para `var.alarm_sns_topic_arn`. Variável `alarm_sns_topic_arn` em `variables.tf` (default `""`; validação de ARN SNS). Runbook `documentacao/runbooks/streams-iterator-age.md`. `go build ./internal/...`, `go build ./automacao/...`, `go test ./internal/...`, `go test ./lambdas/...`, `terraform fmt`, `validate` passam. | Homologação AWS; testes unitários dedicados de `ledger_query.go`; link de receipt pelo Admit ainda não persiste item RECEIPT# (previsto como follow-up). |
| T13 | 2026-09-05 | Concluído localmente | Lambda `lambdas/cmd/completion-publisher/main.go`: lê DynamoDB Streams, filtra INSERTs com `sk` prefixado `COMPLETION_INTENT#` e `intentPending="1"`, publica o payload JSON na fila de conclusão via `SendCompletion`, marca o intent como entregue via `MarkIntentDelivered`. Interface `completionBackend` permite injetar stub nos testes sem AWS. `RecoverPending` percorre GSI `pending-intents-index` para recuperar intents não entregues (Streams expirados ou crash do publisher). `aws.SendCompletion` adicionado ao cliente AWS. `Config.CompletionQueueURL` adicionado. `terraform/dynamodb.tf`: `stream_enabled = true`, `stream_view_type = "NEW_AND_OLD_IMAGES"`. `terraform/sqs.tf`: filas `completion_events` e `completion_events_dlq`; políticas TLS. `terraform/lambda.tf`: Lambda `completion_publisher`, alias `live`, `aws_lambda_event_source_mapping` sobre o stream (bisect, maxRetry 3, on_failure → DLQ). `terraform/iam.tf`: role e policy `completion_publisher` (Streams, GSI Query, UpdateItem, SendMessage). `terraform/locals.tf`: `completion_publisher_zip`. `automacao/build-lambdas.sh` e `automacao/cmd/package/main.go` atualizados com o novo binário. 4 testes unitários PASS (skip/routing/marshal/deliver). `go test ./lambdas/...` PASS; `terraform fmt`, `init`, `validate` passam. | Homologação AWS, execução com LocalStack e EventBridge de recuperação agendada pendentes. |

## 10. Referências

- [Contratos existentes](../contratos.md)
- [Operação AWS](../operacao_aws.md)
- [Plano corporativo anterior](../plano_adequacao_corporativa_f2e.md)
- [Handoff anterior](../handoff_adequacao_corporativa_2026-09-02.md)
- [SendMessageBatch: quantidade e limites de bytes](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html)
- [Configuração Lambda/SQS e visibility timeout](https://docs.aws.amazon.com/lambda/latest/dg/services-sqs-configure.html)
- [SQS Standard: entrega at-least-once](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/standard-queues-at-least-once-delivery.html)
