# Plano de adequação corporativa do F2E

## 1. Objetivo

Este documento transforma as lacunas identificadas na avaliação arquitetural do F2E em um plano de implementação executável pelo Codex.

O objetivo é evoluir o projeto de um MVP funcional para uma solução com controles compatíveis com uma aplicação corporativa de grande porte no setor financeiro, com foco em:

- integridade e rastreabilidade dos dados;
- processamento at-least-once controlado;
- reconciliação completa por arquivo e chunk;
- segurança de aplicação e infraestrutura;
- observabilidade operacional;
- testes confiáveis e automatizados;
- infraestrutura reproduzível;
- governança de contratos e supply chain.

O plano não altera a responsabilidade funcional definida no blueprint: idempotência funcional downstream, consistência de domínio e regras finais de negócio continuam fora do core do F2E.

## 2. Documentos normativos

Antes de executar qualquer fase, o Codex deve ler integralmente:

1. [`AGENTS.md`](../AGENTS.md);
2. [`F2E_blueprint_arquitetural_go.md`](F2E_blueprint_arquitetural_go.md);
3. [`Especificação — Envelope de Eventos do F2E.md`](Especificação%20—%20Envelope%20de%20Eventos%20do%20F2E.md);
4. [`contratos.md`](contratos.md);
5. [`estrutura-de-pacotes.md`](estrutura-de-pacotes.md);
6. este plano.

Em caso de divergência entre implementação e documentação, não escolher silenciosamente uma interpretação. Registrar a decisão em ADR, atualizar os documentos afetados e somente então implementar.

## 3. Protocolo de execução para o Codex

### 3.1 Regras gerais

- Executar comandos de projeto exclusivamente pelo Git Bash indicado no `AGENTS.md`.
- Preservar alterações preexistentes do usuário.
- Não executar `git reset --hard`, `git checkout --`, limpeza recursiva ou exclusão ampla.
- Não criar commits, tags, releases ou deployments sem solicitação explícita.
- Não avançar para a fase seguinte enquanto os critérios de saída da fase atual não forem satisfeitos.
- Preferir mudanças pequenas, verificáveis e separadas por assunto.
- Adicionar ou corrigir testes antes ou junto de cada mudança comportamental.
- Não reduzir validações para fazer testes passarem.
- Não ocultar falhas de batches, DLQs, linters, scanners ou Terraform.
- Não registrar URLs pré-assinadas, credenciais, payloads sensíveis ou conteúdo integral de arquivos em logs.
- Tratar LocalStack como teste de integração, não como prova de equivalência operacional com a AWS.

### 3.2 Ciclo obrigatório por item

Para cada item deste plano, o Codex deve:

1. inspecionar os arquivos e testes relacionados;
2. declarar a hipótese e o comportamento desejado;
3. implementar a menor mudança coerente;
4. formatar os arquivos alterados;
5. executar testes diretamente relacionados;
6. executar os gates globais aplicáveis;
7. revisar o diff;
8. registrar o resultado, riscos residuais e próximos passos.

### 3.3 Pontos de parada obrigatórios

O Codex deve parar e pedir decisão do usuário quando:

- uma mudança alterar o contrato público do envelope;
- for necessário escolher entre evento único e identidade determinística para `eventId`;
- for necessário escolher a tecnologia de persistência do ledger de jobs;
- um controle depender de padrões corporativos externos não presentes no repositório, como KMS, contas AWS, redes, tags, retenção ou classificação de dados;
- uma mudança exigir recursos pagos ou implantação em conta AWS;
- houver alteração destrutiva de infraestrutura ou dados;
- requisitos de ordenação, retenção, RTO, RPO ou replay não estiverem definidos.

## 4. Estado desejado

Ao final do plano, o F2E deve demonstrar:

- testes unitários, integração e E2E sem passos indefinidos;
- nenhuma vulnerabilidade alcançável conhecida no build aprovado;
- infraestrutura Terraform formatada, validada e planejável;
- leitura de uma versão imutável do objeto de origem;
- processamento limitado por memória e tamanho de mensagem;
- estado reconciliável de cada job e chunk;
- retries na menor unidade segura;
- logs estruturados e métricas operacionais;
- alarmes para falhas, backlog, DLQ, throttling e jobs incompletos;
- roles IAM separadas e de menor privilégio;
- criptografia, retenção e políticas de transporte declaradas;
- pipeline de CI com gates obrigatórios;
- documentação de operação, replay, incidentes e decisões arquiteturais.

## 5. Ordem de execução

```text
Fase 0 — Baseline e decisões
  |
  v
Fase 1 — Recuperar confiança nos testes
  |
  v
Fase 2 — Corrigir vulnerabilidades e supply chain
  |
  v
Fase 3 — Integridade imutável da origem
  |
  +--------------------+
  |                    |
  v                    v
Fase 4 — Contratos     Fase 5 — Streaming e limites
  |                    |
  +----------+---------+
             |
             v
Fase 6 — Ledger, reconciliação e replay
             |
             v
Fase 7 — Concorrência, retry e back-pressure
             |
             v
Fase 8 — Segurança e Terraform
             |
             v
Fase 9 — Observabilidade e operação
             |
             v
Fase 10 — CI/CD, desempenho e homologação
```

## 6. Fase 0 — Baseline e decisões arquiteturais

### Objetivo

Congelar uma referência verificável e resolver decisões que afetam várias fases.

### Tarefas

- [ ] Registrar versões efetivas de Go, Terraform, AWS CLI, Docker e LocalStack.
- [ ] Executar e registrar o resultado inicial de build, testes, vet, formatação e vulnerabilidades.
- [ ] Criar `documentacao/adr/`.
- [ ] Criar ADR sobre a semântica de `eventId` e `sourceRecordId`.
- [ ] Criar ADR sobre persistência do ledger de jobs.
- [ ] Criar ADR sobre suporte a URL pré-assinada.
- [ ] Criar ADR sobre os formatos oficialmente suportados versus experimentais.
- [ ] Definir ambientes alvo: local, desenvolvimento, homologação e produção.
- [ ] Definir requisitos ainda ausentes: ordenação, RTO, RPO, retenção, classificação de dados e volume esperado.

### Decisões mínimas

#### Identidade de evento

Escolher uma das abordagens e documentar:

1. `eventId` único por ocorrência e `sourceRecordId` determinístico; ou
2. `eventId` determinístico entre retries, alterando explicitamente a especificação.

A primeira opção mantém a separação conceitual mais clara entre ocorrência do evento e identidade física do registro.

#### Ledger de jobs

Avaliar pelo menos:

- DynamoDB com atualizações condicionais;
- Step Functions, se o escopo deixar de ser apenas event-driven por filas;
- armazenamento externo fornecido por uma porta do framework.

Para manter o core reutilizável, preferir uma porta de persistência na aplicação e um adapter DynamoDB na plataforma.

### Critérios de saída

- ADRs aprovados para as três decisões centrais.
- Baseline registrado sem modificar o comportamento para mascarar falhas.
- Requisitos corporativos desconhecidos explicitamente listados.

## 7. Fase 1 — Recuperar confiança nos testes

### Objetivo

Eliminar falsos positivos e tornar os testes capazes de bloquear regressões reais.

### Arquivos principais

- `e2e/suite_test.go`;
- `e2e/internal/steps.go`;
- `e2e/internal/aws.go`;
- `e2e/features/*.feature`;
- testes em `internal/**`;
- scripts em `automacao/`.

### Tarefas E2E

- [ ] Substituir as expressões de passos que não são reconhecidas por expressões compatíveis com a versão do Godog utilizada.
- [ ] Habilitar execução estrita para falhar em passos indefinidos ou pendentes.
- [ ] Criar um teste de sanidade que prove que um passo inexistente causa falha.
- [ ] Separar filas, buckets ou prefixos por execução para evitar compartilhar estado.
- [ ] Remover a dependência de drenar uma fila global antes de cada cenário.
- [ ] Se a drenagem continuar necessária, limitar o recurso a uma fila criada exclusivamente para a execução de teste.
- [ ] Garantir cleanup mesmo quando o cenário falhar.
- [ ] Desabilitar cache para a suíte E2E no comando oficial com `-count=1`.
- [ ] Validar atributos SQS `schema` e `format`, além do body.
- [ ] Validar ausência de mensagens inesperadas e duplicadas, não apenas `count >= expected`.
- [ ] Validar DLQs ao final de cada cenário.
- [ ] Validar `eventId`, `sourceRecordId`, offsets e números globais de registros.
- [ ] Adicionar cenários de retry e entrega duplicada.
- [ ] Adicionar cenário em que um batch possui uma mensagem válida e outra inválida.
- [ ] Adicionar cenário em que o SQS retorna falha parcial de publicação.

### Testes unitários adicionais

- [ ] Configuração ausente ou inválida.
- [ ] Parser de notificação S3 com múltiplos records, evento ignorado e key codificada.
- [ ] Adapter AWS com resposta parcial do SQS.
- [ ] URL pré-assinada inválida, HTTP, redirect, host não permitido e range incorreto.
- [ ] Alteração de ETag/versionId entre planejamento e leitura.
- [ ] Limites exatos de chunks e registros nas bordas.
- [ ] Arquivo sem LF final, CRLF e registro maior que o máximo.
- [ ] CSV com aspas, vírgulas, CRLF e quebras de linha internas.
- [ ] JSON array vazio, aninhado, com primitivas e conteúdo depois do array.
- [ ] Cancelamento por `context.Context`.
- [ ] Falha ao fechar stream.

### Gates

```bash
gofmt -l $(rg --files -g '*.go')
go test -count=1 ./...
go vet ./...
(cd lambdas && go test -count=1 ./... && go vet ./...)
(cd e2e && go test -count=1 -v ./...)
```

### Critérios de saída

- Zero cenários ou passos indefinidos.
- A suíte falha deliberadamente quando uma asserção é quebrada.
- Testes não apagam mensagens de filas que não pertençam à execução.
- Testes E2E executados em ambiente LocalStack recém-provisionado.

## 8. Fase 2 — Vulnerabilidades e supply chain

### Objetivo

Criar um build reproduzível e sem vulnerabilidades alcançáveis conhecidas.

### Tarefas

- [ ] Atualizar Go de 1.26.5 para pelo menos 1.26.6 em desenvolvimento e CI.
- [ ] Recompilar todos os artefatos com a versão corrigida.
- [ ] Executar `govulncheck` no framework e nas Lambdas.
- [ ] Avaliar também o módulo E2E.
- [ ] Atualizar dependências AWS SDK de forma coordenada entre os módulos.
- [ ] Executar `go mod tidy` em cada módulo e revisar os diffs.
- [ ] Executar `go mod verify` em cada módulo.
- [ ] Remover binários versionados em `lambdas/bin/` e garantir que sejam produzidos apenas pelo pipeline.
- [ ] Versionar `.terraform.lock.hcl`.
- [ ] Definir política de atualização de dependências e runtime.
- [ ] Gerar SBOM para binários e imagens utilizadas.
- [ ] Definir assinatura e verificação dos ZIPs de Lambda.
- [ ] Adicionar varredura de segredos no CI.
- [ ] Fixar imagens Docker por digest nos pipelines controlados.

### Gates

```bash
go mod verify
(cd lambdas && go mod verify)
(cd e2e && go mod verify)
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
(cd lambdas && go run golang.org/x/vuln/cmd/govulncheck@latest ./...)
```

### Critérios de saída

- Zero vulnerabilidades alcançáveis sem exceção formal aprovada.
- Binários de build não versionados.
- Dependências e providers travados por arquivos de lock.
- Build reproduzível a partir de checkout limpo.

## 9. Fase 3 — Integridade imutável do arquivo de origem

### Objetivo

Garantir que os bytes processados pertencem exatamente à identidade registrada no envelope.

### Mudanças de contrato interno

- `ObjectStore.Head` deve retornar uma identidade imutável tipada.
- `GetRange` deve receber essa identidade, e não apenas bucket/key.
- `ChunkJob` deve carregar a identidade imutável necessária para a leitura.

Exemplo conceitual:

```go
type ObjectIdentity struct {
    Bucket    string
    Key       string
    VersionID string
    ETag      string
    Size      int64
}
```

### Tarefas

- [ ] Habilitar versionamento no bucket AWS.
- [ ] Usar `VersionId` no `GetObject` quando disponível.
- [ ] Quando não houver versionamento, usar condição equivalente a `If-Match` com ETag.
- [ ] Validar tamanho e identidade antes de processar o chunk.
- [ ] Falhar de forma explícita se o objeto tiver mudado.
- [ ] Incluir a identidade imutável no cálculo de `fileId`.
- [ ] Definir comportamento para ETags multipart, que não representam MD5 simples.
- [ ] Garantir que replay use a mesma versão do objeto.
- [ ] Cobrir substituição e exclusão do objeto com testes.
- [ ] Atualizar contratos e exemplos da documentação.

### Critérios de saída

- Alterar uma key entre planejamento e execução não produz eventos com proveniência incorreta.
- Todos os eventos conseguem apontar para a versão física processada.
- Replay de um job lê a mesma versão original ou falha explicitamente.

## 10. Fase 4 — Contrato do envelope e atributos

### Objetivo

Alinhar a implementação à especificação File-to-Envelope e eliminar ambiguidades.

### Tarefas

- [ ] Implementar a decisão do ADR sobre `eventId` e `sourceRecordId`.
- [ ] Corrigir `recordNumber` para ser global por arquivo.
- [ ] Omitir `recordNumber` quando o formato não permitir cálculo confiável.
- [ ] Preencher `byteOffset` e `byteLength` sempre que tecnicamente determináveis.
- [ ] Propagar `transactionId`, `correlationId`, `traceId` e `source.system` quando fornecidos.
- [ ] Criar porta para atributos corporativos adicionais.
- [ ] Construir os atributos a partir do envelope final, depois do `RecordProcessor`.
- [ ] Validar quantidade, nomes, tipos e tamanho total dos atributos.
- [ ] Validar coerência entre `metadata.schema`, `metadata.format` e os Message Attributes.
- [ ] Impedir que extensões removam campos técnicos obrigatórios.
- [ ] Definir validação de schema do `data` como responsabilidade da aplicação hospedeira.
- [ ] Criar JSON Schema ou especificação equivalente para contratos versionados.
- [ ] Adicionar testes de compatibilidade retroativa.

### Critérios de saída

- O body permanece autossuficiente fora do SQS/SNS.
- Attributes e body nunca divergem.
- IDs corporativos atravessam o pipeline sem depender exclusivamente dos attributes.
- Existe regra documentada de compatibilidade para novas versões.

## 11. Fase 5 — Streaming, formatos e limites

### Objetivo

Garantir uso de memória limitado e comportamento previsível para arquivos grandes ou malformados.

### Estratégia de arquitetura

Separar explicitamente:

```text
SplitStrategy -> RecordDecoder -> EnvelopeMapper -> Processor -> Publisher
```

### Tarefas gerais

- [ ] Criar interfaces de estratégia por formato sem expor AWS ao domínio.
- [ ] Definir tamanho máximo de arquivo, chunk, registro e evento.
- [ ] Validar o tamanho serializado antes de publicar.
- [ ] Usar readers limitados para impedir alocação ilimitada.
- [ ] Tratar timeout iminente e cancelamento do contexto.
- [ ] Definir quais formatos são produção, preview ou não suportados.
- [ ] Rejeitar cedo configurações incompatíveis.

### CSV

- [ ] Implementar chunking consciente de registros CSV, incluindo quebras de linha dentro de campos entre aspas.
- [ ] Evitar `io.ReadAll` do arquivo completo.
- [ ] Preservar offsets físicos quando possível.
- [ ] Definir tratamento de BOM, charset, header e número variável de colunas.

### JSON Lines e NDJSON

- [ ] Garantir limite real por linha, inclusive sem LF final.
- [ ] Definir se qualquer valor JSON é aceito ou somente objetos.
- [ ] Produzir erro com posição física e identidade do job sem registrar o payload.

### JSON array

- [ ] Remover a sondagem fixa de 64 KiB ou torná-la configurável e limitada com erro claro.
- [ ] Suportar objetos, arrays, strings, números, booleanos e `null`, se o contrato permitir.
- [ ] Encerrar jobs no fim do array, não no fim arbitrário do arquivo.
- [ ] Usar parser streaming consciente de strings, escapes e nesting.
- [ ] Testar conteúdo antes e depois do array selecionado por path.

### Binário

- [ ] Proibir publicação de arquivo inteiro como um evento sem limite explícito.
- [ ] Definir framing, chunking ou classificação como formato não splittable.
- [ ] Considerar publicar referência S3 em vez de Base64 quando o payload exceder o limite.

### Multi-line

- [ ] Validar todos os campos do layout.
- [ ] Limitar quantidade de linhas e bytes por registro.
- [ ] Definir comportamento para header, trailer, linha órfã e marcador inválido.

### Critérios de saída

- Memória cresce com o chunk/registro configurado, não com o arquivo inteiro.
- Nenhuma mensagem maior que o limite do broker é enviada.
- Arquivo malformado falha com causa, posição e unidade de retry conhecidas.
- Testes de boundary cobrem antes, exatamente em cima e depois do limite.

## 12. Fase 6 — Ledger de jobs, reconciliação e replay

### Objetivo

Permitir provar que cada arquivo foi integralmente processado ou identificar exatamente o que falta.

### Modelo mínimo

```text
Job
  jobId
  fileId
  source identity
  status
  expectedChunks
  completedChunks
  failedChunks
  recordsProduced
  bytesProcessed
  createdAt
  startedAt
  completedAt
  lastError

Chunk
  jobId
  chunkId
  status
  attempt
  startByte
  endByte
  recordsProduced
  bytesProcessed
  startedAt
  completedAt
  lastError
```

### Tarefas

- [ ] Criar portas para registrar planejamento, início, sucesso e falha.
- [ ] Implementar adapter persistente escolhido no ADR.
- [ ] Usar operações condicionais/idempotentes.
- [ ] Registrar `expectedChunks` antes ou atomicamente com a publicação dos jobs.
- [ ] Evitar marcar job como planejado se a publicação dos chunks falhar parcialmente sem reconciliação.
- [ ] Registrar tentativa recebida do SQS.
- [ ] Marcar chunk concluído somente após publicação confirmada dos eventos.
- [ ] Calcular conclusão do job de forma idempotente.
- [ ] Publicar evento de conclusão do arquivo.
- [ ] Detectar chunks ausentes, presos e excedendo SLA.
- [ ] Implementar replay por job e chunk com trilha de auditoria.
- [ ] Definir retenção do ledger.
- [ ] Criar testes de concorrência sobre atualizações do ledger.

### Critérios de saída

- Para todo job, `expectedChunks = completedChunks + failedChunks + pendingChunks`.
- É possível listar precisamente chunks pendentes e falhos.
- Retry não incrementa contadores de sucesso duas vezes.
- Replay é auditável e não troca silenciosamente a versão do arquivo.

## 13. Fase 7 — Retry, concorrência e back-pressure

### Objetivo

Controlar throughput, custo e impacto downstream sem perder mensagens.

### Tarefas

- [ ] Usar efetivamente `F2E_BATCH_SIZE` ou remover a configuração.
- [ ] Definir se `F2E_WORKER_CONCURRENCY` representa concorrência interna ou concorrência Lambda.
- [ ] Preferir controle no event source mapping quando a concorrência interna não trouxer benefício comprovado.
- [ ] Configurar maximum concurrency nos mappings SQS.
- [ ] Configurar reserved concurrency nas Lambdas quando aplicável.
- [ ] Relacionar visibility timeout, Lambda timeout e retry policy por validação Terraform.
- [ ] Usar batch partial response corretamente.
- [ ] Em falha parcial de `SendMessageBatch`, repetir apenas itens falhos com backoff e jitter.
- [ ] Preservar IDs ao repetir publicação.
- [ ] Evitar republicar todo o chunk quando apenas alguns eventos falharem, quando for seguro.
- [ ] Definir máximo de tentativas e classificação de erros transitórios/permanentes.
- [ ] Configurar DLQ e redrive allow policy.
- [ ] Configurar retenção da DLQ maior que a fila principal.
- [ ] Definir política de redrive operacional, sem replay automático irrestrito.
- [ ] Validar quotas de SQS, Lambda, payload e concorrência.

### Critérios de saída

- Throughput máximo possui limite explícito e testado.
- Falha parcial não duplica desnecessariamente todo o batch.
- Poison messages chegam à DLQ após número conhecido de tentativas.
- Alarmes detectam backlog, throttling e DLQ.

## 14. Fase 8 — Segurança e Terraform

### Objetivo

Declarar controles preventivos e detectivos compatíveis com produção.

### Organização do Terraform

- [ ] Separar recursos locais dos recursos AWS quando as diferenças forem relevantes.
- [ ] Parametrizar nomes por organização, aplicação, ambiente e região.
- [ ] Adicionar validações nas variáveis.
- [ ] Configurar backend remoto com criptografia e locking.
- [ ] Versionar `.terraform.lock.hcl`.
- [ ] Evitar defaults de produção inseguros ou ambíguos.

### IAM

- [ ] Criar role exclusiva do Organizer.
- [ ] Criar role exclusiva do Worker.
- [ ] Remover ações inexistentes ou desnecessárias, incluindo revisão de `s3:HeadObject`.
- [ ] Restringir S3, SQS, KMS, logs e ledger aos recursos necessários.
- [ ] Adicionar condições de conta, região, origem e encryption context quando aplicável.
- [ ] Evitar políticas AWS-managed amplas quando uma policy dedicada for suficiente.

### S3

- [ ] Habilitar Block Public Access completo.
- [ ] Configurar Object Ownership com ACLs desabilitadas.
- [ ] Configurar versionamento.
- [ ] Configurar SSE-KMS conforme padrão corporativo.
- [ ] Negar transporte sem TLS.
- [ ] Configurar lifecycle e abort de multipart incompleto.
- [ ] Definir retenção, replicação e Object Lock conforme RPO/compliance.
- [ ] Restringir notificações por prefixo/sufixo quando aplicável.

### SQS

- [ ] Configurar SSE-KMS.
- [ ] Exigir TLS nas queue policies.
- [ ] Configurar retenção explícita de filas e DLQs.
- [ ] Adicionar redrive allow policy.
- [ ] Adicionar `aws:SourceAccount` e `aws:SourceArn` na publicação S3.
- [ ] Revisar necessidade da DLQ da fila de saída, pois sem consumer ela não recebe redrive.

### Lambda e logs

- [ ] Criar log groups antes das Lambdas.
- [ ] Configurar retenção e KMS nos logs.
- [ ] Configurar logging JSON e níveis.
- [ ] Habilitar tracing quando aprovado.
- [ ] Configurar arquitetura, memória, timeout e ephemeral storage explicitamente.
- [ ] Configurar reserved concurrency e maximum concurrency.
- [ ] Definir proteção de rollback e deployment seguro.

### URL pré-assinada

- [ ] Exigir HTTPS.
- [ ] Permitir somente hosts e formatos aprovados.
- [ ] Bloquear IPs privados, metadata endpoints e redirects inseguros.
- [ ] Definir timeout, limites e validação de range.
- [ ] Não registrar nem incluir a URL no envelope final.
- [ ] Avaliar tokenização ou armazenamento protegido para evitar repetir a credencial em cada job.
- [ ] Definir comportamento quando a URL expirar durante o job.

### Gates

```bash
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false
terraform -chdir=terraform validate
terraform -chdir=terraform plan -var-file=environments/local.tfvars
```

No CI, adicionar scanner IaC aprovado pela organização e políticas de conformidade.

### Critérios de saída

- `terraform validate` e planos dos ambientes passam.
- Nenhuma role compartilhada ou permissão sem justificativa.
- Criptografia, TLS, retenção e bloqueio público estão declarados.
- O plano de produção não usa LocalStack nem credenciais estáticas.

## 15. Fase 9 — Observabilidade, auditoria e operação

### Objetivo

Responder operacionalmente às perguntas definidas no blueprint.

### Logs estruturados

Cada log deve ser JSON e, conforme o contexto, incluir:

- `service`;
- `environment`;
- `level`;
- `message`;
- `awsRequestId`;
- `sqsMessageId`;
- `receiveCount`;
- `fileId`;
- `jobId`;
- `chunkId`;
- `eventId` ou `sourceRecordId`;
- `bucket` e `key`, quando permitidos pela classificação;
- `startByte` e `endByte`;
- duração;
- classificação do erro.

Não registrar payload funcional nem URL pré-assinada.

### Métricas mínimas

- arquivos recebidos, aceitos e rejeitados;
- jobs planejados, concluídos, falhos e presos;
- chunks planejados, concluídos, falhos e reprocessados;
- bytes lidos;
- registros produzidos e descartados;
- tempo de planejamento e processamento;
- TPS por chunk e job;
- falhas parciais do SQS;
- backlog e idade da mensagem mais antiga;
- mensagens em DLQ;
- throttles e concorrência Lambda;
- uso de memória e aproximação do timeout.

### Alarmes mínimos

- DLQ com mensagem disponível;
- idade do backlog acima do SLA;
- erro ou throttle acima do limite;
- duração próxima do timeout;
- job sem progresso;
- divergência entre chunks esperados e concluídos;
- ausência do evento de conclusão dentro do SLA;
- crescimento anômalo de custo ou volume.

### Runbooks

- [ ] Mensagem na DLQ de intake.
- [ ] Mensagem na DLQ de chunks.
- [ ] Falha na publicação de saída.
- [ ] Job incompleto ou sem progresso.
- [ ] Objeto alterado ou removido.
- [ ] URL pré-assinada expirada.
- [ ] Replay de arquivo e chunk.
- [ ] Suspensão emergencial via concorrência zero.
- [ ] Recuperação após indisponibilidade de S3, SQS, KMS ou ledger.

### Critérios de saída

- Dashboard responde às perguntas da seção de observabilidade do blueprint.
- Cada alarme possui owner, severidade e runbook.
- Um operador consegue rastrear evento → chunk → job → versão do arquivo.
- Um job incompleto é detectado sem inspeção manual de logs.

## 16. Fase 10 — CI/CD, desempenho e homologação

### Pipeline de pull request

- [ ] Verificação de formatação Go e Terraform.
- [ ] Testes unitários com cobertura.
- [ ] `go vet` e linter aprovado.
- [ ] Race detector em runner compatível com CGO.
- [ ] `govulncheck`.
- [ ] Varredura de segredos.
- [ ] Scanner IaC.
- [ ] `terraform validate`.
- [ ] Build reproduzível das Lambdas.
- [ ] Testes E2E em LocalStack isolado.
- [ ] Publicação de relatórios e artefatos, sem deploy.

### Pipeline de promoção

- [ ] Gerar SBOM e checksums.
- [ ] Assinar artefatos.
- [ ] Publicar em repositório imutável.
- [ ] Planejar Terraform por ambiente.
- [ ] Exigir aprovação para produção.
- [ ] Implantar por versão/alias de Lambda.
- [ ] Executar smoke tests e reconciliação.
- [ ] Fazer rollback automático ou manual documentado.

### Desempenho

- [ ] Definir arquivos representativos por formato e tamanho.
- [ ] Medir custo por GB, custo por milhão de registros e duração do job.
- [ ] Medir cold start, memória máxima e throughput.
- [ ] Testar concorrência crescente até o primeiro gargalo.
- [ ] Executar Lambda Power Tuning ou método equivalente.
- [ ] Testar 1 milhão de registros com validação de identidade, não somente contagem aproximada.
- [ ] Testar falhas induzidas de SQS, S3, KMS e ledger.
- [ ] Definir limites operacionais aprovados.

### Critérios de saída

- Nenhum deploy depende de binário criado manualmente.
- Todos os gates deixam evidência auditável.
- Homologação AWS comprova integridade, throughput, custo e recuperação.
- Limites e SLOs estão documentados e monitorados.

## 17. Matriz de rastreabilidade

| Requisito | Implementação esperada | Evidência |
|---|---|---|
| At-least-once | IDs, retries e consumers idempotentes | Testes de duplicação |
| Retry por chunk | SQS, partial batch e ledger | Cenário de falha parcial |
| Identidade imutável | S3 VersionId ou ETag condicional | Teste de substituição do objeto |
| Completude do arquivo | Ledger e evento de conclusão | Reconciliação automática |
| Rastreabilidade | Envelope e logs estruturados | Busca evento → arquivo |
| Back-pressure | Filas e limites de concorrência | Teste de carga |
| Segurança | IAM, KMS, TLS e políticas | Scanner IaC e plano Terraform |
| DLQ e replay | Retenção, alarmes e runbook | Exercício operacional |
| Contrato versionado | Schema e testes de compatibilidade | Gate de contrato |
| Supply chain | Locks, SBOM, scanner e assinatura | Pipeline de build |

## 18. Definition of Done global

O projeto somente deve ser classificado como pronto para avaliação de produção quando:

- [ ] todos os ADRs obrigatórios estiverem aprovados;
- [ ] todos os testes E2E forem executados, definidos e estritos;
- [ ] os testes cobrirem falhas, retry, duplicação e reconciliação;
- [ ] não houver vulnerabilidade alcançável sem exceção formal;
- [ ] `gofmt`, testes, vet, race detector e scanners passarem;
- [ ] Terraform estiver formatado, validado e planejado por ambiente;
- [ ] a origem física do arquivo for imutavelmente verificável;
- [ ] jobs e chunks forem reconciliáveis;
- [ ] limites de memória, payload, timeout e concorrência forem explícitos;
- [ ] segurança S3/SQS/Lambda/KMS estiver declarada como código;
- [ ] logs, métricas, alarmes e runbooks estiverem operacionais;
- [ ] artefatos forem reproduzíveis, rastreáveis e assinados;
- [ ] teste de carga em AWS homologação cumprir SLO e custo aprovados;
- [ ] riscos residuais tiverem owner e aceite formal.

## 19. Prompt sugerido para execução faseada no Codex

Usar um prompt por fase, sem solicitar todas as fases de uma vez:

```text
Execute a Fase N do arquivo
documentacao/plano_adequacao_corporativa_f2e.md.

Leia primeiro todos os documentos normativos indicados no plano e respeite o
AGENTS.md. Inspecione o estado atual antes de editar. Implemente somente o escopo
da fase, preserve alterações preexistentes, adicione testes e execute todos os
gates aplicáveis. Não crie commit nem faça deploy. Ao final, apresente arquivos
alterados, decisões tomadas, testes executados, resultados, riscos residuais e
itens que ainda bloqueiam os critérios de saída da fase.
```

Para fases com ponto de decisão arquitetural, solicitar primeiro apenas a análise:

```text
Analise as decisões pendentes da Fase N do plano de adequação corporativa.
Não altere arquivos ainda. Apresente opções, trade-offs, recomendação e impacto
nos contratos, testes, infraestrutura e operação. Aguarde minha decisão antes de
implementar.
```

## 20. Resultado esperado do programa

O resultado não deve ser apenas uma aplicação que processa arquivos em condições ideais. O F2E deve ser capaz de demonstrar, de forma auditável:

```text
qual arquivo e versão foram processados
        +
quais chunks foram planejados
        +
quais chunks concluíram ou falharam
        +
quais eventos foram produzidos
        +
quais retries e replays ocorreram
        +
quais controles protegeram os dados
        =
processamento reconciliável e operacionalmente confiável
```

