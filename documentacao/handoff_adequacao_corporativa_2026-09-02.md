# Handoff para finalizar a adequação corporativa do F2E

Data do handoff: 2026-09-02.

Este arquivo registra somente o que precisa ser retomado para encerrar o plano
`documentacao/plano_adequacao_corporativa_f2e.md`. As decisões e instruções dos
documentos do repositório continuam normativas; este handoff não as substitui.

## 1. Resultado já comprovado

- O E2E definitivo foi executado em LocalStack recriado, com DynamoDB/ledger
  habilitado.
- Resultado: **35 de 35 cenários e 147 de 147 passos passaram**.
- O teste incluiu arquivos vazios, 1, 2, 1.000 e 10.000 registros, conforme cada
  feature.
- O JSON array de 10.000 elementos passou após a correção das fronteiras dos
  chunks; a falha anterior parava deterministicamente em 9.202 eventos.
- As seis mensagens inválidas chegaram à DLQ de intake.
- Nenhuma mensagem chegou indevidamente à DLQ de chunks.
- Os 29 jobs válidos terminaram reconciliados no ledger, sem chunks ausentes,
  falhos ou contadores divergentes.
- Os testes de integração do ledger passaram para:
  - ciclo `Plan -> Scheduled -> Running -> Completed`;
  - conclusão repetida/idempotente;
  - conclusões concorrentes e duplicadas sem incrementar contadores duas vezes.
- Terraform 1.16.0 validou a configuração e gerou planos para `local`, `aws`,
  `staging` e `production`: 45 recursos a criar, zero alterações e zero
  destruições em cada plano.
- `actionlint`, `bash -n` e a validação sintática do JSON Schema passaram.
- `govulncheck` havia passado nos três módulos antes das últimas pequenas
  alterações; deve ser repetido conforme a seção seguinte.

## 2. Primeiro passo ao retomar: gates finais

Use exclusivamente o Git Bash indicado pelo `AGENTS.md`. Não use o inicializador
WSL chamado apenas como `bash` no PowerShell.

O último comando agregado de gates foi interrompido pelo encerramento desta
sessão. Mesmo que alguns subprocessos tenham terminado, considere a rodada
inconclusiva e execute tudo novamente.

```bash
# Raiz do repositório
gofmt -w $(rg --files -g '*.go')
test -z "$(gofmt -l $(rg --files -g '*.go'))"
go test -count=1 ./...
go vet ./...
go mod verify

(cd lambdas && go test -count=1 ./... && go vet ./... && go mod verify)
(cd e2e && go test -count=1 ./internal/... && go vet ./... && go mod verify)

go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...
(cd lambdas && go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...)
(cd e2e && go run golang.org/x/vuln/cmd/govulncheck@v1.7.0 ./...)

go run github.com/rhysd/actionlint/cmd/actionlint@v1.7.12 \
  -color=false .github/workflows/*.yml

for file in automacao/*.sh automacao/localstack/*.sh; do
  bash -n "$file"
done

jq empty documentacao/schemas/envelope-v2.schema.json
git diff --check
```

Observação: o race detector local exige CGO e um compilador C. Nesta máquina ele
não pôde ser executado porque `CGO_ENABLED=0` e não havia GCC. O workflow de CI
já executa `go test -race` em Ubuntu. No novo computador, executar localmente se
o ambiente possuir CGO:

```bash
CGO_ENABLED=1 go test -count=1 -race ./...
(cd lambdas && CGO_ENABLED=1 go test -count=1 -race ./...)
```

## 3. Revalidar o build reproduzível

As Lambdas foram reconstruídas para o E2E, mas o teste de duas construções com
hash idêntico deve ser repetido depois das correções finais de JSON e ledger.

```bash
bash automacao/build-lambdas.sh
first="$(sha256sum .build/organizer.zip .build/worker.zip)"

bash automacao/build-lambdas.sh
second="$(sha256sum .build/organizer.zip .build/worker.zip)"

test "$first" = "$second"
(cd .build && sha256sum --check SHA256SUMS)
```

## 4. Revalidar Terraform com a versão usada pelo CI

O computador novo deve usar Terraform 1.16.0. Não é necessário reutilizar o
binário temporário desta máquina.

```bash
terraform -chdir=terraform fmt -check -recursive
terraform -chdir=terraform init -backend=false -input=false
terraform -chdir=terraform validate

for environment in local aws staging production; do
  terraform -chdir=terraform plan \
    -input=false \
    -lock=false \
    -refresh=false \
    -var-file="environments/${environment}.tfvars" \
    -out="../.build/${environment}.tfplan"
done
```

Não aplicar os planos `aws`, `staging` ou `production` enquanto os placeholders
de conta, bucket, KMS e backend não forem substituídos por valores corporativos
aprovados.

## 5. Repetição opcional do E2E completo

O E2E completo já passou e levou aproximadamente 18 minutos. Repita-o se houver
qualquer mudança em código de runtime, scripts LocalStack, contratos de fila ou
ledger. Para mudanças apenas documentais, não é necessário.

```bash
bash automacao/subir-ambiente.sh
(cd e2e && go test -count=1 -v -timeout 30m ./...)
```

Resultado esperado:

- 35 cenários aprovados;
- 147 passos aprovados;
- 6 mensagens na DLQ de intake;
- 0 mensagens na DLQ de chunks;
- 29 jobs válidos em `COMPLETED`;
- para cada job, `expectedChunks = completedChunks`, com `failedChunks = 0`;
- teste de passo indefinido aprovado por provar que a suíte estrita falha.

Para repetir apenas a integração concorrente do ledger depois de subir o
LocalStack:

```bash
export F2E_TEST_AWS_ENDPOINT=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

go test -count=1 \
  -run 'TestLedger(Lifecycle|ConcurrentCompletion)AgainstLocalStack' \
  -v ./internal/platform/aws
```

## 6. Construções técnicas ainda necessárias

Os itens abaixo ainda impedem afirmar que toda a Definition of Done do plano foi
cumprida. Eles devem ser implementados antes da homologação de produção.

### 6.1 Evento de conclusão de arquivo

O ledger calcula o estado terminal do job, mas ainda não publica um evento de
conclusão de arquivo em canal próprio.

Implementação recomendada:

1. habilitar DynamoDB Streams no ledger com `NEW_AND_OLD_IMAGES`;
2. criar uma fila dedicada a eventos de conclusão;
3. criar uma Lambda reconciler acionada pelo stream;
4. publicar somente na transição de job não terminal para `COMPLETED` ou
   `FAILED`;
5. usar `eventId` determinístico a partir de `jobId` e estado terminal para
   tolerar reentrega do stream;
6. adicionar IAM mínimo, criptografia, TLS, retenção, logs e alarmes;
7. testar entrega duplicada do stream e garantir um contrato idempotente.

Não publicar conclusão na fila de eventos de registros, pois isso mistura dois
contratos e quebra consumidores que esperam apenas envelopes de registros.

### 6.2 Detecção automática de jobs presos

Hoje backlog, erros, throttles, duração e DLQs possuem alarmes, mas ainda falta
detecção ativa de jobs sem progresso no ledger.

Implementação recomendada:

1. manter `updatedAt` em toda transição de job/chunk;
2. criar índice adequado para consultar estados não terminais por idade, evitando
   `Scan` contínuo em produção;
3. executar reconciler agendado pelo EventBridge;
4. emitir métricas para jobs/chunks presos e divergência de contadores;
5. alarmar acima do SLA configurável e vincular ao runbook;
6. não fazer replay automático irrestrito.

### 6.3 Métricas funcionais e evento ausente

Existem logs JSON e alarmes de serviços AWS. Ainda faltam métricas funcionais
explícitas para arquivos aceitos/rejeitados, chunks reprocessados, registros
descartados, falhas parciais e jobs sem progresso. Preferir Embedded Metric
Format ou uma porta de métricas, sem registrar payload ou URL pré-assinada.

### 6.4 Cobertura de falhas realmente E2E

Retry, IDs estáveis, batch misto e falha parcial de publicação possuem cobertura
unitária/integrada, mas não todos como cenários Godog com falhas induzidas no
LocalStack. Criar mecanismos determinísticos de fault injection e cenários para:

- redelivery da mesma mensagem;
- batch com mensagem válida e inválida;
- falha parcial do `SendMessageBatch`;
- indisponibilidade temporária de S3, SQS, KMS e ledger;
- substituição/exclusão da versão física entre planejamento e leitura.

### 6.5 Limitações dos formatos preview/experimentais

- CSV permanece `preview`; arquivos grandes são limitados por chunk e não têm
  splitter distribuído plenamente consciente de campos multilinha.
- JSON array é `preview`; revisar planejamento para encerrar a criação de jobs no
  fechamento do array selecionado quando existir conteúdo muito grande depois
  dele.
- `binary` e `multi-line` continuam experimentais e proibidos em produção.
- Manter essas classificações até testes de carga e contratos serem homologados.

## 7. Homologação externa que não pode ser concluída no repositório

Estes itens exigem valores, contas ou aprovação corporativa. Não inventar
defaults para fazê-los parecer concluídos.

- aprovação formal dos ADRs;
- contas e roles AWS de desenvolvimento, homologação e produção;
- bucket e chave KMS corporativos;
- backend remoto e locking do Terraform;
- configuração OIDC e environments/aprovadores do GitHub;
- classificação dos dados;
- ordenação requerida;
- RTO, RPO, retenções finais e SLOs;
- volume esperado e orçamento;
- owner e severidade de cada alarme;
- política de Object Lock e replicação;
- execução real de gitleaks, Checkov e demais gates no CI;
- carga de 1 milhão de registros com validação de identidade, não apenas contagem;
- medição em AWS de cold start, memória, throughput, custo por GB e custo por
  milhão de registros;
- teste de concorrência crescente e Lambda Power Tuning;
- exercícios de falha, replay, rollback e recuperação em homologação.

## 8. Critério para encerrar o trabalho

O trabalho deste plano poderá ser declarado encerrado quando:

1. todos os gates da seção 2 passarem em uma única revisão;
2. o build reproduzível e os quatro planos Terraform forem revalidados;
3. evento de conclusão, detecção de job preso e métricas funcionais estiverem
   implementados e testados;
4. os cenários de falha E2E relevantes estiverem automatizados;
5. o workflow de CI real passar, incluindo race, linter, vulnerabilidades,
   segredo e IaC;
6. a homologação AWS cumprir os SLOs e custos aprovados;
7. riscos residuais tiverem owner e aceite formal.

Até que os itens corporativos da seção 7 sejam fornecidos, o repositório pode ser
considerado uma baseline local/CI endurecida e testada, mas não uma solução
formalmente aprovada para produção.
