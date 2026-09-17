# Plano de execução — F2E como solução única

## Objetivo e decisões fechadas

O F2E é uma solução de conversão de arquivos S3 em eventos SQS. Organizer e Worker são etapas internas da solução, não frameworks independentes. Manter duas Lambdas e a fila de chunks porque elas fornecem paralelismo e retry por chunk.

Este plano fixa os seguintes requisitos:

1. Há evento de conclusão por job. O Worker publica esse evento na fila `completion-events`; a entrega é *at-least-once* e recuperável.
2. URL S3 pré-assinada continua sendo uma forma suportada de leitura, inclusive com testes unitários e E2E.
3. Cada prefixo pode usar a fila de saída compartilhada ou uma fila de saída própria. A fila de conclusão continua única.
4. Permanecem os formatos `text`, `json` e `multi-line`, os identificadores estáveis, o ledger, o replay, as DLQs e o contrato Envelope v1.

Não remover uma capacidade exigida acima durante a limpeza. Fazer cada etapa em uma alteração revisável, com testes correspondentes, e atualizar a documentação junto com o código.

## Estado inicial que o agente deve confirmar

- `cmd/organizer/main.go` e `cmd/worker/main.go` são as únicas entradas Lambda implantadas por `terraform/lambda.tf`.
- `CompleteChunk` atualiza contadores, mas não chama `FinalizeJob`; a conclusão ainda não é publicada no fluxo de produção.
- `WriteCompletionIntent` e `PendingCompletionIntents` em `internal/platform/aws/ledger.go` pertencem a um desenho de publisher separado. O Terraform atual não cria o índice `pending-intents-index` citado por esse código.
- `ReserveSlot`, `WAITING` e métodos associados não têm chamadores no fluxo de produção.
- `RecordProcessor` não é configurado no bootstrap do Worker.
- `terraform/ssm.tf` atribui a mesma fila de saída a todos os prefixos, embora `ChunkJob.Configuration.OutputQueueURL` e o Worker já suportem roteamento.
- `e2e/internal/aws.go` e o dispatcher pressupõem uma fila de saída única.
- O repositório já tem um módulo Go principal; `e2e/` é módulo separado para a suíte. O README ainda descreve a antiga divisão com `lambdas/`.

Antes de editar, executar `git status --short`, `go test ./...` e `(cd e2e && go test ./...)` conforme a disponibilidade do ambiente. Registrar falhas preexistentes. Consultar `AGENTS.md` e usar exclusivamente o Git Bash indicado ali.

## 1. Implementar conclusão no Worker

### Contrato e máquina de estados

1. Definir em `internal/domain/f2e` quais resultados terminais geram `CompletionEvent`: `COMPLETED`, `WITH_REJECTIONS`, `FAILED` e `REJECTED`. Fixar contadores, `jobId`, versão e `eventId` determinístico. Documentar quando um chunk vai à DLQ e como o job chega a `FAILED`; não sinalizar sucesso quando há chunk sem desfecho.
2. Substituir a separação `FinalizeJob` + `WriteCompletionIntent` por uma operação de ledger que faça **transição terminal e criação de intenção de publicação na mesma transação DynamoDB**, condicionada ao estado e à contagem de chunks. Uma tentativa concorrente deve encontrar o mesmo evento, sem gerar nova versão. Não marcar a intenção como entregue antes da confirmação do SQS.
3. Após `CompleteChunk`, o Worker tenta fechar o job quando `completedChunks == expectedChunks`. A tentativa precisa ser segura quando vários Workers terminam juntos. Ao receber novamente um chunk já concluído, o Worker ainda deve executar a reconciliação/publicação da conclusão pendente antes de responder com sucesso à mensagem SQS.
4. O Worker envia o evento à `completion-events` e só depois marca a intenção como entregue. Falha no envio ou na marcação retorna falha parcial de lote para retry SQS. Um crash entre envio e marcação pode duplicar o evento; o `eventId` deve permanecer idêntico.
5. Tratar jobs com zero chunks e rejeições antes do primeiro chunk. Usar mensagem de controle durável na fila de trabalho, consumida pelo **mesmo Worker**, para publicar a conclusão. O Organizer pode registrar o estado e a intenção, mas não publica o evento final. Tornar a gravação e o agendamento recuperáveis por retry da mensagem de intake; cobrir a janela de crash entre os dois passos. Não deixar job em `PLANNING` indefinidamente.
6. Definir comportamento após esgotar retries: mensagem na DLQ de chunks ou controle, intenção ainda pendente no ledger, alarme e procedimento documentado de redrive. Se a exigência operacional for recuperação automática sem redrive, acrescentar um mecanismo de reconciliação que invoque o **Worker**, sem criar outro publisher. Não considerar um simples flag `completionPublished` gravado antes do envio como garantia de entrega.

### Arquivos prováveis

`internal/platform/aws/ledger.go`, `internal/application/port/ports.go`, `internal/application/worker/service.go`, `cmd/worker/main.go`, `cmd/organizer/main.go`, `internal/domain/f2e/completion.go`, `terraform/iam.tf`, `terraform/dynamodb.tf`, `terraform/sqs.tf`, `terraform/monitoring.tf` e runbooks.

### Testes obrigatórios

- Unitários do ledger e Worker: um chunk; dois chunks em conclusão concorrente; chunk duplicado; crash após completar chunk e antes de publicar; falha de SQS; envio bem-sucedido seguido de falha ao marcar entregue; zero chunks; rejeição; erro permanente e DLQ. Conferir contadores e `eventId` em cada retry.
- Integração DynamoDB/LocalStack da transação e das condições concorrentes.
- E2E: arquivo normal e array vazio produzem **um evento lógico de conclusão**; retry pode produzir entrega duplicada com o mesmo `eventId`; falhas não produzem conclusão de sucesso. Verificar ledger, fila de conclusão e DLQs.

**Aceite:** nenhum job terminal fica sem uma intenção durável; nenhuma mensagem de trabalho é confirmada enquanto a publicação de conclusão correspondente está pendente; o redrive conclui uma intenção pendente sem republicar registros de um chunk já concluído.

## 2. Limpar legado após a conclusão funcionar

1. Remover métodos, tipos, consultas e comentários do antigo Completion Publisher que não participarem do fluxo escolhido. Manter somente a outbox necessária à conclusão pelo Worker. Remover consultas a índices que o Terraform não cria, ou criar o índice apenas se houver um reconciliador real que o use.
2. Remover `ReserveSlot`, `ReleaseSlot`, `WAITING`, `MaxActiveJobs`, `ErrQuotaExceeded`, `WaitingAdmission` e métodos sem chamador, além do armazenamento auxiliar correspondente. Conferir testes e documentação antes de remover campos de contratos externos.
3. Reduzir `JobLedger` às operações usadas pelo Organizer e Worker. Preferir interfaces pequenas por consumidor; preservar condições e transações de idempotência.
4. Retirar `ErrNotImplemented`, comentários `T07/T08/T13/T20/T22` obsoletos e o binário versionado `lambdas/completion-publisher.exe`. Atualizar `.gitignore` para impedir novo commit de executáveis gerados.

**Aceite:** busca estática não encontra referências ao publisher separado, a `WAITING` nem a índices inexistentes; testes de conclusão da etapa 1 continuam passando.

## 3. Remover a extensão genérica sem uso

Remover `RecordProcessor`, `RecordDecision` e os ramos de publicar/rejeitar/ignorar que existem somente para injeção desse plugin, desde que a busca por chamadores confirme que nenhuma regra concreta os usa. Preservar os contadores e razões que ainda forem produzidos pelo próprio parser ou pelo fluxo de erro. Ajustar contratos e testes para refletir o processamento real do Worker.

Manter interfaces para S3, SQS e ledger onde elas viabilizam testes ou separam efeitos externos. A simplificação é retirar variação hipotética, não acoplar toda a lógica ao SDK.

**Aceite:** Worker continua gerando os mesmos envelopes e IDs para os três formatos; não há campo opcional de plugin sem implementação na solução.

## 4. Consolidar o requisito de URL pré-assinada

1. Preservar `presignedUrl` no contrato explícito, no `ChunkJob` e em `OpenChunkRange`. Validar a combinação entre bucket, key, version/ETag e URL. Manter leitura por intervalo com verificação de `Content-Range`, tamanho e identidade do objeto. Não trocar silenciosamente para S3 SDK quando a URL fornecida falhar.
2. Definir tempo de validade exigido para a URL em relação ao tempo máximo de processamento e retries. Expiração deve produzir erro classificável e caminho documentado de recuperação; replay precisa receber uma URL nova ou usar uma fonte autorizada, pois o ledger não persiste a credencial.
3. Testes unitários: URL válida com `Range`; resposta sem `206`/intervalo incorreto; ETag ou versão divergente; URL expirada; host ou esquema inválido; erro HTTP transitório. Não registrar a URL completa em logs ou ledger.
4. E2E: gerar URL de objeto versionado no ambiente de teste, enviar `OrganizerRequest`, forçar processamento em múltiplos chunks e verificar quantidade, IDs e conteúdo. Para LocalStack, aceitar apenas o host do endpoint de teste configurado; a validação de produção continua restrita a S3. Incluir cenário de URL inválida/expirada que chega a retry ou DLQ sem publicar registros parciais.

**Aceite:** os testes provam a leitura efetiva via HTTP pré-assinado, não apenas a presença do campo no JSON.

## 5. Fila de saída opcional por prefixo

1. Criar um catálogo Terraform de prefixos como fonte única para configuração SSM e seleção de fila. Cada entrada informa formato e `dedicated_output_queue` (ou referência equivalente); o padrão usa `output-events` compartilhada. Para entradas dedicadas, criar fila SQS própria com criptografia, retenção, política TLS, tags, alarmes e outputs por prefixo.
2. Gerar `outputQueueURL` no SSM a partir do recurso criado no mesmo `for_each`. Evitar URL livre em `.tfvars` quando o Worker não tem permissão correspondente. Atualizar IAM do Worker para permitir envio às filas geradas e à compartilhada, sem `sqs:*` ou recurso `*`.
3. Na admissão, validar que o prefixo selecionado captura a fila correta no snapshot imutável do job. O Worker deve publicar todos os registros daquele job nessa fila mesmo que o SSM mude depois. Para requisição explícita sem prefixo configurado, definir e documentar o uso da fila compartilhada.
4. Adaptar o cliente e dispatcher E2E para consumir por URL configurada, sem drenar filas de outros cenários. Criar ao menos dois prefixos: um compartilhado e um dedicado. Enviar arquivos simultaneamente e verificar que cada fila contém apenas seus envelopes, inclusive após retry e replay. A fila `completion-events` recebe as conclusões de ambos.
5. Planejar migração Terraform: identificar prefixos e consumidores existentes, comparar `terraform plan` e preservar a URL compartilhada como padrão. Uma fila dedicada nova só passa a receber jobs admitidos após a configuração SSM mudar; drenar jobs antigos antes de remover permissões ou filas anteriores. Documentar outputs e rollback.

**Aceite:** um prefixo novo pode selecionar fila dedicada apenas por configuração Terraform; os demais continuam na compartilhada; IAM, alarmes, testes unitários e E2E refletem as filas reais.

## 6. Simplificar a estrutura para a solução única

1. Manter `cmd/organizer` e `cmd/worker` como entradas pequenas. Aproximar tipos e interfaces dos fluxos que os usam; deixar em pacote comum apenas contrato de mensagem, identidade e regras realmente compartilhadas. Evitar renomeação ampla sem redução mensurável de dependências.
2. Consolidar configuração de processo e snapshot de job: valores fixos da implantação vêm do ambiente; valores por arquivo vêm do snapshot. Eliminar fallback duplicado que permita a duas fontes discordarem silenciosamente. Não remover o snapshot necessário para retries e roteamento por prefixo.
3. Revisar os modos opcionais restantes (`bundle`, concorrência de publicação, requisição explícita) à luz dos requisitos e testes. Manter URL pré-assinada e requisição explícita porque são necessárias ao item 4. Remover `bundle` apenas se não houver consumidor que dependa desse contrato; registrar a decisão antes da alteração.
4. Atualizar README, arquitetura, contratos, ADRs e runbooks para descrever a topologia real: duas Lambdas, conclusão no Worker, fila de conclusão, URL pré-assinada e fila de saída opcional por prefixo. Arquivar ou corrigir propostas antigas que contradizem o estado final. Remover referências a `lambdas/go.mod`, `go.work` com módulo `lambdas` e publisher separado.

**Aceite:** build e testes têm comandos canônicos claros; documentação e Terraform descrevem o mesmo fluxo; nenhuma abstração remanescente é justificada apenas pela hipótese de terceiros ampliarem o framework.

## Verificação final e entrega

Executar `go test ./...`, testes de integração aplicáveis, `(cd e2e && go test -v -timeout 25m ./...)`, `terraform fmt -check`, `terraform validate` e `terraform plan` para os ambientes relevantes. Executar o fluxo local completo. Não tratar teste indisponível como aprovado: registrar o comando, a causa e a verificação alternativa.

Entregar junto com o código: resumo das mudanças por etapa, evidência dos testes, diffs de plano Terraform, instruções de migração e rollback das filas, e riscos ainda abertos. Não aplicar mudanças destrutivas de infraestrutura a um ambiente ativo sem revisar o plano e os recursos existentes.
