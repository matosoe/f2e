# Propostas de simplificação do F2E

## Objetivo e limites

Este documento reduz código, recursos legados e esforço operacional sem mudar
o contrato atual. Permanecem obrigatórios: fluxo S3/solicitação explícita para
SQS; os modos `text`, `json` e `multi-line`; paralelismo por chunks; entrega
*at-least-once*; `eventId` e `sourceRecordId`; ledger, replay, DLQs e evento
de conclusão recuperável; configuração SSM e isolamento atual por prefixo.

Não é recomendável substituir o ledger por memória, remover a outbox de
conclusão ou compartilhar recursos de prefixos protegidos: isso reduziria
recursos às custas de recuperação, rastreabilidade ou isolamento.

## Prioridade recomendada

| Ordem | Proposta | Ganho | Risco |
|---:|---|---|---|
| 1 | Remover topologia Terraform legada | Menos recursos/alarmes duplicados | Baixo |
| 2 | Criar catálogo único de prefixos | Menos configuração divergente | Baixo a médio |
| 3 | Definir perfis operacionais | Menos decisões por ambiente | Baixo |
| 4 | Unificar build e comandos Go | Menos módulos e passos manuais | Médio |
| 5 | Compactar os scripts de automação | Menos superfície de manutenção | Baixo |

## 1. Encerrar recursos Terraform legados

O módulo `terraform/modules/f2e-prefix` já cria fila de chunks, DLQ, fila de
saída, DLQ e Worker por prefixo. A raiz, porém, ainda declara e referencia o
Worker e as filas compartilhadas antigos (`aws_lambda_function.worker` e
`aws_sqs_queue.chunk_jobs`), inclusive em métricas, aliases e outputs. Isso
mantém uma segunda topologia que não atende ao isolamento por prefixo e deixa
o plano operacional ambíguo.

**Proposta.** Quando todos os prefixos tiverem sido migrados, remover os
recursos compartilhados e derivar outputs e dashboard dos módulos
`module.prefix`. Remover também `F2E_CHUNK_QUEUE_URL` do ambiente comum: o
Worker já deve conhecer apenas a fila vinculada ao seu gatilho.

**Preserva requisitos.** Cada Worker continua consumindo sua fila exclusiva e
publicando apenas na saída correspondente; não muda contrato, ledger nem retry.

**Proteções.** Inventariar recursos ativos, executar os `terraform state mv`
documentados, revisar o `plan` para não destruir filas vivas e rodar E2E para
cada prefixo antes da remoção.

## 2. Usar um catálogo de prefixos como fonte única de verdade

A definição de um prefixo está repartida entre `terraform/locals.tf`, SSM,
`prefix_worker_config`, variáveis de ambiente e o módulo Terraform. Isso abre
espaço para SSM apontar para fila não criada ou para capacidade não associada ao
prefixo correto.

**Proposta.** Definir um objeto Terraform por prefixo, com contrato do arquivo,
limites, perfil de capacidade e referências produzidas. Gerar o JSON do SSM e
instanciar `f2e-prefix` a partir desse mesmo objeto. SSM segue como fonte
dinâmica consultada pelo Organizer; o catálogo apenas elimina duplicação na
provisão.

**Preserva requisitos.** Seleção pelo prefixo mais longo, limites globais e
snapshot na admissão não mudam. Campos que alteram topologia passam
explicitamente por Terraform; os demais continuam atualizáveis no SSM.

**Aceite.** O plano deve provar que cada `outputQueueURL` pertence ao módulo
do mesmo prefixo; testes devem cobrir prefixos sobrepostos e rejeitar fila
externa.

## 3. Oferecer perfis operacionais, não caminhos funcionais novos

KMS, retenção, concorrência, alarmes, quotas e limites são necessários, mas
não precisam ser escolhas repetidas em todo ambiente.

**Proposta.** Disponibilizar perfis `local`, `baseline` e `production`, cada
um com valores seguros e documentados; arquivos `.tfvars` informam apenas
exceções. O perfil de produção exige responsáveis, retenção e segurança.

**Preserva requisitos.** A infraestrutura e as validações continuam iguais.
Nenhum perfil de produção pode desativar ledger, completion outbox, SSM global,
DLQs ou isolamento por prefixo.

## 4. Avaliar a unificação do módulo Go e do build

Há dois módulos Go, `.` e `lambdas/`, ligados por `go.work` e `replace` local.
Isso implica dois `go.mod`, dois `go.sum` e comandos de teste separados, embora
as Lambdas consumam o mesmo core.

**Proposta.** Em uma mudança isolada, mover as entradas para `cmd/organizer`,
`cmd/worker` e `cmd/completion-publisher` no módulo raiz e manter um único
`go.mod`. A automação passa a compilar os três binários na raiz.

**Preserva requisitos.** É uma alteração de organização; portas, domínio,
binários implantados e contratos não mudam.

**Decisão condicionada.** Só adotar se o empacotamento Lambda permanecer
simples e `go test ./...` cobrir tudo. Se a separação for uma fronteira de
release intencional, manter `go.work` e acrescentar apenas um alvo único de
teste e build.

## 5. Reduzir a superfície de scripts

`automacao/` possui scripts próximos para subir, executar, testar, validar e
fazer benchmark local e AWS.

**Proposta.** Manter três entradas canônicas: `ambiente local up|down|run|test`,
`ambiente aws up|down|e2e` e `benchmark <perfil> <alvo>`. Os scripts atuais
podem virar wrappers temporários; variáveis compartilhadas ficam em
`parametros.sh`, assim como geração e validação de manifestos.

**Preserva requisitos.** Testes unitários, E2E, LocalStack, AWS e benchmarks
seguem disponíveis; a mudança elimina apenas lógica de shell duplicada.

## Sequência de execução

1. Criar testes de contrato da topologia por prefixo e plano de migração sem
   destruição.
2. Remover recursos Terraform compartilhados legados e atualizar outputs,
   monitoramento e runbooks.
3. Introduzir catálogo único de prefixos e perfis de ambiente.
4. Consolidar automação; avaliar a unificação dos módulos Go em branch isolada.

Em cada etapa, executar `go test ./...`, os testes de `lambdas/` e o fluxo E2E.
Só aceitar se todos os registros forem produzidos sem lacunas, as DLQs ficarem
vazias, IDs forem estáveis em retry e a conclusão continuar recuperável.

## Fora do escopo

- Trocar SQS Standard por FIFO para impor ordenação global.
- Adicionar CSV, XML, JSONL, binário ou outros formatos removidos.
- Eliminar checkpoints, retry seletivo, DLQs ou outbox.
- Compartilhar Worker/fila entre prefixos que exigem isolamento.

