# F2E MVP

MVP local em Go que recebe notificações S3 via SQS, divide arquivos de largura fixa e publica um evento por registro.

Pré-requisitos: Git Bash, Go 1.26+, Docker, AWS CLI v2, `zip` e `jq`.

No Git Bash, execute:

```bash
cd automacao
./executar-fluxo.sh
```

O resultado esperado é `20.000/20.000 — nenhuma lacuna, nenhuma DLQ e nenhum erro.` O framework está na raiz; as Lambdas estão no módulo `lambdas/`. Para testar: `go test ./...` na raiz e em `lambdas/`. Para compilar as funções: `cd lambdas && go build ./cmd/...`.

Consulte [operação local](documentacao/operacao_local.md) e os [contratos](documentacao/contratos.md).

## Adaptação para produção

Este repositório é uma prova de conceito e não está dimensionado nem homologado
para produção. Embora o cenário esperado possa alcançar milhões de registros
por dia, uma aplicação produtiva deve, antes da implantação:

- executar testes de carga representativos e definir throughput, concorrência,
  limites, SLOs, alarmes e orçamento com base no tamanho e na frequência reais
  dos arquivos;
- definir contas e regiões AWS, rede, roles, nomes, tags, backend remoto do
  Terraform, processo de aprovação e estratégia de recuperação, respeitando o
  RTO de até 1 hora;
- classificar os dados e, a partir dessa classificação, aprovar chaves e
  políticas KMS, acessos, auditoria e requisitos de compliance;
- revisar as retenções parametrizadas no Terraform. A PoC usa 15 dias para
  objetos S3 e ledger; o CloudWatch Logs usa 30 dias, menor período suportado
  acima de 15; filas usam 13 dias e DLQs 14 dias devido ao limite máximo de 14
  dias do SQS e à necessidade de a DLQ reter mensagens por mais tempo;
- garantir que cada arquivo já chegue corretamente ordenado. O F2E não ordena
  os registros e o processamento distribuído não oferece ordenação global das
  mensagens de saída;
- validar replay e deduplicação a partir dos arquivos retidos no bucket. Para
  esta PoC, a recuperação é feita por reprocessamento e não há um RPO separado.
