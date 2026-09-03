# Baseline de adequação corporativa — 2026-09-01

## Ambiente observado

| Ferramenta | Versão |
| --- | --- |
| Go | 1.26.4 windows/amd64 |
| Terraform | 1.14.3 |
| AWS CLI | 2.27.2 |
| Docker | 29.7.2 |
| LocalStack CLI | não instalada |

## Gates iniciais

| Gate | Resultado |
| --- | --- |
| `gofmt -l` | falhou: todos os arquivos Go listados precisavam de formatação |
| `go test -count=1 ./...` | passou |
| `go vet ./...` | passou |
| módulos `lambdas` | testes e vet passaram |
| E2E | passou indevidamente porque a suíte foi ignorada sem LocalStack |
| `terraform fmt -check -recursive` | falhou em `environments/*.tfvars` |
| `terraform init/validate/plan` | não concluído no baseline; dependência do provider ainda era baixada |

## Decisões de escopo da prova de conceito

- **Ordenação:** a aplicação não realizará ordenação. O arquivo deve entrar no
  fluxo com os registros na ordem correta; consumidores não devem inferir uma
  garantia adicional de ordenação global na saída do processamento distribuído.
- **Recuperação:** o RTO aceito é de até 1 hora. Não foi definido um RPO por
  perda de dados: os arquivos permanecem no bucket e o fluxo possui
  deduplicação, portanto a recuperação consiste em reprocessar os arquivos.
- **Retenção:** permanece parametrizada no Terraform. O padrão da PoC é de 15
  dias para objetos do bucket e ledger. O CloudWatch Logs usa 30 dias, menor
  período suportado acima dos 15 dias solicitados. Filas SQS usam 13 dias e
  DLQs 14 dias porque o serviço aceita no máximo 14 dias e a DLQ deve ter
  retenção maior que a fila de origem.
- **Volume:** o cenário esperado em uma aplicação produtiva é de milhões de
  registros por dia, mas dimensionamento e homologação desse volume não fazem
  parte desta PoC. Os cuidados para adaptação produtiva estão registrados no
  `README.md`.
- **Ambiente AWS:** contas, rede, tags corporativas, backend remoto, aprovadores
  e demais definições de implantação produtiva ficam fora do escopo da PoC e
  devem ser definidos pela aplicação que a adaptar para produção.
- **Dados e criptografia:** classificação dos dados e escolha/configuração da
  chave KMS também ficam fora do escopo da PoC. Uma adoção produtiva deve usar
  a classificação e a política criptográfica aprovadas pela organização.

SLOs, orçamento e os valores corporativos de produção não são inferidos por
esta baseline. Eles devem ser validados durante a adaptação e homologação da
aplicação produtiva.
