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

## Requisitos ainda sem definição corporativa

- ordenação por arquivo/registro;
- RTO e RPO;
- retenção de objetos, filas, DLQs, logs e ledger;
- classificação de dados e política KMS;
- volume esperado, SLOs e orçamento;
- contas AWS, rede, tags corporativas, backend remoto e aprovadores de produção.

Esses itens bloqueiam somente a configuração final de homologação/produção; não
autorizam valores implícitos em infraestrutura real.
