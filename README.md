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
