# Automações

Esta pasta é o destino obrigatório de todos os artefatos usados para criar, executar e validar o ambiente local do F2E.

Os scripts shell desta pasta implementam a [operação local](../documentacao/operacao_local.md). A estrutura inclui:

- Docker Compose e inicialização idempotente do LocalStack;
- scripts Bash para subir e parar o ambiente;
- `parametros.sh` para definir a quantidade de registros do teste;
- gerador determinístico do arquivo conforme a quantidade configurada;
- execução ponta a ponta por upload S3;
- validação da quantidade configurada de mensagens na fila de saída;
- massas em `automacao/dados` e resultados locais em `automacao/resultados`.

## AWS real

As automações AWS usam o arquivo local e ignorado
`terraform/environments/aws.local.tfvars`. Crie-o a partir de
`terraform/environments/ALTERAR_aws.local.tfvars.example` e preencha todos os
valores antes de executar.

- `subir-ambiente-aws.sh` cria ou atualiza a infraestrutura e **a mantém em
  execução** para upload, inspeção de filas, ledger, parâmetros SSM e logs. Ao
  terminar, ele lista todos os prefixos S3 configurados pelo Terraform.
- `parar-ambiente-aws.sh` é o único script que executa `terraform destroy`.
- `executar-e2e-aws.sh` sobe o ambiente, executa a suíte E2E em AWS real e o
  destrói ao terminar, inclusive se os testes falharem. Ele aceita somente
  `environment = "development"`.
- `benchmark-aws-5m.sh` executa, fora da suíte funcional, o cenário de uma
  mensagem por linha com 10 Workers: valida primeiro 10 mil linhas e só então
  processa 5 milhões, coletando CPU, memória, init, GC e métricas CloudWatch.
  O mesmo harness aceita `BENCHMARK_OUTPUT_MODE=bundle`,
  `BENCHMARK_MAX_ENVELOPES_PER_MESSAGE=0` e
  `BENCHMARK_MAX_MESSAGE_BYTES=256000` para preencher mensagens até o teto de
  250 KiB sem limite de envelopes por quantidade.
- `benchmark-p3-memory.sh` executa a matriz P3.10 em 128, 256, 512 e 1.024
  MiB, três vezes por ponto, e grava o relatório comparativo em
  `documentacao/benchmark/`.

Defina `F2E_AWS_TFVARS=/caminho/para/outro.tfvars` para usar outro arquivo de
variáveis. Consulte [Operação na AWS](../documentacao/operacao_aws.md) para o
procedimento completo.
