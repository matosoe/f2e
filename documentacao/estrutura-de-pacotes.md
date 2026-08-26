# Estrutura de pacotes Go

O repositório possui dois módulos Go. A raiz, `github.com/f2e/f2e`, é o framework reutilizável; `lambdas/`, `github.com/f2e/f2e/lambdas`, contém somente os executáveis de execução. O `go.work` conecta ambos no desenvolvimento local e o módulo de Lambdas declara a dependência explícita do framework.

```text
internal/
  application/
    organizer/               # Planejamento e agendamento de ChunkJob
    worker/                  # Leitura do chunk e publicação de OutputEvent
    port/                    # Dependências requeridas pelos casos de uso
  domain/
    f2e/                     # Mensagens centrais: ChunkJob e OutputEvent
    fixedwidth/              # Leitor técnico do formato fixo do MVP
  adapter/
    inbound/s3event/         # Decodificador do contrato de entrada S3
  platform/
    aws/                     # Clientes/adaptadores concretos S3 e SQS
    config/                  # Leitura e validação das variáveis de ambiente
lambdas/
  cmd/
    organizer/               # Bootstrap da Lambda que planeja os chunks
    worker/                  # Bootstrap da Lambda que processa chunks
  go.mod                     # Módulo de execução
```

## Regras de dependência

```text
lambdas/cmd -> framework/application -> framework/domain
       |                |
       |                +-> framework/application/port <- framework/platform/aws
       +-> framework/platform/config

adapter/inbound/s3event -> domain
```

- `lambdas/cmd` apenas compõe dependências, inicializa clientes reutilizáveis e registra o handler Lambda. É o único código que depende de `aws-lambda-go`.
- `application` contém os casos de uso do blueprint e depende de interfaces em `application/port`, nunca de AWS SDK.
- `domain` contém os contratos e regras técnicas independentes do provedor.
- `adapter` traduz contratos externos para o modelo utilizado pela aplicação.
- `platform` reúne detalhes operacionais e implementações concretas. Eles são conectados nos comandos.

O framework preserva liberdade de refatoração por meio de `internal`; o módulo de execução está abaixo do mesmo prefixo de importação e pode consumir esses pacotes sem expor a API para repositórios externos.
