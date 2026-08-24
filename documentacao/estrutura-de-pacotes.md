# Estrutura de pacotes Go

O F2E é uma aplicação serverless composta por dois binários. Por isso, mantém os pontos de entrada em `cmd` e todo o código de implementação em `internal`. Essa decisão segue a recomendação oficial para projetos Go com múltiplos comandos e evita expor uma API de framework antes de ela estar estável.

```text
cmd/
  organizer/                 # Bootstrap da Lambda que planeja os chunks
  worker/                    # Bootstrap da Lambda que processa chunks
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
```

## Regras de dependência

```text
cmd -> application -> domain
  |         |
  |         +-> application/port <- platform/aws
  +-> platform/config

adapter/inbound/s3event -> domain
```

- `cmd` apenas compõe dependências, inicializa clientes reutilizáveis e registra o handler Lambda.
- `application` contém os casos de uso do blueprint e depende de interfaces em `application/port`, nunca de AWS SDK.
- `domain` contém os contratos e regras técnicas independentes do provedor.
- `adapter` traduz contratos externos para o modelo utilizado pela aplicação.
- `platform` reúne detalhes operacionais e implementações concretas. Eles são conectados nos comandos.

Quando o F2E tiver uma API estável para outros repositórios, ela deve ser extraída para um módulo Go independente. Até lá, `internal` preserva liberdade de refatoração.
