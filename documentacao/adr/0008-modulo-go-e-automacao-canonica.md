# ADR 0008 — Módulo Go único e automação canônica

**Status:** aceito em 2026-09-16

## Contexto

As entradas Lambda estavam em um segundo módulo Go ligado ao módulo principal
por `go.work` e `replace` local. Isso exigia módulos, somas e comandos de teste
separados, embora Organizer e Worker dependam do mesmo core. A automação também expunha diversos scripts próximos para as mesmas
operações locais, AWS e de benchmark.

## Decisão

- As entradas Lambda ficam em `cmd/organizer` e `cmd/worker`, no módulo Go raiz.
- Um único `go test ./...` e `automacao/build-lambdas.sh` cobrem core, comandos
  e empacotamento dos dois binários.
- As entradas de operação são `automacao/ambiente local up|down|run|test`,
  `automacao/ambiente aws up|down|e2e` e
  `automacao/benchmark <perfil> <alvo>`.
- Scripts com nomes anteriores podem permanecer como wrappers transitórios
  enquanto chamadas existentes forem migradas.

## Consequências

- Dependências e versões são resolvidas uma única vez, sem `replace` local.
- Caminhos dos binários implantados, portas de entrada, contratos de eventos e
  os dois executáveis Lambda permanecem inalterados.
- A automação reduz a superfície pública sem remover LocalStack, AWS, E2E ou
  benchmarks; novos fluxos devem usar as entradas canônicas.
