# Benchmark E2E local de 5 milhões de registros

Este teste de carga é intencionalmente apartado da suíte funcional. Ele não é
descoberto por `go test ./...` nem pelo Godog e somente roda quando chamado de
forma explícita.

```bash
./automacao/benchmark-local-5m.sh
```

O harness:

- gera, em streaming, 5.000.000 linhas de exatamente 100 bytes (incluindo LF),
  totalizando 500.000.000 bytes;
- recria o LocalStack descartável e usa o prefixo exclusivo
  `benchmark-text/`;
- mede geração, provisionamento, upload e processamento;
- valida no ledger a leitura/publicação de todos os registros, todos os chunks
  concluídos e ausência de DLQ;
- amostra CPU, memória, rede, disco e PIDs dos containers em
  `docker-resources.csv`;
- grava `report.json` e `run.log` em
  `automacao/resultados/benchmark-local-5m/<run-id>/`.

Por padrão o benchmark usa bundles de até 100 envelopes e 24.000 bytes. O teto
de bytes deixa margem para que dez bundles continuem abaixo do limite agregado
de 256 KiB de uma chamada `SendMessageBatch`. Isso mantém 5 milhões de eventos
lógicos, mas reduz radicalmente a quantidade de mensagens físicas no
SQS/LocalStack. Para comparar com a topologia de uma mensagem por evento:

```bash
BENCHMARK_OUTPUT_MODE=single ./automacao/benchmark-local-5m.sh
```

Esse perfil pode exigir muitas dezenas de GiB de armazenamento temporário no
LocalStack. Os principais parâmetros podem ser variados sem editar o script:

```bash
BENCHMARK_RECORDS_PER_CHUNK=25000 \
BENCHMARK_PUBLISH_CONCURRENCY=4 \
BENCHMARK_MAX_ENVELOPES_PER_MESSAGE=50 \
BENCHMARK_MAX_MESSAGE_BYTES=24000 \
./automacao/benchmark-local-5m.sh
```

Para um ensaio rápido do próprio harness, use por exemplo
`BENCHMARK_RECORDS=10000`. A massa é reutilizada apenas quando o manifesto
confirma quantidade, largura e tamanho esperados.
