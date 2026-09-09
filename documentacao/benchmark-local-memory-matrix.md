# Benchmark local — matriz memória × CPU

Esta automação é deliberadamente separada de `go test`, Godog e da suíte E2E
oficial. Ela executa, para cada perfil, primeiro um smoke de 10.000 linhas e
somente executa a carga completa de 5.000.000 de linhas se o smoke concluir
com todas as validações do ledger e das DLQs.

```bash
./automacao/benchmark-local-memory-matrix.sh
```

A matriz padrão aproxima a CPU proporcional da AWS Lambda: 128/256/512/1.024
MiB com 0,07/0,14/0,29/0,58 vCPU. O `benchmark-local-5m.sh` pré-aquece o
container Lambda do LocalStack e aplica `docker update --cpus` e `--memory`
ao container do Worker; qualquer novo container Lambda é detectado durante a
carga e recebe os mesmos limites. O arquivo
`worker-container-limits.csv` prova os limites aplicados.

Por padrão, os bundles usam o teto de 250 KiB, sem teto por quantidade de
envelopes. A implementação incremental do packer torna essa configuração
viável para medir publicação, em vez de reserializar o bundle crescente.
O timeout de execução do Worker é 600 segundos (10 minutos); o prazo global
de cada carga completa permanece em até duas horas.

Os resultados ficam em
`automacao/resultados/benchmark-local-memory-matrix/<run-id>/`, com um
`report.json` por etapa e `summary.json` consolidado.

## Personalização

```bash
BENCHMARK_LOCAL_MEMORY_CPU_MATRIX='256:0.14 512:0.29' \
BENCHMARK_LOCAL_PUBLISH_CONCURRENCY=4 \
BENCHMARK_LOCAL_RECORDS_PER_CHUNK=50000 \
./automacao/benchmark-local-memory-matrix.sh
```

O Docker Desktop é um simulador local: `--cpus` é uma quota rígida de cgroup,
enquanto a Lambda atribui CPU proporcionalmente à memória. Logo, a matriz é
adequada para comparar regressões e tendências relativas; a decisão de
produção continua exigindo validação na AWS.
