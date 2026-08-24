# Operação local

No Git Bash, use `automacao/subir-ambiente.sh` para compilar os binários Linux, iniciar o LocalStack e provisionar bucket, seis filas (três DLQs), notificações S3, Lambdas e mappings SQS. Use `executar-fluxo.sh` para gerar e enviar a massa; `validar-fluxo.sh` consome e verifica a saída. Pare o ambiente com `parar-ambiente.sh`.

Configurações: `F2E_RECORD_LENGTH=100`, `F2E_RECORDS_PER_CHUNK=1000`, `F2E_BATCH_SIZE=10`, `F2E_WORKER_CONCURRENCY=4`. A visibility timeout é 90s e as Lambdas têm timeout de 60s. O LocalStack é uma simulação: não representa IAM, desempenho ou semântica integral da AWS; Terraform e deploy real estão fora do MVP.
