# Operação local

No Git Bash, use `automacao/subir-ambiente.sh` para compilar ZIPs determinísticos,
recriar o LocalStack dedicado e provisionar bucket, cinco filas (duas DLQs),
ledger DynamoDB, Lambdas e mappings SQS. A suíte envia o contrato de intake
explicitamente e, portanto, não habilita uma segunda notificação S3. Use
`executar-fluxo.sh` para gerar e enviar a massa; `validar-fluxo.sh` consome e
verifica a saída. Pare o ambiente com `parar-ambiente.sh`.

Configurações: `F2E_RECORD_LENGTH=100`, `F2E_RECORDS_PER_CHUNK=1000` e
`F2E_BATCH_SIZE=10`. A concorrência é controlada exclusivamente pelo event
source mapping, com um chunk por invocação Worker. No perfil E2E, a visibilidade
é 1800s para chunks e 60s para o intake; o timeout Lambda é 300s e poison
messages usam uma única tentativa para exercitar a DLQ dentro da suíte. Essa
exceção curta do intake existe somente no simulador. O LocalStack não representa
desempenho ou toda a semântica IAM/KMS da AWS.
