# Guia de operação na AWS

Este guia descreve a operação do F2E em uma conta AWS real. A automação usa
Terraform para criar S3, SQS, DLQs, DynamoDB, Lambdas, mapeamentos SQS,
CloudWatch Logs e alarmes. Diferentemente do ambiente local, recursos AWS têm
custo e o estado deve ser mantido em backend remoto antes do uso compartilhado.

> Execute os comandos no **Git Bash**, a partir da raiz do repositório. Use uma
> conta e um ambiente de desenvolvimento isolados para testes E2E.

## Pré-requisitos

Tenha no `PATH`: Git Bash, Go 1.26+, AWS CLI v2, Terraform 1.6+, `jq`, `bc` e
`sha256sum`. O empacotamento das Lambdas usa a biblioteca `archive/zip` do Go;
o executável `zip` não é necessário. Configure credenciais AWS com permissão para os recursos
do Terraform e confirme a identidade antes de qualquer alteração:

```bash
aws sts get-caller-identity
aws configure get region
terraform version
go version
```

Crie a configuração local, que é ignorada pelo Git, e substitua todos os
marcadores pelos valores da conta:

```bash
cp terraform/environments/ALTERAR_aws.local.tfvars.example \
  terraform/environments/aws.local.tfvars
```

Use um bucket S3 globalmente único e `environment = "development"` para a
operação de teste. Para um ambiente E2E descartável, configure também
`s3_force_destroy = true`; assim o destroy remove objetos e versões restantes
do bucket. Para estado compartilhado, configure o backend remoto antes
da primeira subida, conforme [Configuração de ambientes Terraform](../terraform/environments/README.md).
Não compartilhe o mesmo state entre ambientes.

## Subir e manter o ambiente

```bash
./automacao/subir-ambiente-aws.sh
```

O script valida as credenciais, gera os ZIPs das Lambdas, inicializa o
Terraform e aplica o arquivo `aws.local.tfvars`. Ele **não** executa destroy:
o ambiente fica disponível para enviar arquivos e observar o fluxo.

Para usar outro arquivo de variáveis:

```bash
F2E_AWS_TFVARS=/caminho/seguro/meu-ambiente.tfvars \
  ./automacao/subir-ambiente-aws.sh
```

## Executar o fluxo manualmente

Defina atalhos com os nomes reais provisionados. Os valores são lidos do state
Terraform, evitando suposições sobre o prefixo.

```bash
tf='terraform -chdir=terraform'
region="$($tf output -raw aws_region)"
bucket="$($tf output -raw input_bucket)"

aws s3 cp arquivo.txt "s3://${bucket}/ingest/data/arquivo.txt" --region "$region"
```

O Terraform cria o marcador `ingest/.keep` para tornar o prefixo visível no
console S3. O prefixo de processamento padrão é `ingest/data/`, que deve
corresponder a `s3_notification_prefix` do seu tfvars. O upload cria a
notificação S3 e inicia o processamento. Para arquivos
de largura fixa, os registros devem respeitar
`f2e_record_length` configurado no tfvars.

Também é possível publicar o contrato explícito no `file-intake`, porém use uma
chave que não corresponda ao filtro da notificação S3. Nunca use a notificação
e a requisição explícita para o mesmo objeto: isso cria duas solicitações ao
Organizer e duplica o processamento.

## Verificar resultados

```bash
tf='terraform -chdir=terraform'
region="$($tf output -raw aws_region)"

for output in file_intake_queue_url chunk_jobs_queue_url output_events_queue_url; do
  url="$($tf output -raw "$output")"
  aws sqs get-queue-attributes --region "$region" --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --query Attributes --output json
done

aws dynamodb scan --region "$region" \
  --table-name "$($tf output -raw ledger_table_name)" --output json | jq '.Items'
```

Depois de estabilizar, `file-intake` e `chunk-jobs` devem estar vazias. A fila
`output-events` contém um evento por registro até ser consumida. Consulte as
DLQs antes de limpar qualquer mensagem:

```bash
for output in file_intake_dlq_name chunk_jobs_dlq_name; do
  aws sqs get-queue-attributes --region "$region" \
    --queue-url "$(aws sqs get-queue-url --region "$region" --queue-name "$($tf output -raw "$output")" --query QueueUrl --output text)" \
    --attribute-names ApproximateNumberOfMessages --output json
done
```

Para logs, substitua os valores pelo prefixo e ambiente configurados:

```bash
aws logs tail "/aws/lambda/<prefixo>-<ambiente>-organizer" --region "$region" --since 1h
aws logs tail "/aws/lambda/<prefixo>-<ambiente>-worker" --region "$region" --since 1h
```

O dashboard e os alarmes CloudWatch provisionados pelo Terraform permitem
acompanhar backlog, idade da mensagem, erros, throttles, duração e concorrência.

## Testes E2E na AWS

```bash
./automacao/executar-e2e-aws.sh
```

Essa automação é descartável: sobe a infraestrutura, executa a suíte com
`F2E_E2E_TARGET=aws` e sempre chama `parar-ambiente-aws.sh` ao sair, inclusive
quando o teste falha. Por segurança, ela recusa ambiente diferente de
`development`.

## Encerrar o ambiente manualmente

Quando terminar a inspeção, destrua os recursos explicitamente:

```bash
./automacao/parar-ambiente-aws.sh
```

Este comando remove primeiro os objetos, versões e marcadores de exclusão do
bucket de entrada e então executa `terraform destroy -auto-approve` para o
mesmo arquivo de variáveis e backend usados na subida. Confirme conta, região e
state antes de executá-lo. Em caso de falha parcial, corrija a permissão ou
dependência e repita o comando; não remova recursos manualmente do state sem
uma revisão.
