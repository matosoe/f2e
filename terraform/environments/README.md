# Configuração de ambientes Terraform

Ambientes suportados: `local`, `development`, `staging` e `production`.

O módulo mantém backend local para permitir o gate LocalStack. Produção deve
inicializar uma cópia/wrapper corporativo com o conteúdo de
`backend.tf.example`, habilitando S3 criptografado e lockfile nativo. Bucket,
key, região e KMS nunca são inferidos nem versionados com credenciais.

```bash
cp terraform/environments/backend.tf.example terraform/backend.tf
terraform -chdir=terraform init
terraform -chdir=terraform plan -var-file=environments/production.tfvars
```

Produção exige uma chave KMS corporativa, nomes/tags aprovados, conta/região
corretas e revisão do plano. `local.tfvars` é exclusivo para LocalStack.
