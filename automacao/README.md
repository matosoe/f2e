# Automação local

Esta pasta é o destino obrigatório de todos os artefatos usados para criar, executar e validar o ambiente local do F2E.

Os scripts shell desta pasta implementam a operação local descrita em [`documentacao/plano_execucao_f2e.md`](../documentacao/plano_execucao_f2e.md). A estrutura inclui:

- Docker Compose e inicialização idempotente do LocalStack;
- scripts Bash para subir e parar o ambiente;
- `parametros.sh` para definir a quantidade de registros do teste;
- gerador determinístico do arquivo conforme a quantidade configurada;
- execução ponta a ponta por upload S3;
- validação da quantidade configurada de mensagens na fila de saída;
- massas em `automacao/dados` e resultados locais em `automacao/resultados`.

Terraform e scripts de implantação na AWS não pertencem a esta etapa.
