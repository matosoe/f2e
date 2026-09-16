# Migração da topologia legada para prefixos

Esta mudança remove da raiz Terraform a fila compartilhada `chunk-jobs`, sua
DLQ, a fila `output-events` e o Worker compartilhado. Antes de aplicar em uma
conta com tráfego, faça a migração por prefixo em uma janela controlada.

1. Inventarie filas, mappings e Workers ativos e execute o E2E de cada prefixo.
2. Crie os módulos por prefixo com um `terraform apply` que não remova recursos
   ativos; compare o plano e interrompa se houver destruição inesperada.
3. Para um prefixo que já usava o recurso legado, mova apenas o estado do
   Worker compatível depois de confirmar nomes, código e configuração:

   ```bash
   terraform state mv aws_lambda_function.worker 'module.prefix["example-text"].aws_lambda_function.worker'
   ```

4. Drene a fila compartilhada e suas DLQs. Não mova uma fila compartilhada para
   um prefixo: ela não preserva o isolamento. Confirme filas e DLQs novas vazias,
   IDs estáveis em retry e a entrega de conclusão recuperável.
5. Aplique a remoção da topologia legada somente após o plano mostrar que os
   recursos compartilhados estão vazios ou já foram desativados.

O catálogo em `terraform/locals.tf` é a única fonte de prefixos provisionados;
o JSON SSM de cada um é derivado dele e sempre recebe URLs do mesmo módulo.
