# ADR 0011 — Catálogo Terraform como fonte de prefixos provisionados

**Status:** supersedido em 2026-09-16 pelo [ADR 0013](0013-implantacao-simples-compartilhada.md)

## Contexto

O isolamento definido no ADR 0008 exige que fila de chunks, DLQ, Worker e fila
de saída pertençam ao mesmo prefixo. A definição desse prefixo estava dividida
entre `locals.tf`, variáveis de Worker, módulo Terraform e documentos SSM, o
que permitia configurar no SSM uma URL que não pertencesse ao prefixo atendido.
A raiz Terraform ainda mantinha uma topologia compartilhada legada, tornando o
plano de operação ambíguo.

## Decisão

- `local.prefix_catalog` é a fonte de verdade dos prefixos provisionados, do
  contrato de arquivo e da capacidade do Worker;
- o módulo `f2e-prefix` é instanciado com `for_each` desse catálogo;
- cada parâmetro SSM de configuração de arquivo é derivado da mesma entrada e
  recebe `chunkQueueURL` e `outputQueueURL` produzidas pelo módulo homônimo;
- a raiz mantém somente recursos realmente compartilhados: intake, Organizer,
  ledger, limites globais e publicação de conclusão;
- as filas compartilhadas legadas de chunks/saída e o Worker compartilhado não
  fazem parte da topologia desejada.

O SSM continua sendo a fonte dinâmica lida pelo Organizer. O catálogo não
substitui o snapshot de configuração na admissão e não permite uma URL externa
para um prefixo Terraform-gerenciado.

## Consequências

- O plano Terraform expõe uma relação direta entre prefixo, Worker e filas;
  outputs e alarmes são mapas por prefixo.
- Alterações de topologia e capacidade passam por revisão Terraform; campos de
  contrato que não alteram recursos continuam disponíveis no SSM.
- A migração exige drenagem e revisão de plano antes de remover recursos vivos;
  uma fila compartilhada nunca é movida para um prefixo porque isso não cria
  isolamento. O procedimento operacional está em
  [`migracao-topologia-prefixos.md`](../runbooks/migracao-topologia-prefixos.md).
- Esta decisão concretiza a seção Terraform do ADR 0008 e não altera as regras
  de quota, ledger, retries, DLQs ou outbox de conclusão.
