# ADR 0010 — Ledger compartilhado e admissão justa por prefixo

## Decisão

Para a PoC, o DynamoDB permanece uma única tabela compartilhada. `prefixId` é
obrigatório para toda admissão em espera e para o contador de quota; os itens
de espera usam `WAITING#<fileId>` e nunca são reutilizados por outro prefixo.
Isso é isolamento lógico e rastreabilidade por prefixo, não isolamento de
segurança: as Lambdas ainda usam uma role compartilhada para a tabela.

Quando `maxActiveJobs` está saturado, o Organizer grava um único item `WAITING`
por versão física do arquivo e confirma a mensagem de entrada. A cada minuto,
o Organizer seleciona os itens mais antigos, no máximo um por prefixo, tenta a
admissão normal e só remove o item depois de publicar os chunks. Se a quota
ainda estiver cheia ou houver erro transitório, ele volta para `WAITING`.

## Consequências

Não há Lambda aguardando vaga e nem consumo repetido que envie arquivos válidos
para a DLQ. A ordem é FIFO dentro de cada prefixo; entre prefixos o agendador
faz progresso independente. Para isolamento forte futuro, a evolução é uma
tabela e role por prefixo, sem mudar a identidade do arquivo ou o protocolo de
eventos.
