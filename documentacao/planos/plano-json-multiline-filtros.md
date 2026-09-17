# Plano de execução — JSON array por marcador e multi-line por campos

## Objetivo e regras de execução

Implementar os dois itens abaixo em sequência. Este arquivo é uma instrução para o agente executor; a implementação ainda não foi feita. Seguir `AGENTS.md`: executar comandos do projeto exclusivamente com `C:\Program Files\Git\bin\bash.exe`.

Para **cada item**, criar ou ajustar testes unitários e cenários E2E que comprovem o comportamento. Depois de implementar cada item, executar os testes unitários e corrigir qualquer falha antes de prosseguir. Ao concluir **todos** os itens, executar a suíte E2E completa, corrigir falhas e repetir as suítes afetadas até que todos os testes unitários e E2E passem. Registrar comandos e resultados no relatório final. Não considerar um teste ignorado, indisponível ou interrompido como aprovado.

## 1. JSON array sem leitura do Organizer

### Contrato

1. Remover `jsonArraySearchBytes` e equivalentes (`JSONArraySearchBytes`, `maxJsonArraySearchBytes`, `MaxJSONArraySearchBytes`, `F2E_JSON_ARRAY_SEARCH_BYTES`, variável Terraform) de **todo o projeto**: modelos, configuração, limites globais, validação, SSM, Terraform, LocalStack, fixtures, testes e documentação. Remover também a busca inicial do array feita pelo Organizer (`findJSONArrayOffset` e navegação associada) e o campo de job `jsonArrayOffset` se não houver outro uso legítimo. O Organizer deve planejar os chunks apenas com metadados do objeto e limites configurados, sem `GetRange` do conteúdo JSON; investigar e retirar também leituras de padding JSON no planejamento.
2. Acrescentar em `jsonArrayLayout` um campo obrigatório **somente para `dataType=json`**, por exemplo `firstFieldName`, contendo o nome da primeira propriedade de **cada objeto** do array selecionado. Validar nome não vazio e `maxBytesPerElement` positivo. O formato de cada elemento passa a ser objeto JSON; documentar a incompatibilidade com arrays de primitivos e com objetos cujo primeiro campo varie.
3. O arquivo JSON precisa ser padronizado: todos os elementos do array têm o mesmo primeiro campo, na primeira posição; esse nome não pode aparecer fora do array selecionado nem dentro de um elemento (inclusive em objetos aninhados). Tratar a unicidade global como **pré-condição do produtor**, pois o Organizer não percorrerá o arquivo inteiro para comprová-la. Delimitar com precisão o escopo de `arrayPath` e preservar a seleção configurada se ela continuar necessária; retirar `arrayPath` somente se a nova leitura tornar seu significado impossível ou redundante, com migração explícita de contrato.
4. Cada Worker deve descobrir autonomamente o início e o fim dos elementos em sua faixa usando leitura S3 por range e o marcador configurado, inclusive se o chunk começa no meio de um objeto ou de uma string. Detectar **chaves JSON reais**, respeitando aspas, escapes, UTF-8 e estrutura, sem confundir ocorrências do nome em valores de string. Um item pertence exatamente ao chunk que contém seu byte inicial; permitir leitura limitada à frente/atrás pelo `maxBytesPerElement` para completar itens nas fronteiras. Não duplicar nem perder registros. Definir e testar comportamento para arquivo malformado, elemento maior que o limite e marcador ausente. Revisar o dimensionamento de ranges e `maxChunkBytes` para manter os limites configurados.
5. Preservar conclusão correta do array vazio com zero eventos e sem depender da leitura do Organizer. Ajustar a contabilização de chunks/ledger e o fluxo de conclusão conforme a nova estratégia. Conferir offsets e IDs estáveis usados em `Envelope` e replay.

### Locais a revisar

`internal/domain/f2e/messages.go`, `internal/domain/f2e/json.go`, `internal/application/organizer/service.go`, `internal/application/organizer/validate.go`, `internal/application/worker/service.go`, `internal/platform/config`, `cmd/organizer`, `terraform/locals.tf`, `automacao/localstack/init-aws.sh`, `e2e/internal`, `documentacao/schemas/fixtures`, `README.md`, `documentacao/contratos.md`, `documentacao/requisitos_e_restricoes.md` e `documentacao/adr/0005-tres-modos-de-delimitacao.md`. Fazer busca global pelos nomes antigos antes de encerrar; atualizar outras referências normativas encontradas. ADRs supersedidas podem manter histórico, mas devem apontar claramente para a decisão vigente quando necessário.

### Testes de aceite do item

- Unitários: validação do novo campo apenas em JSON; partições em vários chunks (início, meio e fim de elemento); chave semelhante em strings e objetos aninhados; aspas/escapes; whitespace e CR/LF; objeto acima de `maxBytesPerElement`; array vazio; JSON inválido; nenhuma leitura do corpo pelo Organizer no caminho JSON.
- E2E em `e2e/features/json_array.feature`: arquivo com vários chunks e primeiro campo padronizado, conferindo contagem, payload, unicidade, offsets/IDs e conclusão; array vazio; cenário de rejeição de formato inválido detectável localmente. Atualizar geradores, tipos e fixtures para `firstFieldName`.
- Executar `go test ./...` na raiz após este item e corrigir até passar.

## 2. Multi-line com campos de filtro por posição

### Contrato

1. Substituir `breakPosition`, `breakMarker` e `acceptedPrefixes` por três conjuntos de campos em `multiLineLayout`: **quebra de registro**, **inclusão/junção ao registro atual** e **linha ignorada**. Dar nomes JSON/Go claros e consistentes. O conjunto de quebra é obrigatório; inclusão e ignorar podem ser omitidos. Cada conjunto configurado contém de **1 a 99 campos**. Cada campo contém `startByte` (offset zero-based), `lengthBytes` (> 0) e `value` (string cuja representação em bytes deve ter exatamente `lengthBytes`). Comparar por bytes, sem trim, conversão de caracteres ou pressuposto de prefixo. Validar offsets não negativos, tamanho, valor e limite de 99 na entrada explícita, SSM e job.
2. Uma linha corresponde a um conjunto **somente se todos os seus campos corresponderem** (AND). A correspondência permite posições arbitrárias e vários trechos da linha. Se algum campo ultrapassar o fim da linha física, o conjunto simplesmente não corresponde; a linha é descartada quando nenhum conjunto aplicável corresponde, sem erro. A regra deve valer igualmente no Worker e na busca de fronteiras do Organizer.
3. Definir precedência determinística quando conjuntos coincidirem: quebra primeiro, depois inclusão, depois ignorar. A quebra inicia novo registro; inclusão só junta quando já existe registro aberto; ignorar descarta a linha. Linhas sem correspondência também são descartadas e contabilizadas nas métricas de ignoradas, preservando a distinção possível entre cabeçalho e trailer. Manter `lineSeparator`, `maxBytesPerRecord`, terminadores CR/LF/CRLF e limites de chunk existentes.
4. No planejamento, trocar `nextBreakOffset` para usar exatamente o mesmo avaliador de campos usado pelo leitor multi-line, evitando divergência nas fronteiras de chunk. Garantir que uma linha curta no lookahead não cause erro nem uma quebra falsa, e que um registro não seja dividido nem emitido duas vezes.
5. Migrar todos os contratos de configuração e exemplos: `PrefixConfiguration`, `FileRequest`, `ChunkJob`, validação, catálogo Terraform, inicialização LocalStack, fixtures de schema, geradores E2E, `README.md`, `documentacao/contratos.md`, `documentacao/requisitos_e_restricoes.md` e ADR 0005. Documentar as novas regras, o máximo de 99 campos **por conjunto**, a indexação em bytes, a conjunção e o tratamento de linha curta. Remover os campos antigos do uso ativo em todo o projeto.

### Testes de aceite do item

- Unitários: conjunto com 1 campo e com vários campos; filtros em posições não iniciais; falha de qualquer campo; comprimento que ultrapassa linha curta; UTF-8 com offset em bytes; limites de 0, 99 e 100 campos; empate entre conjuntos; linha ignorada explícita e sem correspondência; CR/LF/CRLF; cabeçalho/trailer; fronteiras entre chunks, sem perda/duplicação. Testar validação no Organizer e no Worker.
- E2E em `e2e/features/multiline.feature`: arquivo com linhas de cabeçalho, detalhe, continuação e trailer; filtros de múltiplos campos em posições diferentes; linhas curtas descartadas; múltiplos chunks; conferir composição e ordem do `data.raw`, contagem, unicidade e conclusão. Atualizar tipos, geradores e fixtures.
- Executar `go test ./...` na raiz após este item e corrigir até passar.

## Fechamento obrigatório

1. Fazer busca global por nomes retirados e eliminar referências **ativas** restantes em código, infraestrutura, fixtures, README e documentação vigente. Atualizar ADR 0005 (ou criar ADR nova que a superseda explicitamente) com os dois contratos finais e as consequências de migração. Não apresentar a regra antiga como vigente.
2. Executar novamente `go test ./...` na raiz.
3. Preparar o ambiente local conforme `documentacao/operacao_local.md` e executar a suíte E2E completa do módulo separado: `cd e2e && go test -v -timeout 25m ./...`. Se houver falha, corrigir, repetir os testes unitários afetados e a suíte E2E completa até tudo passar. Se a suíte exigir serviços indisponíveis, registrar o impedimento concreto e não declarar aprovação.
4. Conferir `git diff --check` e relatar arquivos alterados, decisões de contrato/migração e resultados reais dos testes.
