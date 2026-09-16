# ADR 0005 — Três modos de delimitação

**Status:** aceito em 2026-09-05

## Contexto

O F2E suporta oito tipos de dados (`fixed-width`, `jsonl`, `ndjson`, `csv`,
`binary`, `text`, `multi-line`, `json`). Essa diversidade não traz benefício
proporcional ao custo de manutenção e cria ambiguidade para consumidores:
`jsonl` e `ndjson` são equivalentes; `csv` de uma coluna é `text`; `binary`
requer envelope Base64 incompatível com a maioria dos consumidores; `fixed-width`
exige LF como terminador, tornando-o um subconjunto de `text`.

## Decisão

Manter somente três modos:

| Modo | Registro lógico |
|---|---|
| `text` | Uma linha física terminada por CR, LF ou CRLF. |
| `json` | Um elemento do array selecionado via `arrayPath`, com parsing estrutural. |
| `multi-line` | Linhas físicas agrupadas por marcadores configurados via `MultiLineLayout`. |

### Contratos de terminadores

- CRLF é um único terminador; LFCR são dois terminadores separados.
- O terminador não integra `data.raw`; espaços e conteúdo restante são
  preservados.
- Linha vazia terminada é um registro vazio e válido.
- Terminador final não cria registro extra.
- Arquivo vazio é rejeitado.
- Última linha não vazia sem terminador é erro (exige terminador explícito).
- Esses erros podem ser descobertos após publicação parcial; sem garantia de
  atomicidade do arquivo.

### Modo `text`

Usa o leitor físico compartilhado de CR/LF/CRLF com offsets em bytes e validação
UTF-8. `MaxRecordLengthBytes` é obrigatório para planejamento de chunks. Bytes
UTF-8 inválidos geram erro identificável; não há substituição silenciosa.

### Modo `json`

Preserva o contrato atual de seleção por `arrayPath` com parser estrutural.
Array vazio conclui com zero registros e zero chunks, sem aguardar Worker.
Busca limitada por `F2E_JSON_ARRAY_SEARCH_BYTES` (1 MiB padrão).

### Modo `multi-line`

Reutiliza o leitor físico compartilhado. Marcadores (`BreakMarker`,
`AcceptedPrefixes`, `LineSeparator`) preservados. Cabeçalhos/trailers ignorados
são contados como linhas físicas ignoradas, em métrica separada de registros
lógicos. Terminador de linhas físicas também é obrigatório neste modo.

### Remoção

Tipos removidos: `fixed-width`, `csv`, `binary`, `jsonl`, `ndjson`.
Campo `bypassJsonValidation` / `BypassJSONValidation` removido.
Campo `RecordLengthBytes` de `PrefixConfiguration` removido (substituído por
`MaxRecordLengthBytes` em todos os modos variáveis).
Campos `Fields` e `Base64` de `RecordPayload` removidos.

Nomes antigos não são aceitos silenciosamente como aliases.

### Caminho de migração

| Tipo antigo | Caminho | Perda |
|---|---|---|
| `jsonl` / `ndjson` | Reconfigurar como `text` | Interpretação JSON por linha; validação estrutural |
| `csv` (uma coluna ou delimitador como LF) | Reconfigurar como `text` | Parsing RFC 4180; campos multi-linha; aspas |
| `fixed-width` (com LF) | Reconfigurar como `text` com `MaxRecordLengthBytes` | Nenhuma, se `RecordLengthBytes` já incluía LF |
| `binary` | Sem equivalente | Encapsulamento Base64; contrato de referência S3 deve ser usado pelo chamador |
| `csv` (campos multi-linha) | Sem equivalente | Requer parsing RFC 4180 externo |
| `fixed-width` (sem terminador) | Sem equivalente | Necessita terminador físico |

## Consequências

- O organizer rejeita `DataType` fora do conjunto `{text, json, multi-line}`.
- Jobs anteriores em tipos removidos não são reinterpretados silenciosamente;
  a estratégia de drenagem é declarada na seção de versões de contratos (T01).
- `ADR 0004` fica supersedido por este ADR.
- O leitor compartilhado é criado em T03; a remoção efetiva do código legado
  ocorre em T04.
