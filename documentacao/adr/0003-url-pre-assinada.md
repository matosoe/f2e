# ADR 0003 — Uso de URL pré-assinada

**Status:** aceito em 2026-09-01

## Decisão

O F2E mantém suporte a URL pré-assinada apenas como credencial transitória de
ingestão. A URL deve usar HTTPS, host S3 previamente permitido, não pode fazer
redirect e nunca é registrada em logs, jobs persistidos ou envelopes.

Quando a URL puder ser resolvida em bucket, key, VersionId e ETag, o pipeline
passa a usar essa identidade. Expiração antes da leitura é falha transitória
classificada e encaminhada para retry/DLQ; não há renovação automática.
