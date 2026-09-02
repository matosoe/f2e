# Política de supply chain

- Go usa a última revisão de segurança aprovada da série declarada nos módulos.
- Dependências e providers são revisados mensalmente e em alertas críticos.
- `go mod verify`, `govulncheck`, secret scanning e scanner IaC bloqueiam PRs.
- Lambdas são compiladas com `-trimpath`, recebem checksum e SBOM no pipeline.
- `SHA256SUMS` cobre binários e ZIPs; o CI assina e verifica esse manifesto com
  Sigstore/Cosign keyless e publica o bundle de transparência junto do SBOM.
- Imagens controladas e GitHub Actions são fixadas por digest/SHA.
- Artefatos são publicados uma vez em repositório imutável; ambientes promovem
  o mesmo checksum, sem recompilação.
- Dependabot abre atualizações mensais separadas para módulos Go, Terraform,
  Docker e Actions; mudanças continuam sujeitas a todos os gates.
