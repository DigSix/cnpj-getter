# CNPJ Getter

Pipeline local para baixar, extrair e transformar os dados oficiais de CNPJ da Receita Federal em uma base pronta para prospecção e análise.

O projeto segue três princípios:

- **KISS:** poucos componentes e responsabilidades claras;
- **funcional:** cada etapa recebe uma entrada e produz uma saída verificável;
- **idempotência:** executar novamente não deve duplicar ou corromper dados.

## Fluxo de dados

```text
Receita Federal / WebDAV
          ↓
Go: download e extração
          ↓
DuckDB: leitura, joins e transformação dos CSVs
          ↓
SQLite: leads prontos para consulta e distribuição
```

## Por que duas bases?

**DuckDB** é a camada analítica: lê CSVs grandes diretamente e executa joins,
filtros e agregações sem precisar de um servidor.

**SQLite** é a camada de entrega: guarda apenas os leads consolidados, sendo
portátil, fácil de copiar, fazer backup e consultar em uma aplicação local.

O SQLite não recebe os arquivos brutos. O DuckDB não precisa ser exposto ao
usuário final.

## Estrutura

```text
main.go        CLI e orquestração
internal/pipeline/  fluxo de dados interno
  files.go     download WebDAV e extração dos ZIPs
  duckdb.go    preparação e consolidação dos dados
  sqlite.go    publicação dos leads prontos
  paths.go     caminhos e diretórios do pipeline
data/raw       ZIPs baixados
data/extracted CSVs extraídos
data/db        arquivos DuckDB e SQLite
```

## Requisitos

- Go 1.23 ou superior;
- GCC, necessário para o driver do DuckDB;
- token público configurado em `NEXTCLOUD_PUBLIC_TOKEN`.

## Uso

```bash
go mod tidy
go run . -command info
go run . -command setup
```

Execute o fluxo completo para um mês:

```bash
export NEXTCLOUD_PUBLIC_TOKEN="seu-token"
go run . -command full -month 2026-09
```

As etapas também podem ser executadas separadamente:

```bash
go run . -command download -month 2026-09
go run . -command extract -month 2026-09
go run . -command load -month 2026-09
go run . -command publish -month 2026-09
```

O mês padrão é o mês anterior ao atual. A base entregue fica em
`data/db/leads.sqlite`. A tabela `leads` contém CNPJ, razão social, nome
fantasia, município, UF, CNAE, porte, natureza jurídica, e-mail, telefone e
mês de referência.
