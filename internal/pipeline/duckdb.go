package pipeline

import (
	"database/sql"
	"fmt"
	"path/filepath"
)

// DuckDB reads the Receita files in-place. This avoids copying tens of GB into
// a staging database and keeps the transformation declarative and inspectable.
func LoadDuckDB(p Paths) error {
	db, err := sql.Open("duckdb", p.DuckDB())
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS leads (cnpj VARCHAR, razao_social VARCHAR, nome_fantasia VARCHAR, municipio_nome VARCHAR, uf VARCHAR, cnae VARCHAR, cnae_descricao VARCHAR, porte_empresa VARCHAR, natureza_juridica VARCHAR, email VARCHAR, telefone VARCHAR, data_referencia VARCHAR, PRIMARY KEY(cnpj, data_referencia))`); err != nil {
		return err
	}
	base := filepath.ToSlash(p.Extracted())
	query := fmt.Sprintf(`CREATE OR REPLACE TEMP TABLE estabelecimentos AS SELECT * FROM read_csv('%s/*.ESTABELE', delim=';', header=false, ignore_errors=true, columns={cnpj_basico:'VARCHAR',cnpj_ordem:'VARCHAR',cnpj_dv:'VARCHAR',identificador:'VARCHAR',nome_fantasia:'VARCHAR',situacao:'VARCHAR',data_situacao:'VARCHAR',motivo:'VARCHAR',cidade_ext:'VARCHAR',pais:'VARCHAR',inicio:'VARCHAR',cnae:'VARCHAR',secundaria:'VARCHAR',tipo:'VARCHAR',logradouro:'VARCHAR',numero:'VARCHAR',complemento:'VARCHAR',bairro:'VARCHAR',cep:'VARCHAR',uf:'VARCHAR',municipio:'VARCHAR',ddd:'VARCHAR',telefone:'VARCHAR',ddd2:'VARCHAR',telefone2:'VARCHAR',dddfax:'VARCHAR',fax:'VARCHAR',email:'VARCHAR',especial:'VARCHAR',data_especial:'VARCHAR'}); CREATE OR REPLACE TEMP TABLE empresas AS SELECT * FROM read_csv('%s/*.EMPRECSV', delim=';', header=false, ignore_errors=true, columns={cnpj_basico:'VARCHAR',razao_social:'VARCHAR',natureza_juridica:'VARCHAR',qualificacao:'VARCHAR',capital:'VARCHAR',porte_empresa:'VARCHAR',ente:'VARCHAR'});`, base, base)
	if _, err = db.Exec(query); err != nil {
		return err
	}
	_, err = db.Exec(`INSERT OR REPLACE INTO leads SELECT e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv, MAX(emp.razao_social), MAX(e.nome_fantasia), MAX(e.municipio), MAX(e.uf), MAX(e.cnae), NULL, MAX(emp.porte_empresa), MAX(emp.natureza_juridica), MAX(e.email), MAX(e.ddd || e.telefone), ? FROM estabelecimentos e LEFT JOIN empresas emp ON emp.cnpj_basico=e.cnpj_basico WHERE e.situacao='02' GROUP BY e.cnpj_basico,e.cnpj_ordem,e.cnpj_dv`, p.Month)
	return err
}
