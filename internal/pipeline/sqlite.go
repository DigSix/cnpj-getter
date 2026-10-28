package pipeline

import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
)

// SQLite is the delivery database: small, portable, easy to copy and safe for
// read-heavy local usage. It never receives the Receita CSVs directly.
func SetupSQLite(path string) error {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS leads (
        cnpj TEXT NOT NULL, razao_social TEXT, nome_fantasia TEXT,
        municipio_nome TEXT, uf TEXT, cnae TEXT, cnae_descricao TEXT,
        porte_empresa TEXT, natureza_juridica TEXT, email TEXT, telefone TEXT,
        data_referencia TEXT NOT NULL, PRIMARY KEY(cnpj, data_referencia));
        CREATE INDEX IF NOT EXISTS idx_leads_filter ON leads(uf, cnae, municipio_nome);`)
	return err
}

func PublishSQLite(p Paths) error {
	if err := SetupSQLite(p.SQLite()); err != nil {
		return err
	}
	duck, err := sql.Open("duckdb", p.DuckDB())
	if err != nil {
		return err
	}
	defer duck.Close()
	sqlite, err := sql.Open("sqlite", p.SQLite())
	if err != nil {
		return err
	}
	defer sqlite.Close()
	rows, err := duck.Query(`SELECT cnpj, razao_social, nome_fantasia, municipio_nome, uf, cnae, cnae_descricao, porte_empresa, natureza_juridica, email, telefone, data_referencia FROM leads WHERE data_referencia = ?`, p.Month)
	if err != nil {
		return err
	}
	defer rows.Close()
	tx, err := sqlite.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT OR REPLACE INTO leads VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	count := 0
	for rows.Next() {
		var v [12]any
		args := make([]any, 12)
		for i := range v {
			args[i] = &v[i]
		}
		if err := rows.Scan(args...); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := stmt.Exec(v[:]...); err != nil {
			tx.Rollback()
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	fmt.Printf("published %d leads to %s\n", count, p.SQLite())
	return nil
}
