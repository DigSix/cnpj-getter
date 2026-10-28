package pipeline

import (
	"os"
	"path/filepath"
)

type Paths struct{ Data, Month string }

func (p Paths) Raw() string       { return filepath.Join(p.Data, "raw", p.Month) }
func (p Paths) Extracted() string { return filepath.Join(p.Data, "extracted", p.Month) }
func (p Paths) DuckDB() string    { return filepath.Join(p.Data, "db", "warehouse.duckdb") }
func (p Paths) SQLite() string    { return filepath.Join(p.Data, "db", "leads.sqlite") }
func (p Paths) Ensure() error {
	for _, dir := range []string{p.Raw(), p.Extracted(), filepath.Dir(p.DuckDB())} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}
