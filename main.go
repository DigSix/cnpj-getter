package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cnpj-getter/internal/pipeline"
)

// The executable intentionally keeps orchestration small. Each command has one job:
// download/extract prepare files; load builds the analytical DuckDB layer; publish
// copies the useful, stable result to SQLite.
func main() {
	log.SetFlags(log.LstdFlags)
	dataDir := getenv("DATA_DIR", "./data")
	month := flag.String("month", previousMonth(), "CNPJ reference month (YYYY-MM)")
	command := flag.String("command", "info", "info|setup|download|extract|load|publish|full")
	flag.Parse()

	if *command == "info" {
		fmt.Printf("command=%s data_dir=%s month=%s\n", *command, dataDir, *month)
		return
	}
	if *command != "setup" && os.Getenv("NEXTCLOUD_PUBLIC_TOKEN") == "" {
		log.Fatal("NEXTCLOUD_PUBLIC_TOKEN is required")
	}

	paths := pipeline.Paths{Data: dataDir, Month: *month}
	ctx := context.Background()
	switch *command {
	case "setup":
		if err := paths.Ensure(); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.SetupSQLite(paths.SQLite()); err != nil {
			log.Fatal(err)
		}
	case "download":
		if err := pipeline.DownloadMonth(ctx, paths, os.Getenv("NEXTCLOUD_PUBLIC_TOKEN")); err != nil {
			log.Fatal(err)
		}
	case "extract":
		if err := pipeline.ExtractMonth(paths); err != nil {
			log.Fatal(err)
		}
	case "load":
		if err := pipeline.LoadDuckDB(paths); err != nil {
			log.Fatal(err)
		}
	case "publish":
		if err := pipeline.PublishSQLite(paths); err != nil {
			log.Fatal(err)
		}
	case "full":
		if err := paths.Ensure(); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.SetupSQLite(paths.SQLite()); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.DownloadMonth(ctx, paths, os.Getenv("NEXTCLOUD_PUBLIC_TOKEN")); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.ExtractMonth(paths); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.LoadDuckDB(paths); err != nil {
			log.Fatal(err)
		}
		if err := pipeline.PublishSQLite(paths); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown command %q", *command)
	}
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
func previousMonth() string { t := time.Now().AddDate(0, -1, 0); return t.Format("2006-01") }
