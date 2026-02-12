import sys
from pathlib import Path

from app.config import get_settings
from app.pipeline.download import CNPJDownloader
from app.pipeline.extract import CNPJExtractor
from app.pipeline.warehouse import CNPJWarehouse
from app.orchestrator.find import CNPJMonthFinder


def main() -> None:
    settings = get_settings()

    # -------------------------
    # CLI parsing
    # -------------------------
    cmd: str = sys.argv[1] if len(sys.argv) > 1 else "info"

    raw_dir: Path = settings.data_dir / "raw"
    extracted_dir: Path = settings.data_dir / "extracted"
    duckdb_path: Path = settings.duckdb_path

    # -------------------------
    # Pipeline components
    # -------------------------
    downloader = CNPJDownloader(
        public_token=settings.nextcloud_public_token,
        raw_dir=raw_dir,
    )

    extractor = CNPJExtractor(
        raw_dir=raw_dir,
        extracted_dir=extracted_dir,
    )

    warehouse = CNPJWarehouse(
        duckdb_path=duckdb_path,
        extracted_dir=extracted_dir,
    )

    finder = CNPJMonthFinder(
        public_token=settings.nextcloud_public_token,
    )

    # -------------------------
    # Commands
    # -------------------------
    if cmd == "info":
        print(settings)
        return

    if cmd == "setup":
        print(duckdb_path)
        warehouse.setup()
        return

    # ---------------------------------
    # Full pipeline requires schema
    # ---------------------------------
    if cmd == "full":
        print("[MAIN] Running setup")
        warehouse.setup()

    # ---------------------------------
    # Finder decides which months exist
    # ---------------------------------
    months = finder.get_updated_months()

    if not months:
        print("[MAIN] No updated months found")
        return

    print(f"[MAIN] Months to process: {months}")

    # -------------------------
    # Execute pipeline per month
    # -------------------------
    for month in months:
        print(f"[MAIN] Processing month {month}")

        if cmd == "download":
            result = downloader.download_month(month)
            print(f"Downloaded: {len(result.downloaded)}")
            print(f"Skipped: {len(result.skipped)}")

        elif cmd == "extract":
            result = extractor.extract_month(month)
            print(f"Extracted files: {len(result.extracted_files)}")

        elif cmd == "load":
            warehouse.load_raw(month)
            warehouse.load_dim(month)
            warehouse.build_leads(month)

        elif cmd == "full":
            downloader.download_month(month)
            extractor.extract_month(month)

            warehouse.load_raw(month)
            warehouse.load_dim(month)
            warehouse.build_leads(month)

        else:
            raise SystemExit(f"Unknown command: {cmd}")

    print("[MAIN] Done")


if __name__ == "__main__":
    main()
