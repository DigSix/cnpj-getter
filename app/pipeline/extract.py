from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile, BadZipFile


# ============================================================
# RESULT
# ============================================================

@dataclass(frozen=True)
class ExtractResult:
    """
    Simple result info for an extract operation.
    """
    extracted_files: list[Path]
    skipped_files: list[Path]
    failed_files: list[Path]


# ============================================================
# EXTRACTOR
# ============================================================

class CNPJExtractor:
    """
    Extract ZIP files downloaded from CNPJ WebDAV.
    Fail-soft: invalid ZIPs are skipped and removed.
    """

    def __init__(
        self,
        raw_dir: Path,
        extracted_dir: Path,
    ) -> None:
        self.raw_dir = raw_dir
        self.extracted_dir = extracted_dir

    # --------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------

    def extract_month(self, month: str) -> ExtractResult:
        """
        Extract all ZIP files for a given month.
        """
        print(f"[EXTRACT] Extracting files for {month}")

        raw_month_dir = self.raw_dir / month
        extracted_month_dir = self.extracted_dir / month

        self._ensure_dir(extracted_month_dir)

        extracted: list[Path] = []
        skipped: list[Path] = []
        failed: list[Path] = []

        zip_paths = sorted(raw_month_dir.glob("*.zip"))

        if not zip_paths:
            print("[EXTRACT] No ZIP files found")
            return ExtractResult(extracted, skipped, failed)

        for zip_path in zip_paths:
            try:
                with ZipFile(zip_path, "r") as zf:
                    zf.extractall(extracted_month_dir)

                    for name in zf.namelist():
                        extracted.append(extracted_month_dir / name)

            except BadZipFile:
                print(f"[EXTRACT] Invalid ZIP removed: {zip_path.name}")
                zip_path.unlink(missing_ok=True)
                failed.append(zip_path)

            except Exception as exc:
                print(f"[EXTRACT] Failed to extract {zip_path.name}: {exc}")
                failed.append(zip_path)

        print(
            f"[EXTRACT] Completed: "
            f"{len(extracted)} files extracted, "
            f"{len(failed)} ZIPs failed"
        )

        return ExtractResult(
            extracted_files=extracted,
            skipped_files=skipped,
            failed_files=failed,
        )

    # --------------------------------------------------------
    # INTERNALS
    # --------------------------------------------------------

    def _ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
