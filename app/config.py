from dataclasses import dataclass
from pathlib import Path
from datetime import date
import os

from dotenv import load_dotenv


# Load variables from .env file
load_dotenv()


def get_default_cnpj_month() -> str:
    """
    Return previous month in YYYY-MM format.
    """
    today: date = date.today()

    year: int = today.year
    month: int = today.month - 1

    if month == 0:
        month = 12
        year -= 1

    return f"{year}-{month:02d}"


@dataclass(frozen=True)
class Settings:
    """
    Application settings loaded from environment variables.
    """
    data_dir: Path
    duckdb_path: Path
    cnpj_month: str
    nextcloud_public_token: str

    # ----------------------------------------------------
    # Derived paths
    # ----------------------------------------------------

    @property
    def raw_dir(self) -> Path:
        """
        Directory to store downloaded ZIP files.
        """
        return self.data_dir / "raw"

    @property
    def extracted_dir(self) -> Path:
        """
        Directory to store extracted CSV files.
        """
        return self.data_dir / "extracted"


def get_settings() -> Settings:
    """
    Read environment variables and return Settings object.
    """
    # Base data directory
    data_dir: Path = Path(
        os.getenv("DATA_DIR", "./data")
    ).resolve()

    # DuckDB file path
    duckdb_path: Path = Path(
        os.getenv("DUCKDB_PATH", str(data_dir / "db/local.duckdb"))
    ).resolve()

    # Target CNPJ month (YYYY-MM)
    cnpj_month: str = os.getenv(
        "CNPJ_MONTH",
        get_default_cnpj_month(),
    )

    # Nextcloud public WebDAV token (required)
    nextcloud_public_token: str = os.getenv("NEXTCLOUD_PUBLIC_TOKEN", "").strip()

    if not nextcloud_public_token:
        raise RuntimeError(
            "NEXTCLOUD_PUBLIC_TOKEN environment variable is required"
        )

    return Settings(
        data_dir=data_dir,
        duckdb_path=duckdb_path,
        cnpj_month=cnpj_month,
        nextcloud_public_token=nextcloud_public_token,
    )
