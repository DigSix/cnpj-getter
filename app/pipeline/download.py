from dataclasses import dataclass
from pathlib import Path
import time
import urllib.request
import urllib.error
import base64
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed

from tqdm import tqdm


# ============================================================
# RESULT
# ============================================================

@dataclass(frozen=True)
class DownloadResult:
    """
    Simple result info for a download operation.
    """
    downloaded: list[Path]
    skipped: list[Path]


# ============================================================
# DOWNLOADER
# ============================================================

class CNPJDownloader:
    """
    Public WebDAV downloader for CNPJ monthly ZIP files.

    Design goals:
    - Idempotent
    - Retry-safe
    - Fail only when the month is provably incomplete
    - Stable against Receita/WebDAV instability
    """

    WEBDAV_BASE = "https://dados-hom.receitafederal.gov.br/public.php/webdav"

    def __init__(
        self,
        public_token: str,
        raw_dir: Path,
        max_workers: int = 2,
    ) -> None:
        self.public_token = public_token
        self.raw_dir = raw_dir
        self.max_workers = max_workers

    # --------------------------------------------------------
    # PUBLIC API
    # --------------------------------------------------------

    def download_month(self, month: str) -> DownloadResult:
        """
        Ensure that ALL ZIP files for a given month are downloaded.
        Will retry the month multiple times until complete or fail hard.
        """
        print(f"[DOWNLOADER] Ensuring complete download for {month}")

        max_rounds = 5
        downloaded_all: list[Path] = []
        skipped_all: list[Path] = []

        for round_num in range(1, max_rounds + 1):
            print(f"[DOWNLOADER] Download round {round_num}/{max_rounds}")

            result = self._download_once(month)
            downloaded_all.extend(result.downloaded)
            skipped_all = result.skipped

            missing = self._find_missing_zips(month)

            if not missing:
                print("[DOWNLOADER] Month download complete")
                return DownloadResult(
                    downloaded=downloaded_all,
                    skipped=skipped_all,
                )

            print(
                f"[DOWNLOADER] Month incomplete: "
                f"{len(missing)} ZIPs missing"
            )
            time.sleep(10)

        raise RuntimeError(
            f"[DOWNLOADER] Failed to fully download month {month} "
            f"after {max_rounds} rounds"
        )

    # --------------------------------------------------------
    # CORE
    # --------------------------------------------------------

    def _download_once(self, month: str) -> DownloadResult:
        print(f"[DOWNLOADER] Processing month {month}")

        month_dir = self.raw_dir / month
        self._ensure_dir(month_dir)

        zip_names = self._list_month_zips(month)

        downloaded: list[Path] = []
        skipped: list[Path] = []
        tasks: list[tuple[str, Path]] = []

        for name in zip_names:
            out_path = month_dir / name

            if out_path.exists() and out_path.stat().st_size > 0:
                skipped.append(out_path)
            else:
                tasks.append((name, out_path))

        if not tasks:
            print("[DOWNLOADER] Nothing to download")
            return DownloadResult(downloaded, skipped)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}

            for name, out_path in tasks:
                url = self._build_file_url(month, name)
                futures[
                    executor.submit(
                        self._download_file,
                        url,
                        out_path,
                    )
                ] = name

            with tqdm(
                total=len(futures),
                desc="Downloading ZIP files",
                unit="file",
            ) as pbar:
                for future in as_completed(futures):
                    name = futures[future]

                    try:
                        future.result()
                        downloaded.append(month_dir / name)
                    except Exception as exc:
                        print(
                            f"[DOWNLOADER] Failed to download "
                            f"{name}: {exc}"
                        )

                    pbar.update(1)

        return DownloadResult(downloaded, skipped)

    # --------------------------------------------------------
    # VALIDATION
    # --------------------------------------------------------

    def _find_missing_zips(self, month: str) -> list[str]:
        """
        Compare expected ZIPs vs files on disk.
        """
        expected = self._list_month_zips(month)
        month_dir = self.raw_dir / month

        missing: list[str] = []

        for name in expected:
            path = month_dir / name
            if not path.exists() or path.stat().st_size == 0:
                missing.append(name)

        return missing

    # --------------------------------------------------------
    # WEBDAV
    # --------------------------------------------------------

    def _list_month_zips(self, month: str, retries: int = 5) -> list[str]:
        """
        List ZIP files available for a given month using WebDAV PROPFIND.
        Highly retry-safe.
        """
        url = f"{self.WEBDAV_BASE}{self._build_month_dir(month)}/"

        headers = {
            "Authorization": self._auth_header(),
            "Depth": "1",
            "User-Agent": "cnpj-getter",
            "Content-Type": "application/xml",
        }

        body = b"""<?xml version="1.0"?>
            <d:propfind xmlns:d="DAV:">
                <d:allprop/>
            </d:propfind>
        """

        for attempt in range(1, retries + 1):
            try:
                req = urllib.request.Request(
                    url,
                    data=body,
                    headers=headers,
                    method="PROPFIND",
                )

                with urllib.request.urlopen(req, timeout=120) as resp:
                    xml_data = resp.read()

                tree = ET.fromstring(xml_data)

                zip_names: list[str] = []

                for elem in tree.findall(".//{DAV:}href"):
                    name = elem.text.rstrip("/").split("/")[-1]

                    if (
                        name.lower().endswith(".zip")
                        and self._is_relevant_zip(name)
                    ):
                        zip_names.append(name)

                if not zip_names:
                    raise RuntimeError("No ZIPs found")

                return zip_names

            except Exception as exc:
                if attempt == retries:
                    raise RuntimeError(
                        f"Failed to list ZIPs for {month}"
                    ) from exc

                wait = 3.0 * (attempt ** 2)
                print(
                    f"[DOWNLOADER] Retry {attempt}/{retries} "
                    f"listing ZIPs for {month} "
                    f"(waiting {wait:.1f}s)"
                )
                time.sleep(wait)

        return []

    # --------------------------------------------------------
    # FILE DOWNLOAD
    # --------------------------------------------------------

    def _download_file(
        self,
        url: str,
        out_path: Path,
        retries: int = 3,
    ) -> None:
        headers = {
            "Authorization": self._auth_header(),
            "User-Agent": "cnpj-getter",
        }

        for attempt in range(1, retries + 1):
            try:
                req = urllib.request.Request(url, headers=headers)

                with urllib.request.urlopen(req, timeout=120) as resp:
                    content_type = resp.headers.get(
                        "Content-Type", ""
                    ).lower()

                    if "zip" not in content_type:
                        raise RuntimeError(
                            f"Expected ZIP, got {content_type}"
                        )

                    total_bytes = resp.headers.get("Content-Length")
                    total = int(total_bytes) if total_bytes else None

                    self._ensure_dir(out_path.parent)

                    with out_path.open("wb") as f, tqdm(
                        total=total,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=out_path.name,
                        leave=False,
                    ) as pbar:
                        while True:
                            chunk = resp.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
                            pbar.update(len(chunk))

                return

            except (
                urllib.error.URLError,
                TimeoutError,
                ConnectionResetError,
                RuntimeError,
            ):
                if out_path.exists():
                    out_path.unlink(missing_ok=True)

                if attempt == retries:
                    raise

                wait = 3.0 * (attempt ** 2)
                print(
                    f"[DOWNLOADER] Retry {attempt}/{retries} "
                    f"for {out_path.name} "
                    f"(waiting {wait:.1f}s)"
                )
                time.sleep(wait)

    # --------------------------------------------------------
    # HELPERS
    # --------------------------------------------------------

    def _build_month_dir(self, month: str) -> str:
        return f"/Dados/Cadastros/CNPJ/{month}"

    def _build_file_url(self, month: str, filename: str) -> str:
        return (
            f"{self.WEBDAV_BASE}"
            f"{self._build_month_dir(month)}/{filename}"
        )

    def _auth_header(self) -> str:
        auth = base64.b64encode(
            f"{self.public_token}:".encode()
        ).decode()
        return f"Basic {auth}"

    def _ensure_dir(self, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)

    def _is_relevant_zip(self, name: str) -> bool:
        name = name.lower()
        return (
            name.startswith("estabelecimentos")
            or name.startswith("empresas")
            or name.startswith("cnae")
            or name.startswith("municipios")
        )
