from datetime import datetime, timedelta, timezone
import base64
import urllib.request
import xml.etree.ElementTree as ET


class CNPJMonthFinder:
    """
    Finder for CNPJ monthly folders on WebDAV.
    Identifies months updated within a given time window.
    """

    WEBDAV_BASE = "https://dados-hom.receitafederal.gov.br/public.php/webdav"

    def __init__(
        self,
        public_token: str,
        days_window: int = 15,
    ) -> None:
        self.public_token = public_token
        self.days_window = days_window

    # ----------------------------------------------------
    # PUBLIC API
    # ----------------------------------------------------

    def get_updated_months(self) -> list[str]:
        """
        Return list of YYYY-MM months updated within the time window.
        """
        print("[WATCHER] Checking updated months")

        cutoff = self._cutoff_datetime()
        months = self._list_month_folders()

        updated: list[str] = []

        for month, last_modified in months.items():
            if last_modified >= cutoff:
                updated.append(month)

        updated.sort()

        print(f"[WATCHER] {len(updated)} months updated")
        return updated

    # ----------------------------------------------------
    # INTERNALS
    # ----------------------------------------------------

    def _cutoff_datetime(self) -> datetime:
        return (
            datetime.now(timezone.utc)
            - timedelta(days=self.days_window)
        )

    def _auth_header(self) -> str:
        auth = base64.b64encode(f"{self.public_token}:".encode()).decode()
        return f"Basic {auth}"

    def _list_month_folders(self) -> dict[str, datetime]:
        """
        List monthly folders and their getlastmodified timestamps.
        """
        url = f"{self.WEBDAV_BASE}/Dados/Cadastros/CNPJ/"

        headers = {
            "Authorization": self._auth_header(),
            "Depth": "1",
            "User-Agent": "cnpj-getter",
            "Content-Type": "application/xml",
        }

        body = b"""<?xml version="1.0"?>
            <d:propfind xmlns:d="DAV:">
                <d:prop>
                    <d:getlastmodified />
                </d:prop>
            </d:propfind>
        """

        req = urllib.request.Request(
            url,
            data=body,
            headers=headers,
            method="PROPFIND",
        )

        with urllib.request.urlopen(req, timeout=120) as resp:
            xml_data = resp.read()

        tree = ET.fromstring(xml_data)

        months: dict[str, datetime] = {}

        for response in tree.findall("{DAV:}response"):
            href = response.find("{DAV:}href")
            prop = response.find(".//{DAV:}getlastmodified")

            if href is None or prop is None:
                continue

            name = href.text.rstrip("/").split("/")[-1]

            # only YYYY-MM folders
            if not self._is_month_folder(name):
                continue

            last_modified = self._parse_http_datetime(prop.text)
            months[name] = last_modified

        return months

    def _is_month_folder(self, name: str) -> bool:
        if len(name) != 7:
            return False
        return name[4] == "-" and name[:4].isdigit() and name[5:].isdigit()

    def _parse_http_datetime(self, value: str) -> datetime:
        """
        Parse RFC 1123 datetime (WebDAV standard).
        """
        return datetime.strptime(
            value,
            "%a, %d %b %Y %H:%M:%S %Z",
        ).replace(tzinfo=timezone.utc)
