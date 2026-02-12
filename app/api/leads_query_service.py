from pathlib import Path
import duckdb


class LeadsQueryService:
    """
    Query service responsible for reading data from leads table.
    Read-only access layer for API consumption.
    """

    def __init__(self, duckdb_path: Path) -> None:
        self.duckdb_path = duckdb_path

    # ============================================================
    # INTERNAL
    # ============================================================

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.duckdb_path))

    def _fetch_all(
        self,
        query: str,
        params: list | None = None,
    ) -> list[dict]:
        conn = self._connect()

        try:
            result = conn.execute(query, params or [])
            columns = [col[0] for col in result.description]
            rows = result.fetchall()

            return [
                dict(zip(columns, row))
                for row in rows
            ]
        finally:
            conn.close()

    # ============================================================
    # PUBLIC API
    # ============================================================

    def list(
        self,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        List leads with pagination.
        """
        print("[QUERY] list leads")

        query = """
            SELECT *
            FROM leads
            ORDER BY cnpj
            LIMIT ?
            OFFSET ?
        """

        return self._fetch_all(query, [limit, offset])

    def get_by_cnpj(
        self,
        cnpj: str,
        data_referencia: str,
    ) -> dict | None:
        """
        Get a single lead by CNPJ and reference month.
        """
        print(f"[QUERY] get lead {cnpj} ({data_referencia})")

        query = """
            SELECT *
            FROM leads
            WHERE cnpj = ?
              AND data_referencia = ?
            LIMIT 1
        """

        results = self._fetch_all(query, [cnpj, data_referencia])

        return results[0] if results else None

    def by_uf(
        self,
        uf: str,
        data_referencia: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Filter leads by UF.
        """
        print(f"[QUERY] filter by uf={uf}")

        query = """
            SELECT *
            FROM leads
            WHERE uf = ?
              AND data_referencia = ?
            ORDER BY cnpj
            LIMIT ?
            OFFSET ?
        """

        return self._fetch_all(
            query,
            [uf, data_referencia, limit, offset],
        )

    def by_cnae(
        self,
        cnae: str,
        data_referencia: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Filter leads by CNAE principal.
        """
        print(f"[QUERY] filter by cnae={cnae}")

        query = """
            SELECT *
            FROM leads
            WHERE cnae = ?
              AND data_referencia = ?
            ORDER BY cnpj
            LIMIT ?
            OFFSET ?
        """

        return self._fetch_all(
            query,
            [cnae, data_referencia, limit, offset],
        )
    
    def by_cnae_municipio_uf(
        self,
        cnae: str,
        municipio: str,
        uf: str,
        data_referencia: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        """
        Filter leads by CNAE, municipio name and UF.
        """

        print(
            f"[QUERY] filter by "
            f"cnae={cnae}, municipio={municipio}, uf={uf}"
        )

        query = """
            SELECT *
            FROM leads
            WHERE cnae = ?
              AND municipio_nome = ?
              AND uf = ?
              AND data_referencia = ?
            ORDER BY cnpj
            LIMIT ?
            OFFSET ?
        """

        return self._fetch_all(
            query,
            [
                cnae,
                municipio,
                uf,
                data_referencia,
                limit,
                offset,
            ],
        )

