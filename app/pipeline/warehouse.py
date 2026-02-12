import duckdb
from pathlib import Path


class CNPJWarehouse:
    """
    DuckDB warehouse for CNPJ data.

    RAW  : rebuilt every run (staging)
    DIM  : incremental
    CURR : current consolidated state per CNPJ
    LEADS: historical monthly snapshots
    """

    def __init__(
        self,
        duckdb_path: Path,
        extracted_dir: Path,
    ) -> None:
        self.duckdb_path = duckdb_path
        self.extracted_dir = extracted_dir

    def _connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.duckdb_path))

    # ============================================================
    # SCHEMA
    # ============================================================

    def setup(self) -> None:
        """
        Create all tables if not exists.
        Safe to run multiple times.
        """
        conn = self._connect()

        # -------------------------
        # RAW
        # -------------------------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS estabelecimentos_raw (
                cnpj_basico VARCHAR,
                cnpj_ordem VARCHAR,
                cnpj_dv VARCHAR,
                nome_fantasia VARCHAR,
                situacao_cadastral VARCHAR,
                cnae_fiscal_principal VARCHAR,
                cnae_fiscal_secundaria VARCHAR,
                municipio VARCHAR,
                uf VARCHAR,
                correio_eletronico VARCHAR,
                ddd1 VARCHAR,
                telefone1 VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS empresas_raw (
                cnpj_basico VARCHAR,
                razao_social VARCHAR,
                porte_empresa VARCHAR,
                natureza_juridica VARCHAR
            )
        """)

        # -------------------------
        # DIMENSIONS
        # -------------------------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_cnae (
                codigo VARCHAR PRIMARY KEY,
                descricao VARCHAR
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_municipio (
                codigo VARCHAR PRIMARY KEY,
                nome VARCHAR
            )
        """)

        # -------------------------
        # CURRENT STATE
        # -------------------------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads_current (
                cnpj VARCHAR PRIMARY KEY,
                razao_social VARCHAR,
                nome_fantasia VARCHAR,
                municipio_nome VARCHAR,
                uf VARCHAR,
                cnae VARCHAR,
                cnae_descricao VARCHAR,
                porte_empresa VARCHAR,
                natureza_juridica VARCHAR,
                email VARCHAR,
                telefone VARCHAR
            )
        """)

        # -------------------------
        # HISTORY
        # -------------------------
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                cnpj VARCHAR,
                razao_social VARCHAR,
                nome_fantasia VARCHAR,
                municipio_nome VARCHAR,
                uf VARCHAR,
                cnae VARCHAR,
                cnae_descricao VARCHAR,
                porte_empresa VARCHAR,
                natureza_juridica VARCHAR,
                email VARCHAR,
                telefone VARCHAR,
                data_referencia VARCHAR,
                PRIMARY KEY (cnpj, data_referencia)
            )
        """)

        conn.close()
        print("[WAREHOUSE] Schema ready")

    # ============================================================
    # RAW LOAD
    # ============================================================

    def load_raw(self, month: str) -> None:
        conn = self._connect()
        base_path = self.extracted_dir / month

        print("[WAREHOUSE] Loading RAW tables")

        conn.execute("DELETE FROM estabelecimentos_raw")
        conn.execute("DELETE FROM empresas_raw")

        conn.execute(
            """
            INSERT INTO estabelecimentos_raw
            SELECT
                cnpj_basico,
                cnpj_ordem,
                cnpj_dv,
                nome_fantasia,
                situacao_cadastral,
                cnae_fiscal_principal,
                cnae_fiscal_secundaria,
                municipio,
                uf,
                correio_eletronico,
                ddd1,
                telefone1
            FROM read_csv(
                ?,
                sep=';',
                header=false,
                ignore_errors=true,
                columns={
                    'cnpj_basico': 'VARCHAR',
                    'cnpj_ordem': 'VARCHAR',
                    'cnpj_dv': 'VARCHAR',
                    'identificador_matriz_filial': 'VARCHAR',
                    'nome_fantasia': 'VARCHAR',
                    'situacao_cadastral': 'VARCHAR',
                    'data_situacao_cadastral': 'VARCHAR',
                    'motivo_situacao_cadastral': 'VARCHAR',
                    'nome_cidade_exterior': 'VARCHAR',
                    'pais': 'VARCHAR',
                    'data_inicio_atividade': 'VARCHAR',
                    'cnae_fiscal_principal': 'VARCHAR',
                    'cnae_fiscal_secundaria': 'VARCHAR',
                    'tipo_logradouro': 'VARCHAR',
                    'logradouro': 'VARCHAR',
                    'numero': 'VARCHAR',
                    'complemento': 'VARCHAR',
                    'bairro': 'VARCHAR',
                    'cep': 'VARCHAR',
                    'uf': 'VARCHAR',
                    'municipio': 'VARCHAR',
                    'ddd1': 'VARCHAR',
                    'telefone1': 'VARCHAR',
                    'ddd2': 'VARCHAR',
                    'telefone2': 'VARCHAR',
                    'ddd_fax': 'VARCHAR',
                    'fax': 'VARCHAR',
                    'correio_eletronico': 'VARCHAR',
                    'situacao_especial': 'VARCHAR',
                    'data_situacao_especial': 'VARCHAR'
                }
            )
            """,
            [str(base_path / "*.ESTABELE")],
        )

        conn.execute(
            """
            INSERT INTO empresas_raw
            SELECT
                cnpj_basico,
                razao_social,
                natureza_juridica,
                porte_empresa
            FROM read_csv(
                ?,
                sep=';',
                header=false,
                ignore_errors=true,
                columns={
                    'cnpj_basico': 'VARCHAR',
                    'razao_social': 'VARCHAR',
                    'natureza_juridica': 'VARCHAR',
                    'qualificacao_responsavel': 'VARCHAR',
                    'capital_social': 'VARCHAR',
                    'porte_empresa': 'VARCHAR',
                    'ente_federativo_responsavel': 'VARCHAR'
                }
            )
            """,
            [str(base_path / "*.EMPRECSV")],
        )

        conn.close()
        print("[WAREHOUSE] RAW load completed")

    # ============================================================
    # DIM LOAD
    # ============================================================

    def load_dim(self, month: str) -> None:
        conn = self._connect()
        base_path = self.extracted_dir / month

        print("[WAREHOUSE] Loading dimensions")

        conn.execute(
            """
            INSERT INTO dim_cnae
            SELECT codigo, descricao
            FROM read_csv(
                ?,
                sep=';',
                header=false,
                ignore_errors=true,
                columns={
                    'codigo': 'VARCHAR',
                    'descricao': 'VARCHAR'
                }
            )
            WHERE codigo NOT IN (SELECT codigo FROM dim_cnae)
            """,
            [str(base_path / "*.CNAECSV")],
        )

        conn.execute(
            """
            INSERT INTO dim_municipio
            SELECT codigo, nome
            FROM read_csv(
                ?,
                sep=';',
                header=false,
                ignore_errors=true,
                columns={
                    'codigo': 'VARCHAR',
                    'nome': 'VARCHAR'
                }
            )
            WHERE codigo NOT IN (SELECT codigo FROM dim_municipio)
            """,
            [str(base_path / "*.MUNICCSV")],
        )

        conn.close()
        print("[WAREHOUSE] Dimension load completed")

    # ============================================================
    # PRODUCT
    # ============================================================

    def build_leads(self, month: str) -> None:
        """
        Build current consolidated state and append monthly snapshot.
        """
        conn = self._connect()

        print(f"[WAREHOUSE] Building leads for {month}")

        # --------------------------------------------------
        # CURRENT STATE (rebuild)
        # --------------------------------------------------
        print("[WAREHOUSE] Rebuilding leads_current")

        conn.execute("DELETE FROM leads_current")

        conn.execute(
            """
            INSERT INTO leads_current
            SELECT
                e.cnpj_basico || e.cnpj_ordem || e.cnpj_dv AS cnpj,
                MAX(emp.razao_social)        AS razao_social,
                MAX(e.nome_fantasia)         AS nome_fantasia,
                MAX(m.nome)                  AS municipio_nome,
                MAX(e.uf)                    AS uf,
                MAX(e.cnae_fiscal_principal) AS cnae,
                MAX(c.descricao)             AS cnae_descricao,
                MAX(emp.porte_empresa)       AS porte_empresa,
                MAX(emp.natureza_juridica)   AS natureza_juridica,
                MAX(e.correio_eletronico)    AS email,
                MAX(e.ddd1 || e.telefone1)   AS telefone
            FROM estabelecimentos_raw e
            LEFT JOIN empresas_raw emp
                ON emp.cnpj_basico = e.cnpj_basico
            LEFT JOIN dim_cnae c
                ON c.codigo = e.cnae_fiscal_principal
            LEFT JOIN dim_municipio m
                ON m.codigo = e.municipio
            WHERE e.situacao_cadastral = '02'
            GROUP BY
                e.cnpj_basico,
                e.cnpj_ordem,
                e.cnpj_dv
            """
        )

        # --------------------------------------------------
        # HISTORY SNAPSHOT
        # --------------------------------------------------
        print("[WAREHOUSE] Inserting monthly snapshot")

        conn.execute(
            """
            INSERT INTO leads
            SELECT
                cnpj,
                razao_social,
                nome_fantasia,
                municipio_nome,
                uf,
                cnae,
                cnae_descricao,
                porte_empresa,
                natureza_juridica,
                email,
                telefone,
                ? AS data_referencia
            FROM leads_current
            WHERE NOT EXISTS (
                SELECT 1
                FROM leads l
                WHERE l.cnpj = leads_current.cnpj
                  AND l.data_referencia = ?
            )
            """,
            [month, month],
        )

        conn.close()
        print("[WAREHOUSE] Leads build completed")
