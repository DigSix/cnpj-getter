from fastapi import FastAPI, Query, HTTPException
from typing import List, Dict, Any

from app.config import get_settings
from app.api.leads_query_service import LeadsQueryService


# ============================================================
# SETUP
# ============================================================

settings = get_settings()

query_service = LeadsQueryService(
    duckdb_path=settings.duckdb_path,
)

app = FastAPI(
    title="CNPJ Leads API",
    version="1.0.0",
)


# ============================================================
# INTERNAL
# ============================================================

def _safe_limit(limit: int) -> int:
    MAX_LIMIT = 1000

    if limit < 1:
        return 1

    return min(limit, MAX_LIMIT)


# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/leads")
def list_leads(
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:

    print("[API] list leads")

    safe_limit = _safe_limit(limit)

    return query_service.list(
        limit=safe_limit,
        offset=offset,
    )


@app.get("/leads/{cnpj}")
def get_lead(
    cnpj: str,
    data_referencia: str = Query(...),
) -> Dict[str, Any]:

    print(f"[API] get lead {cnpj}")

    result = query_service.get_by_cnpj(
        cnpj=cnpj,
        data_referencia=data_referencia,
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Lead not found",
        )

    return result


@app.get("/leads/filter/uf")
def filter_by_uf(
    uf: str = Query(...),
    data_referencia: str = Query(...),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:

    print(f"[API] filter by uf={uf}")

    safe_limit = _safe_limit(limit)

    return query_service.by_uf(
        uf=uf,
        data_referencia=data_referencia,
        limit=safe_limit,
        offset=offset,
    )


@app.get("/leads/filter/cnae")
def filter_by_cnae(
    cnae: str = Query(...),
    data_referencia: str = Query(...),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:

    print(f"[API] filter by cnae={cnae}")

    safe_limit = _safe_limit(limit)

    return query_service.by_cnae(
        cnae=cnae,
        data_referencia=data_referencia,
        limit=safe_limit,
        offset=offset,
    )


@app.get("/leads/filter/full")
def filter_by_cnae_municipio_uf(
    cnae: str = Query(...),
    municipio: str = Query(...),
    uf: str = Query(...),
    data_referencia: str = Query(...),
    limit: int = Query(100, ge=1),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:

    print(
        f"[API] filter full "
        f"cnae={cnae}, municipio={municipio}, uf={uf}"
    )

    safe_limit = _safe_limit(limit)

    return query_service.by_cnae_municipio_uf(
        cnae=cnae,
        municipio=municipio,
        uf=uf,
        data_referencia=data_referencia,
        limit=safe_limit,
        offset=offset,
    )
