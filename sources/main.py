"""Aplicação FastAPI principal das fontes de Ads simuladas.

Registra os routers de Google Ads, Meta Ads e TikTok Ads em um único
servidor. Cada plataforma tem seu próprio prefixo e schema de resposta.

Uso:
    uvicorn sources.main:app --reload --port 8000
"""

from fastapi import FastAPI

from sources.google_ads.router import router as google_ads_router
from sources.meta_ads.router import router as meta_ads_router
from sources.tiktok_ads.router import router as tiktok_ads_router

app = FastAPI(
    title="Ad Analytics — Mock Sources API",
    description=(
        "APIs simuladas de plataformas de Ads para o pipeline de analytics. "
        "Cada endpoint retorna dados com schemas intencionalmente diferentes, "
        "representando o cenário real de integração com múltiplas fontes."
    ),
    version="0.1.0",
)

app.include_router(google_ads_router)
app.include_router(meta_ads_router)
app.include_router(tiktok_ads_router)


@app.get("/health", tags=["Health"], summary="Health check geral")
def global_health_check() -> dict[str, str]:
    """Verifica se o servidor principal está operacional.

    Returns:
        Dicionário com status geral da aplicação.
    """
    return {"status": "ok", "service": "ad-analytics-mock-sources"}
