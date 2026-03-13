"""Endpoint HTTP de health check do pipeline Ad Analytics.

Expõe /health (liveness) e /health/detail (readiness completo)
para monitoramento externo — Docker healthcheck, Grafana, etc.

Uso standalone:
    uvicorn observability.health_check:app --port 8080

Ou montado na app principal:
    app.include_router(router, prefix="/health")
"""

from datetime import datetime
from pathlib import Path

import sqlalchemy as sa
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from pipeline.gold_to_postgres.settings import get_settings

# ─── Configuração ─────────────────────────────────────────────────────────────

_STORAGE_ROOT = Path("storage")
_MAX_AGE_HOURS = 25  # arquivo considerado stale após este período

# ─── Schemas de resposta ──────────────────────────────────────────────────────


class LayerStatus(BaseModel):
    """Status de uma camada de armazenamento (Bronze/Silver/Gold).

    Attributes:
        ok: True se a camada tem arquivos recentes.
        latest_file: Nome do arquivo mais recente encontrado.
        age_hours: Horas desde a última modificação.
        message: Descrição do status.
    """

    ok: bool
    latest_file: str | None = None
    age_hours: float | None = None
    message: str


class HealthDetail(BaseModel):
    """Resposta completa do health check com status por componente.

    Attributes:
        status: 'healthy' ou 'degraded'.
        timestamp: Momento da verificação.
        postgres: Status da conexão com PostgreSQL.
        bronze: Status da camada Bronze.
        silver: Status da camada Silver.
        gold: Status da camada Gold.
    """

    status: str
    timestamp: str
    postgres: LayerStatus
    bronze: LayerStatus
    silver: LayerStatus
    gold: LayerStatus


# ─── Funções de verificação ───────────────────────────────────────────────────


def _check_layer(layer_name: str) -> LayerStatus:
    """Verifica se uma camada tem arquivos Parquet recentes.

    Args:
        layer_name: Nome da camada ('bronze', 'silver', 'gold').

    Returns:
        LayerStatus com resultado da verificação.
    """
    layer_dir = _STORAGE_ROOT / layer_name
    if not layer_dir.exists():
        return LayerStatus(ok=False, message=f"Diretório {layer_name}/ não encontrado.")

    parquet_files = sorted(layer_dir.rglob("*.parquet"))
    if not parquet_files:
        return LayerStatus(ok=False, message=f"Nenhum Parquet em {layer_name}/.")

    latest = parquet_files[-1]
    mtime = datetime.fromtimestamp(latest.stat().st_mtime)
    age_hours = round((datetime.now() - mtime).total_seconds() / 3600, 1)

    if age_hours > _MAX_AGE_HOURS:
        return LayerStatus(
            ok=False,
            latest_file=latest.name,
            age_hours=age_hours,
            message=f"Arquivo mais recente tem {age_hours}h — pipeline pode estar parado.",
        )

    return LayerStatus(
        ok=True,
        latest_file=latest.name,
        age_hours=age_hours,
        message="ok",
    )


def _check_postgres() -> LayerStatus:
    """Verifica se a conexão com PostgreSQL está ativa.

    Returns:
        LayerStatus com resultado da verificação.
    """
    try:
        engine = sa.create_engine(get_settings().dsn, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        engine.dispose()
        return LayerStatus(ok=True, message="Conexão PostgreSQL ok.")
    except Exception as exc:
        return LayerStatus(ok=False, message=f"PostgreSQL indisponível: {exc}")


# ─── Router FastAPI ───────────────────────────────────────────────────────────

router = APIRouter(tags=["health"])


@router.get("/")
def health_liveness() -> dict:
    """Liveness check — responde 200 se o processo está vivo.

    Usado pelo Docker HEALTHCHECK e load balancers.
    """
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@router.get("/detail", response_model=HealthDetail)
def health_detail() -> HealthDetail:
    """Readiness check completo — verifica todos os componentes.

    Retorna 200 mesmo em estado degradado; o campo `status` indica
    se algo está fora do esperado.
    """
    bronze = _check_layer("bronze")
    silver = _check_layer("silver")
    gold = _check_layer("gold")
    postgres = _check_postgres()

    all_ok = all([bronze.ok, silver.ok, gold.ok, postgres.ok])

    return HealthDetail(
        status="healthy" if all_ok else "degraded",
        timestamp=datetime.now().isoformat(),
        postgres=postgres,
        bronze=bronze,
        silver=silver,
        gold=gold,
    )


# ─── App standalone ───────────────────────────────────────────────────────────

app = FastAPI(title="Ad Analytics Health Check")
app.include_router(router, prefix="/health")
