"""Router FastAPI para a API mock do TikTok Ads."""

from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query

from sources.shared.date_utils import parse_date_query
from sources.tiktok_ads.mock_data import generate_tiktok_ads_rows
from sources.tiktok_ads.schemas import TikTokAdsStatsResponse

router = APIRouter(prefix="/tiktok-ads", tags=["TikTok Ads"])


@router.get(
    "/stats",
    response_model=TikTokAdsStatsResponse,
    summary="Estatísticas diárias de campanhas",
    description=(
        "Retorna estatísticas de performance de campanhas do TikTok Ads "
        "para um intervalo de datas. Máximo de 90 dias por requisição."
    ),
)
def get_campaign_stats(
    start_date: str | None = Query(
        default=None,
        description="Data inicial no formato YYYY-MM-DD. Padrão: 7 dias atrás.",
    ),
    end_date: str | None = Query(
        default=None,
        description="Data final no formato YYYY-MM-DD. Padrão: hoje.",
    ),
) -> TikTokAdsStatsResponse:
    """Retorna estatísticas diárias de campanhas no formato TikTok Ads.

    Args:
        start_date: Data inicial do relatório (YYYY-MM-DD).
        end_date: Data final do relatório (YYYY-MM-DD).

    Returns:
        Estatísticas com linhas de performance por campanha/dia.

    Raises:
        HTTPException: 422 se start_date for posterior a end_date.
        HTTPException: 422 se o intervalo exceder 90 dias.
    """
    today = date.today()
    parsed_start = parse_date_query(start_date, fallback=today - timedelta(days=7))
    parsed_end = parse_date_query(end_date, fallback=today)

    if parsed_start > parsed_end:
        raise HTTPException(
            status_code=422,
            detail="start_date não pode ser posterior a end_date.",
        )

    if (parsed_end - parsed_start).days > 90:
        raise HTTPException(
            status_code=422,
            detail="Intervalo máximo permitido é de 90 dias.",
        )

    rows, request_id = generate_tiktok_ads_rows(parsed_start, parsed_end)

    return TikTokAdsStatsResponse(
        source="tiktok_ads",
        request_id=request_id,
        total_rows=len(rows),
        list=rows,
    )


@router.get("/health", summary="Health check da API TikTok Ads")
def health_check() -> dict[str, str]:
    """Verifica se a API está operacional.

    Returns:
        Dicionário com status e nome da fonte.
    """
    return {"status": "ok", "source": "tiktok_ads"}
