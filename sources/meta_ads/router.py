"""Router FastAPI para a API mock do Meta Ads."""

from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query

from sources.meta_ads.mock_data import generate_meta_ads_rows
from sources.meta_ads.schemas import MetaAdsInsightsResponse
from sources.shared.date_utils import parse_date_query

router = APIRouter(prefix="/meta-ads", tags=["Meta Ads"])


@router.get(
    "/insights",
    response_model=MetaAdsInsightsResponse,
    summary="Insights diários de campanhas",
    description=(
        "Retorna insights de performance de campanhas do Meta Ads "
        "para um intervalo de datas. Máximo de 90 dias por requisição."
    ),
)
def get_campaign_insights(
    start_date: str | None = Query(
        default=None,
        description="Data inicial no formato YYYY-MM-DD. Padrão: 7 dias atrás.",
    ),
    end_date: str | None = Query(
        default=None,
        description="Data final no formato YYYY-MM-DD. Padrão: hoje.",
    ),
) -> MetaAdsInsightsResponse:
    """Retorna insights diários de campanhas no formato Meta Ads.

    Args:
        start_date: Data inicial do relatório (YYYY-MM-DD).
        end_date: Data final do relatório (YYYY-MM-DD).

    Returns:
        Insights com linhas de performance por campanha/dia.

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

    rows = generate_meta_ads_rows(parsed_start, parsed_end)

    return MetaAdsInsightsResponse(
        source="meta_ads",
        total_rows=len(rows),
        data=rows,
    )


@router.get("/health", summary="Health check da API Meta Ads")
def health_check() -> dict[str, str]:
    """Verifica se a API está operacional.

    Returns:
        Dicionário com status e nome da fonte.
    """
    return {"status": "ok", "source": "meta_ads"}
