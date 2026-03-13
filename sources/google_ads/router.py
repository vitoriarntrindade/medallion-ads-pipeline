"""Router FastAPI para a API mock do Google Ads."""

from datetime import date, timedelta

from fastapi import APIRouter, HTTPException, Query

from sources.google_ads.mock_data import generate_google_ads_rows
from sources.google_ads.schemas import GoogleAdsReportResponse
from sources.shared.date_utils import parse_date_query

router = APIRouter(prefix="/google-ads", tags=["Google Ads"])


@router.get(
    "/report",
    response_model=GoogleAdsReportResponse,
    summary="Relatório diário de campanhas",
    description=(
        "Retorna dados de performance de campanhas do Google Ads "
        "para um intervalo de datas. Máximo de 90 dias por requisição."
    ),
)
def get_campaign_report(
    start_date: str | None = Query(
        default=None,
        description="Data inicial no formato YYYY-MM-DD. Padrão: 7 dias atrás.",
        alias="start_date",
    ),
    end_date: str | None = Query(
        default=None,
        description="Data final no formato YYYY-MM-DD. Padrão: hoje.",
        alias="end_date",
    ),
) -> GoogleAdsReportResponse:
    """Retorna relatório diário de campanhas no formato Google Ads.

    Args:
        start_date: Data inicial do relatório (YYYY-MM-DD).
        end_date: Data final do relatório (YYYY-MM-DD).

    Returns:
        Relatório com linhas de performance por campanha/dia.

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

    rows = generate_google_ads_rows(parsed_start, parsed_end)

    return GoogleAdsReportResponse(
        source="google_ads",
        total_rows=len(rows),
        rows=rows,
    )


@router.get("/health", summary="Health check da API Google Ads")
def health_check() -> dict[str, str]:
    """Verifica se a API está operacional.

    Returns:
        Dicionário com status e nome da fonte.
    """
    return {"status": "ok", "source": "google_ads"}
