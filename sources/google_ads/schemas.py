"""Schemas Pydantic para a API mock do Google Ads.

O Google Ads representa custo em micros (inteiros), datas no formato
YYYY-MM-DD e usa o campo 'impressions' para alcance.
"""

from pydantic import BaseModel, Field


class GoogleAdsCampaignRow(BaseModel):
    """Representa uma linha de reporte diário de campanha do Google Ads.

    Attributes:
        date: Data do relatório no formato YYYY-MM-DD.
        campaign_name: Nome da campanha.
        ad_group_name: Nome do grupo de anúncios.
        impressions: Número de impressões.
        clicks: Número de cliques.
        cost_micros: Custo total em micros (valor em BRL * 1.000.000).
        conversions: Número de conversões registradas.
        currency: Código ISO da moeda.
    """

    date: str = Field(..., description="Data no formato YYYY-MM-DD.")
    campaign_name: str = Field(..., description="Nome da campanha.")
    ad_group_name: str = Field(..., description="Nome do grupo de anúncios.")
    impressions: int = Field(..., ge=0, description="Total de impressões.")
    clicks: int = Field(..., ge=0, description="Total de cliques.")
    cost_micros: int = Field(..., ge=0, description="Custo em micros (BRL * 1e6).")
    conversions: float = Field(..., ge=0, description="Total de conversões.")
    currency: str = Field(default="BRL", description="Código ISO da moeda.")


class GoogleAdsReportResponse(BaseModel):
    """Resposta completa do endpoint de relatório do Google Ads.

    Attributes:
        source: Identificador da plataforma de origem.
        total_rows: Número total de linhas retornadas.
        rows: Lista de linhas do relatório.
    """

    source: str = Field(default="google_ads")
    total_rows: int = Field(..., ge=0)
    rows: list[GoogleAdsCampaignRow]
