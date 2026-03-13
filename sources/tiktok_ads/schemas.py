"""Schemas Pydantic para a API mock do TikTok Ads.

O TikTok Ads usa 'views' para impressões, 'engagements' para cliques,
'total_cost' como string (!) para custo, timestamps para datas e
'campaign_title' ao invés de 'campaign_name'.
"""

from pydantic import BaseModel, Field


class TikTokAdsCampaignRow(BaseModel):
    """Representa uma linha de estatísticas diárias de campanha do TikTok Ads.

    Attributes:
        stat_time: Timestamp da estatística no formato YYYY-MM-DD HH:MM:SS.
        campaign_title: Título da campanha.
        ad_group_title: Título do grupo de anúncios.
        views: Número de visualizações (equivalente a impressões).
        engagements: Número de engajamentos (equivalente a cliques).
        total_cost: Custo total como string em BRL (ex: "125.50").
        conversions_count: Número de conversões.
        campaign_objective: Objetivo da campanha.
        country_code: Código do país.
    """

    stat_time: str = Field(..., description="Timestamp no formato YYYY-MM-DD HH:MM:SS.")
    campaign_title: str = Field(..., description="Título da campanha.")
    ad_group_title: str = Field(..., description="Título do grupo de anúncios.")
    views: int = Field(..., ge=0, description="Total de visualizações.")
    engagements: int = Field(..., ge=0, description="Total de engajamentos.")
    total_cost: str = Field(..., description="Custo total como string (ex: '125.50').")
    conversions_count: int = Field(..., ge=0, description="Total de conversões.")
    campaign_objective: str = Field(..., description="Objetivo da campanha.")
    country_code: str = Field(default="BR", description="Código do país.")


class TikTokAdsStatsResponse(BaseModel):
    """Resposta completa do endpoint de estatísticas do TikTok Ads.

    Attributes:
        source: Identificador da plataforma de origem.
        request_id: ID único da requisição (simulado).
        total_rows: Número total de linhas retornadas.
        list: Lista de estatísticas de campanha.
    """

    source: str = Field(default="tiktok_ads")
    request_id: str = Field(..., description="ID único simulado da requisição.")
    total_rows: int = Field(..., ge=0)
    list: list[TikTokAdsCampaignRow]
