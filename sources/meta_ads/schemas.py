"""Schemas Pydantic para a API mock do Meta Ads.

O Meta Ads usa 'reach' para impressões, 'link_clicks' para cliques,
'spend' como float para custo, datas no formato DD/MM/YYYY e
'results' para conversões.
"""

from pydantic import BaseModel, Field


class MetaAdsCampaignRow(BaseModel):
    """Representa uma linha de insight diário de campanha do Meta Ads.

    Attributes:
        date_start: Data inicial do período no formato DD/MM/YYYY.
        date_stop: Data final do período no formato DD/MM/YYYY.
        campaign_name: Nome da campanha.
        adset_name: Nome do conjunto de anúncios.
        reach: Número de pessoas alcançadas (equivalente a impressões).
        link_clicks: Número de cliques em links.
        spend: Valor gasto no período em BRL.
        results: Número de resultados/conversões.
        objective: Objetivo da campanha.
        currency: Código ISO da moeda.
    """

    date_start: str = Field(..., description="Data inicial no formato DD/MM/YYYY.")
    date_stop: str = Field(..., description="Data final no formato DD/MM/YYYY.")
    campaign_name: str = Field(..., description="Nome da campanha.")
    adset_name: str = Field(..., description="Nome do conjunto de anúncios.")
    reach: int = Field(..., ge=0, description="Pessoas alcançadas.")
    link_clicks: int = Field(..., ge=0, description="Total de cliques em links.")
    spend: float = Field(..., ge=0.0, description="Valor gasto em BRL.")
    results: int = Field(..., ge=0, description="Total de resultados/conversões.")
    objective: str = Field(..., description="Objetivo da campanha.")
    currency: str = Field(default="BRL", description="Código ISO da moeda.")


class MetaAdsInsightsResponse(BaseModel):
    """Resposta completa do endpoint de insights do Meta Ads.

    Attributes:
        source: Identificador da plataforma de origem.
        total_rows: Número total de linhas retornadas.
        data: Lista de insights de campanha.
    """

    source: str = Field(default="meta_ads")
    total_rows: int = Field(..., ge=0)
    data: list[MetaAdsCampaignRow]
