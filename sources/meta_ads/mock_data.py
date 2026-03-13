"""Geração de dados mock para a API do Meta Ads."""

import random
from datetime import date

from sources.meta_ads.schemas import MetaAdsCampaignRow
from sources.shared.date_utils import generate_date_range
from sources.shared.faker_utils import (
    pick_ad_set,
    pick_campaign,
    random_clicks,
    random_conversions,
    random_cost,
    random_impressions,
)

CAMPAIGN_OBJECTIVES = [
    "LINK_CLICKS",
    "CONVERSIONS",
    "BRAND_AWARENESS",
    "REACH",
    "LEAD_GENERATION",
]


def generate_meta_ads_rows(
    start_date: date,
    end_date: date,
) -> list[MetaAdsCampaignRow]:
    """Gera linhas de insights diários simuladas no formato do Meta Ads.

    Diferenças intencionais em relação ao Google Ads:
    - Datas no formato DD/MM/YYYY.
    - Impressões representadas como 'reach'.
    - Cliques como 'link_clicks'.
    - Custo como float em 'spend' (não em micros).
    - Conversões como 'results'.

    Args:
        start_date: Data inicial do intervalo.
        end_date: Data final do intervalo.

    Returns:
        Lista de linhas de insight no schema do Meta Ads.
    """
    rows = []

    for current_date in generate_date_range(start_date, end_date):
        impressions = random_impressions()
        clicks = random_clicks(impressions)
        cost = random_cost(clicks)
        conversions = random_conversions(clicks)
        date_formatted = current_date.strftime("%d/%m/%Y")  # formato diferente do Google

        row = MetaAdsCampaignRow(
            date_start=date_formatted,
            date_stop=date_formatted,
            campaign_name=pick_campaign(),
            adset_name=pick_ad_set(),
            reach=impressions,
            link_clicks=clicks,
            spend=cost,
            results=conversions,
            objective=random.choice(CAMPAIGN_OBJECTIVES),
            currency="BRL",
        )
        rows.append(row)

    return rows
