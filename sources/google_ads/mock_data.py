"""Geração de dados mock para a API do Google Ads."""

from datetime import date

from sources.google_ads.schemas import GoogleAdsCampaignRow
from sources.shared.date_utils import generate_date_range
from sources.shared.faker_utils import (
    pick_ad_set,
    pick_campaign,
    random_clicks,
    random_conversions,
    random_cost,
    random_impressions,
)


def generate_google_ads_rows(
    start_date: date,
    end_date: date,
) -> list[GoogleAdsCampaignRow]:
    """Gera linhas de relatório diário simuladas no formato do Google Ads.

    Para cada data no intervalo, gera uma linha com métricas sintéticas.
    O custo é convertido para micros (int) conforme o padrão da API real.

    Args:
        start_date: Data inicial do intervalo do relatório.
        end_date: Data final do intervalo do relatório.

    Returns:
        Lista de linhas de campanha no schema do Google Ads.
    """
    rows = []

    for current_date in generate_date_range(start_date, end_date):
        impressions = random_impressions()
        clicks = random_clicks(impressions)
        cost = random_cost(clicks)
        conversions = random_conversions(clicks)

        row = GoogleAdsCampaignRow(
            date=current_date.strftime("%Y-%m-%d"),
            campaign_name=pick_campaign(),
            ad_group_name=pick_ad_set(),
            impressions=impressions,
            clicks=clicks,
            cost_micros=int(cost * 1_000_000),  # converte BRL → micros
            conversions=float(conversions),
            currency="BRL",
        )
        rows.append(row)

    return rows
