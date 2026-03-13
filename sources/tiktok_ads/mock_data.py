"""Geração de dados mock para a API do TikTok Ads."""

import random
import uuid
from datetime import date

from sources.shared.date_utils import generate_date_range
from sources.shared.faker_utils import (
    pick_ad_set,
    pick_campaign,
    random_clicks,
    random_conversions,
    random_cost,
    random_impressions,
)
from sources.tiktok_ads.schemas import TikTokAdsCampaignRow

TIKTOK_OBJECTIVES = [
    "TRAFFIC",
    "CONVERSIONS",
    "APP_INSTALL",
    "VIDEO_VIEWS",
    "REACH",
]


def generate_tiktok_ads_rows(
    start_date: date,
    end_date: date,
) -> tuple[list[TikTokAdsCampaignRow], str]:
    """Gera linhas de estatísticas diárias simuladas no formato do TikTok Ads.

    Diferenças intencionais em relação ao Google e Meta:
    - Datas como timestamp completo (YYYY-MM-DD HH:MM:SS).
    - Impressões como 'views'.
    - Cliques como 'engagements'.
    - Custo como string em 'total_cost' (requer parsing na Silver).
    - Conversões como 'conversions_count'.
    - Nome da campanha como 'campaign_title'.
    - Retorna um request_id simulado junto com as linhas.

    Args:
        start_date: Data inicial do intervalo.
        end_date: Data final do intervalo.

    Returns:
        Tupla contendo (lista de linhas, request_id simulado).
    """
    rows = []
    request_id = str(uuid.uuid4())

    for current_date in generate_date_range(start_date, end_date):
        impressions = random_impressions()
        clicks = random_clicks(impressions)
        cost = random_cost(clicks)
        conversions = random_conversions(clicks)

        # timestamp no formato diferente dos outros — intencional
        stat_time = f"{current_date.strftime('%Y-%m-%d')} 00:00:00"

        row = TikTokAdsCampaignRow(
            stat_time=stat_time,
            campaign_title=pick_campaign(),
            ad_group_title=pick_ad_set(),
            views=impressions,
            engagements=clicks,
            total_cost=str(cost),  # custo como string — intencional
            conversions_count=conversions,
            campaign_objective=random.choice(TIKTOK_OBJECTIVES),
            country_code="BR",
        )
        rows.append(row)

    return rows, request_id
