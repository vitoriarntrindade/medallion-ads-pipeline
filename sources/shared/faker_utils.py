"""Utilitários compartilhados para geração de dados sintéticos."""

import random

CAMPAIGN_NAMES = [
    "Brand Awareness Q1",
    "Retargeting Summer",
    "Black Friday Promo",
    "Lead Gen B2B",
    "App Install Campaign",
    "Holiday Sale",
]

AD_SETS = [
    "Audience 18-34",
    "Lookalike 1%",
    "Remarketing Cart",
    "Interest Tech",
    "Broad Match",
]


def random_impressions(min_val: int = 1_000, max_val: int = 500_000) -> int:
    """Gera um número aleatório de impressões dentro de um intervalo.

    Args:
        min_val: Valor mínimo de impressões.
        max_val: Valor máximo de impressões.

    Returns:
        Número inteiro de impressões.
    """
    return random.randint(min_val, max_val)


def random_clicks(impressions: int, ctr_min: float = 0.005, ctr_max: float = 0.08) -> int:
    """Gera cliques baseado em um CTR aleatório aplicado sobre as impressões.

    Args:
        impressions: Total de impressões da linha.
        ctr_min: CTR mínimo (taxa de cliques).
        ctr_max: CTR máximo (taxa de cliques).

    Returns:
        Número inteiro de cliques.
    """
    ctr = random.uniform(ctr_min, ctr_max)
    return max(1, int(impressions * ctr))


def random_cost(clicks: int, cpc_min: float = 0.10, cpc_max: float = 5.00) -> float:
    """Gera custo baseado em um CPC aleatório aplicado sobre os cliques.

    Args:
        clicks: Total de cliques da linha.
        cpc_min: CPC mínimo (custo por clique).
        cpc_max: CPC máximo (custo por clique).

    Returns:
        Custo total como float, arredondado em 2 casas decimais.
    """
    cpc = random.uniform(cpc_min, cpc_max)
    return round(clicks * cpc, 2)


def random_conversions(clicks: int, cvr_min: float = 0.01, cvr_max: float = 0.15) -> int:
    """Gera conversões baseado em uma taxa de conversão aleatória.

    Args:
        clicks: Total de cliques da linha.
        cvr_min: Taxa de conversão mínima.
        cvr_max: Taxa de conversão máxima.

    Returns:
        Número inteiro de conversões.
    """
    cvr = random.uniform(cvr_min, cvr_max)
    return max(0, int(clicks * cvr))


def pick_campaign() -> str:
    """Seleciona aleatoriamente um nome de campanha da lista pré-definida.

    Returns:
        Nome da campanha selecionado.
    """
    return random.choice(CAMPAIGN_NAMES)


def pick_ad_set() -> str:
    """Seleciona aleatoriamente um nome de ad set da lista pré-definida.

    Returns:
        Nome do ad set selecionado.
    """
    return random.choice(AD_SETS)
