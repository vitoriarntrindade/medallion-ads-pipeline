"""Suite de Expectations para a camada Silver.

Define todas as regras de qualidade de dados que um DataFrame Silver
deve satisfazer, independente da fonte de origem.

As Expectations cobrem:
- Presença e não-nulidade das colunas obrigatórias
- Valores dentro dos conjuntos e faixas esperadas
- Consistência entre colunas (clicks <= impressions, conversions <= clicks)

Nota sobre a API GX 1.x:
    `suite.add_expectation()` requer um DataContext ativo. Por isso,
    `build_silver_suite()` recebe o `ctx` já inicializado e registra
    a suite nele antes de popular com Expectations.
"""

import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)

from pipeline.bronze_to_silver.schema import SILVER_COLUMN_ORDER

# Fontes reconhecidas — mesma lista do SilverRow validator
_KNOWN_SOURCES = ["google_ads", "meta_ads", "tiktok_ads"]

# Nome fixo da suite — identificador reutilizável entre execuções
SUITE_NAME = "silver_quality_suite"


def build_silver_suite(ctx: AbstractDataContext) -> gx.ExpectationSuite:
    """Constrói e registra a ExpectationSuite com todas as regras da camada Silver.

    Registra a suite no DataContext fornecido antes de adicionar Expectations,
    conforme exigido pela API do GX 1.x. Cada regra é anotada com uma
    descrição em português para facilitar a leitura dos relatórios.

    Args:
        ctx: DataContext GX ativo (ex: retornado por gx.get_context).

    Returns:
        Suite registrada e populada com todas as Expectations.
    """
    suite = ctx.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    # ── 1. Todas as colunas canônicas devem existir ──────────────────────────
    suite.add_expectation(
        gxe.ExpectTableColumnsToMatchSet(
            column_set=SILVER_COLUMN_ORDER,
            exact_match=False,  # permite colunas extras no futuro
            description="Todas as colunas canônicas da Silver devem estar presentes.",
        )
    )

    # ── 2. Campos obrigatórios não-nulos ─────────────────────────────────────
    not_null_columns = [
        "date",
        "source",
        "campaign_name",
        "ad_group_name",
        "impressions",
        "clicks",
        "cost_brl",
        "conversions",
        "source_file",
        "transformed_at",
    ]
    for col in not_null_columns:
        suite.add_expectation(
            gxe.ExpectColumnValuesToNotBeNull(
                column=col,
                description=f"'{col}' não pode conter valores nulos.",
            )
        )

    # ── 3. source: apenas valores conhecidos ─────────────────────────────────
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(
            column="source",
            value_set=_KNOWN_SOURCES,
            description=f"'source' deve ser um de: {_KNOWN_SOURCES}.",
        )
    )

    # ── 4. Campos numéricos: faixas válidas (>= 0) ───────────────────────────
    for col in ("impressions", "clicks", "conversions"):
        suite.add_expectation(
            gxe.ExpectColumnValuesToBeBetween(
                column=col,
                min_value=0,
                description=f"'{col}' deve ser >= 0.",
            )
        )

    suite.add_expectation(
        gxe.ExpectColumnValuesToBeBetween(
            column="cost_brl",
            min_value=0.0,
            description="'cost_brl' deve ser >= 0.0.",
        )
    )

    # ── 5. Consistência: clicks <= impressions ────────────────────────────────
    # GX 1.x usa ExpectColumnPairValuesAToBeGreaterThanB (sem OrEqualTo).
    # Usamos or_equal=True para cobrir o caso clicks == impressions.
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="impressions",
            column_B="clicks",
            or_equal=True,
            description="'impressions' deve ser >= 'clicks' em todas as linhas.",
        )
    )

    # ── 6. Consistência: conversions <= clicks ────────────────────────────────
    suite.add_expectation(
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="clicks",
            column_B="conversions",
            or_equal=True,
            description="'clicks' deve ser >= 'conversions' em todas as linhas.",
        )
    )

    return suite
