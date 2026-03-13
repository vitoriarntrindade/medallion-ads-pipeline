"""Schema Pydantic da camada Silver e função de validação de linha.

Define o contrato de dados que qualquer fonte deve satisfazer após
a transformação. Toda linha que chega à Silver deve ser validável
por este schema.
"""

from datetime import date

from pydantic import BaseModel, Field, field_validator


class SilverRow(BaseModel):
    """Representa uma linha padronizada na camada Silver.

    Este é o schema comum que todas as fontes (Google, Meta, TikTok)
    devem produzir após a transformação Bronze → Silver.

    Attributes:
        date: Data da campanha.
        source: Identificador da plataforma de origem.
        campaign_name: Nome da campanha, normalizado.
        ad_group_name: Nome do grupo/conjunto de anúncios.
        impressions: Total de impressões/views/reach.
        clicks: Total de cliques/engajamentos/link_clicks.
        cost_brl: Custo total em BRL como float.
        conversions: Total de conversões.
        source_file: Caminho do arquivo Bronze de origem.
        transformed_at: Timestamp ISO da transformação.
    """

    date: date
    source: str = Field(..., min_length=1)
    campaign_name: str = Field(..., min_length=1)
    ad_group_name: str = Field(..., min_length=1)
    impressions: int = Field(..., ge=0)
    clicks: int = Field(..., ge=0)
    cost_brl: float = Field(..., ge=0.0)
    conversions: int = Field(..., ge=0)
    source_file: str = Field(..., min_length=1)
    transformed_at: str = Field(..., min_length=1)

    @field_validator("cost_brl")
    @classmethod
    def round_cost(cls, value: float) -> float:
        """Garante que o custo tenha no máximo 6 casas decimais.

        Args:
            value: Valor do custo em BRL.

        Returns:
            Custo arredondado em 6 casas decimais.
        """
        return round(value, 6)

    @field_validator("source")
    @classmethod
    def validate_source(cls, value: str) -> str:
        """Garante que a fonte seja um dos valores conhecidos.

        Args:
            value: Nome da fonte.

        Returns:
            Nome da fonte em lowercase.

        Raises:
            ValueError: Se a fonte não for reconhecida.
        """
        known_sources = {"google_ads", "meta_ads", "tiktok_ads"}
        normalized = value.strip().lower()
        if normalized not in known_sources:
            raise ValueError(f"Fonte desconhecida: '{value}'. Esperado: {known_sources}")
        return normalized


SILVER_COLUMN_ORDER = [
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
