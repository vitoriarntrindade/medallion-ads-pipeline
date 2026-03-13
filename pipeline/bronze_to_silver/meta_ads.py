"""Transformer Bronze → Silver para dados do Meta Ads.

Problemas que este transformer resolve:
- reach → impressions: renomeia o campo
- link_clicks → clicks: renomeia o campo
- spend (float) → cost_brl: renomeia (já está no formato correto)
- results → conversions: renomeia o campo
- date_start (str "DD/MM/YYYY") → date (Date): parse de formato europeu
- adset_name → ad_group_name: renomeia para o campo canônico
"""

import polars as pl
from loguru import logger

from pipeline.bronze_to_silver.base_transformer import (
    BaseTransformer,
    TransformationError,
)


class MetaAdsTransformer(BaseTransformer):
    """Transforma dados brutos do Meta Ads para o schema Silver.

    Conversões realizadas:
    - ``reach`` → ``impressions``
    - ``link_clicks`` → ``clicks``
    - ``spend`` → ``cost_brl`` (float já no formato correto)
    - ``results`` → ``conversions``
    - ``adset_name`` → ``ad_group_name``
    - ``date_start`` (str "DD/MM/YYYY") → ``date`` (pl.Date)
    """

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'meta_ads'.
        """
        return "meta_ads"

    def transform(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Transforma DataFrame bruto do Meta Ads para o schema Silver.

        Args:
            df: DataFrame com colunas brutas do Meta Ads.
            source_file: Caminho do arquivo Bronze de origem.

        Returns:
            DataFrame no schema Silver (sem metadados — adicionados pelo base).

        Raises:
            TransformationError: Se colunas obrigatórias estiverem ausentes.
        """
        required_columns = {
            "date_start",
            "campaign_name",
            "adset_name",
            "reach",
            "link_clicks",
            "spend",
            "results",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise TransformationError(
                message=f"Colunas ausentes no DataFrame do Meta Ads: {missing}",
                source=self.source_name,
                source_file=source_file,
            )

        try:
            df_silver = df.select(
                [
                    # formato europeu DD/MM/YYYY — diferente do Google
                    pl.col("date_start").str.to_date("%d/%m/%Y").alias("date"),
                    pl.lit(self.source_name).alias("source"),
                    pl.col("campaign_name").cast(pl.Utf8).alias("campaign_name"),
                    pl.col("adset_name").cast(pl.Utf8).alias("ad_group_name"),
                    pl.col("reach").cast(pl.Int64).alias("impressions"),
                    pl.col("link_clicks").cast(pl.Int64).alias("clicks"),
                    pl.col("spend").cast(pl.Float64).alias("cost_brl"),
                    pl.col("results").cast(pl.Int64).alias("conversions"),
                ]
            )
        except Exception as exc:
            raise TransformationError(
                message=f"Falha ao transformar colunas do Meta Ads: {exc}",
                source=self.source_name,
                source_file=source_file,
            ) from exc

        logger.debug(f"Meta Ads | reach→impressions, link_clicks→clicks, results→conversions | linhas={len(df_silver)}")
        return df_silver
