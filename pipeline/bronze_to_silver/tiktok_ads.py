"""Transformer Bronze → Silver para dados do TikTok Ads.

Problemas que este transformer resolve:
- views → impressions: renomeia o campo
- engagements → clicks: renomeia o campo
- total_cost (string!) → cost_brl (float): parse de string
- conversions_count → conversions: renomeia o campo
- campaign_title → campaign_name: renomeia para o campo canônico
- ad_group_title → ad_group_name: renomeia para o campo canônico
- stat_time (str "YYYY-MM-DD HH:MM:SS") → date (Date): extrai apenas a data
"""

import polars as pl
from loguru import logger

from pipeline.bronze_to_silver.base_transformer import (
    BaseTransformer,
    TransformationError,
)


class TikTokAdsTransformer(BaseTransformer):
    """Transforma dados brutos do TikTok Ads para o schema Silver.

    Conversões realizadas:
    - ``views`` → ``impressions``
    - ``engagements`` → ``clicks``
    - ``total_cost`` (str) → ``cost_brl`` (float via cast)
    - ``conversions_count`` → ``conversions``
    - ``campaign_title`` → ``campaign_name``
    - ``ad_group_title`` → ``ad_group_name``
    - ``stat_time`` (str "YYYY-MM-DD HH:MM:SS") → ``date`` (pl.Date)
    """

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'tiktok_ads'.
        """
        return "tiktok_ads"

    def transform(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Transforma DataFrame bruto do TikTok Ads para o schema Silver.

        Args:
            df: DataFrame com colunas brutas do TikTok Ads.
            source_file: Caminho do arquivo Bronze de origem.

        Returns:
            DataFrame no schema Silver (sem metadados — adicionados pelo base).

        Raises:
            TransformationError: Se colunas obrigatórias estiverem ausentes.
        """
        required_columns = {
            "stat_time",
            "campaign_title",
            "ad_group_title",
            "views",
            "engagements",
            "total_cost",
            "conversions_count",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise TransformationError(
                message=f"Colunas ausentes no DataFrame do TikTok Ads: {missing}",
                source=self.source_name,
                source_file=source_file,
            )

        try:
            df_silver = df.select(
                [
                    # extrai apenas a data do timestamp completo "YYYY-MM-DD HH:MM:SS"
                    pl.col("stat_time").str.slice(0, 10).str.to_date("%Y-%m-%d").alias("date"),
                    pl.lit(self.source_name).alias("source"),
                    pl.col("campaign_title").cast(pl.Utf8).alias("campaign_name"),
                    pl.col("ad_group_title").cast(pl.Utf8).alias("ad_group_name"),
                    pl.col("views").cast(pl.Int64).alias("impressions"),
                    pl.col("engagements").cast(pl.Int64).alias("clicks"),
                    # total_cost vem como string — precisa de cast explícito
                    pl.col("total_cost").cast(pl.Float64).alias("cost_brl"),
                    pl.col("conversions_count").cast(pl.Int64).alias("conversions"),
                ]
            )
        except Exception as exc:
            raise TransformationError(
                message=f"Falha ao transformar colunas do TikTok Ads: {exc}",
                source=self.source_name,
                source_file=source_file,
            ) from exc

        logger.debug(
            f"TikTok Ads | stat_time→date, total_cost(str)→cost_brl(float), views→impressions | linhas={len(df_silver)}"
        )
        return df_silver
