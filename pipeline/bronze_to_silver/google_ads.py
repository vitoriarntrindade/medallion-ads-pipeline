"""Transformer Bronze → Silver para dados do Google Ads.

Problemas que este transformer resolve:
- cost_micros (int) → cost_brl (float): divide por 1.000.000
- conversions (float) → conversions (int): cast de tipo
- date (string YYYY-MM-DD) → date (Date): parse de string
- Renomeia ad_group_name para o campo canônico
"""

import polars as pl
from loguru import logger

from pipeline.bronze_to_silver.base_transformer import (
    BaseTransformer,
    TransformationError,
)


class GoogleAdsTransformer(BaseTransformer):
    """Transforma dados brutos do Google Ads para o schema Silver.

    Conversões realizadas:
    - ``cost_micros`` (int) → ``cost_brl`` (float) dividindo por 1.000.000
    - ``conversions`` (float) → ``conversions`` (int)
    - ``date`` (str "YYYY-MM-DD") → ``date`` (pl.Date)
    - Mantém ``campaign_name`` e ``ad_group_name`` sem alteração de nome
    """

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'google_ads'.
        """
        return "google_ads"

    def transform(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Transforma DataFrame bruto do Google Ads para o schema Silver.

        Args:
            df: DataFrame com colunas brutas do Google Ads.
            source_file: Caminho do arquivo Bronze de origem.

        Returns:
            DataFrame no schema Silver (sem metadados — adicionados pelo base).

        Raises:
            TransformationError: Se colunas obrigatórias estiverem ausentes.
        """
        required_columns = {
            "date",
            "campaign_name",
            "ad_group_name",
            "impressions",
            "clicks",
            "cost_micros",
            "conversions",
        }
        missing = required_columns - set(df.columns)
        if missing:
            raise TransformationError(
                message=f"Colunas ausentes no DataFrame do Google Ads: {missing}",
                source=self.source_name,
                source_file=source_file,
            )

        try:
            df_silver = df.select(
                [
                    pl.col("date").str.to_date("%Y-%m-%d").alias("date"),
                    pl.lit(self.source_name).alias("source"),
                    pl.col("campaign_name").cast(pl.Utf8).alias("campaign_name"),
                    pl.col("ad_group_name").cast(pl.Utf8).alias("ad_group_name"),
                    pl.col("impressions").cast(pl.Int64).alias("impressions"),
                    pl.col("clicks").cast(pl.Int64).alias("clicks"),
                    (pl.col("cost_micros").cast(pl.Float64) / 1_000_000).alias("cost_brl"),
                    pl.col("conversions").cast(pl.Int64).alias("conversions"),
                ]
            )
        except Exception as exc:
            raise TransformationError(
                message=f"Falha ao transformar colunas do Google Ads: {exc}",
                source=self.source_name,
                source_file=source_file,
            ) from exc

        logger.debug(f"Google Ads | cost_micros → cost_brl | linhas={len(df_silver)}")
        return df_silver
