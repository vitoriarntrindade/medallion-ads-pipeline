"""Classe base abstrata para transformers Bronze → Silver.

Define o contrato que cada transformer de fonte deve implementar,
garantindo interface uniforme independente da origem dos dados.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from pipeline.bronze_to_silver.schema import SILVER_COLUMN_ORDER


class TransformationError(Exception):
    """Erro ocorrido durante a transformação de dados Bronze → Silver.

    Attributes:
        source: Nome da fonte que originou o erro.
        source_file: Arquivo Bronze que estava sendo processado.
    """

    def __init__(self, message: str, source: str, source_file: str) -> None:
        """Inicializa o erro com contexto da transformação.

        Args:
            message: Descrição do erro.
            source: Nome da fonte (ex: 'google_ads').
            source_file: Caminho do arquivo Bronze sendo processado.
        """
        super().__init__(message)
        self.source = source
        self.source_file = source_file


class BaseTransformer(ABC):
    """Contrato base para transformers de dados Bronze → Silver.

    Cada plataforma implementa esta classe, sobrescrevendo `source_name`
    e o método `transform`, que recebe um DataFrame bruto e devolve
    um DataFrame no schema Silver.

    Attributes:
        silver_root: Caminho raiz da camada Silver.
    """

    def __init__(self, silver_root: str) -> None:
        """Inicializa o transformer com o caminho de destino Silver.

        Args:
            silver_root: Caminho raiz da camada Silver.
        """
        self.silver_root = silver_root

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Nome identificador da fonte (ex: 'google_ads').

        Returns:
            String com o nome único da fonte.
        """
        ...

    @abstractmethod
    def transform(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Transforma um DataFrame Bronze no schema Silver.

        Deve renomear campos, converter tipos e normalizar valores
        de forma que o DataFrame resultante satisfaça o schema Silver.

        Args:
            df: DataFrame com dados brutos da Bronze.
            source_file: Caminho do arquivo Bronze de origem (rastreabilidade).

        Returns:
            DataFrame no schema Silver com colunas padronizadas.

        Raises:
            TransformationError: Se a transformação falhar por dados inválidos.
        """
        ...

    def _add_metadata(self, df: pl.DataFrame, source_file: str) -> pl.DataFrame:
        """Adiciona colunas de metadados de rastreabilidade ao DataFrame Silver.

        Args:
            df: DataFrame já transformado para o schema Silver.
            source_file: Caminho do arquivo Bronze de origem.

        Returns:
            DataFrame com as colunas source_file e transformed_at adicionadas.
        """
        return df.with_columns(
            [
                pl.lit(source_file).alias("source_file"),
                pl.lit(datetime.now().isoformat(timespec="seconds")).alias("transformed_at"),
            ]
        )

    def _enforce_column_order(self, df: pl.DataFrame) -> pl.DataFrame:
        """Garante que as colunas estejam na ordem canônica do schema Silver.

        Args:
            df: DataFrame com todas as colunas Silver.

        Returns:
            DataFrame com colunas reordenadas conforme SILVER_COLUMN_ORDER.
        """
        return df.select(SILVER_COLUMN_ORDER)

    def run(self, bronze_file: Path) -> pl.DataFrame | None:
        """Executa o ciclo completo de leitura, transformação e retorno.

        Lê o Parquet da Bronze, aplica a transformação específica da fonte,
        adiciona metadados e garante a ordem das colunas. Em caso de falha,
        loga o erro e retorna None para não interromper outras fontes.

        Args:
            bronze_file: Path do arquivo Parquet na camada Bronze.

        Returns:
            DataFrame Silver transformado, ou None em caso de falha.
        """
        logger.info(f"Transformando | fonte={self.source_name} | arquivo={bronze_file.name}")

        try:
            df_raw = pl.read_parquet(bronze_file)
            logger.debug(f"Lido | linhas={len(df_raw)} | colunas={df_raw.columns}")

            df_silver = self.transform(df_raw, source_file=str(bronze_file))
            df_silver = self._add_metadata(df_silver, source_file=str(bronze_file))
            df_silver = self._enforce_column_order(df_silver)

            logger.success(f"Transformação concluída | fonte={self.source_name} | linhas={len(df_silver)}")
            return df_silver

        except TransformationError as exc:
            logger.error(f"Falha na transformação | fonte={exc.source} | arquivo={exc.source_file} | erro={exc}")
            return None
        except Exception as exc:
            logger.error(
                f"Erro inesperado na transformação | fonte={self.source_name} | arquivo={bronze_file} | erro={exc}"
            )
            return None
