"""Módulo responsável por persistir DataFrames transformados na camada Silver.

Salva o resultado da transformação em Parquet particionado por fonte,
mantendo rastreabilidade com o nome do arquivo Bronze de origem.
"""

from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger


def write_silver(
    df: pl.DataFrame,
    source_name: str,
    silver_root: str,
    ingestion_timestamp: datetime | None = None,
) -> Path:
    """Persiste um DataFrame transformado em Parquet na camada Silver.

    O arquivo é salvo em:
        {silver_root}/{source_name}/{source_name}_{YYYYMMDD_HHMMSS}.parquet

    Args:
        df: DataFrame já transformado no schema Silver.
        source_name: Identificador da fonte (ex: 'google_ads').
        silver_root: Caminho raiz do diretório Silver.
        ingestion_timestamp: Timestamp do arquivo. Se None, usa o momento atual.

    Returns:
        Path do arquivo Parquet gerado.

    Raises:
        ValueError: Se o DataFrame estiver vazio.
        OSError: Se não for possível criar o diretório ou escrever o arquivo.
    """
    if df.is_empty():
        raise ValueError(f"DataFrame vazio — nada para persistir na Silver. Fonte: {source_name}")

    timestamp = ingestion_timestamp or datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    output_dir = Path(silver_root) / source_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{source_name}_{timestamp_str}.parquet"

    df.write_parquet(output_path)

    logger.info(f"Silver | fonte={source_name} | linhas={len(df)} | arquivo={output_path}")

    return output_path
