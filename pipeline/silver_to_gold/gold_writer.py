"""Persistência dos DataFrames Gold em Parquet.

Cada tabela Gold é salva em seu próprio subdiretório dentro do gold_root,
com o mesmo padrão de nomenclatura das camadas anteriores:
    {gold_root}/{table_name}/{table_name}_{YYYYMMDD_HHMMSS}.parquet
"""

from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger

# Tabelas Gold disponíveis — define a estrutura esperada de subdiretórios
GOLD_TABLES = ["daily_summary", "campaign_summary", "source_comparison"]


def write_gold(
    df: pl.DataFrame,
    table_name: str,
    gold_root: str,
    ingestion_timestamp: datetime | None = None,
) -> Path:
    """Persiste um DataFrame Gold em Parquet.

    Cria o subdiretório da tabela se necessário e salva o arquivo
    com timestamp no nome para garantir rastreabilidade sem sobrescrever
    execuções anteriores.

    Args:
        df: DataFrame Gold a persistir.
        table_name: Nome lógico da tabela (ex: 'daily_summary').
        gold_root: Caminho raiz da camada Gold.
        ingestion_timestamp: Timestamp da execução. Usa o momento atual
            se não fornecido.

    Returns:
        Path do arquivo Parquet gerado.

    Raises:
        ValueError: Se o DataFrame estiver vazio ou o table_name for inválido.
    """
    if df.is_empty():
        raise ValueError(f"DataFrame Gold '{table_name}' está vazio. Nada será salvo.")

    if table_name not in GOLD_TABLES:
        raise ValueError(f"Tabela Gold desconhecida: '{table_name}'. Esperado: {GOLD_TABLES}")

    ts = ingestion_timestamp or datetime.now()
    ts_str = ts.strftime("%Y%m%d_%H%M%S")

    output_dir = Path(gold_root) / table_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{table_name}_{ts_str}.parquet"
    df.write_parquet(output_path)

    logger.info(f"Gold salvo | tabela={table_name} | linhas={len(df)} | arquivo={output_path.name}")
    return output_path
