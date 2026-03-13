"""Módulo responsável por persistir dados brutos na camada Bronze.

Salva os dados exatamente como recebidos das APIs, sem transformações.
O particionamento é feito por fonte e timestamp de ingestão.
"""

from datetime import datetime
from pathlib import Path

import polars as pl
from loguru import logger


def write_bronze(
    records: list[dict],
    source_name: str,
    bronze_root: str,
    ingestion_timestamp: datetime | None = None,
) -> Path:
    """Persiste uma lista de registros brutos em Parquet na camada Bronze.

    O arquivo é salvo em:
        {bronze_root}/{source_name}/{source_name}_{YYYYMMDD_HHMMSS}.parquet

    Não realiza nenhuma transformação nos dados — o que chegou é o que
    é salvo. Isso garante rastreabilidade total para reprocessamento.

    Args:
        records: Lista de dicionários com os dados brutos da fonte.
        source_name: Identificador da fonte (ex: "google_ads").
        bronze_root: Caminho raiz do diretório Bronze.
        ingestion_timestamp: Timestamp da ingestão. Se None, usa o momento atual.

    Returns:
        Path do arquivo Parquet gerado.

    Raises:
        ValueError: Se a lista de registros estiver vazia.
        OSError: Se não for possível criar o diretório ou escrever o arquivo.
    """
    if not records:
        raise ValueError(f"Nenhum registro para persistir na Bronze. Fonte: {source_name}")

    timestamp = ingestion_timestamp or datetime.now()
    timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

    output_dir = Path(bronze_root) / source_name
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{source_name}_{timestamp_str}.parquet"

    df = pl.DataFrame(records, infer_schema_length=len(records))
    df.write_parquet(output_path)

    logger.info(f"Bronze | fonte={source_name} | linhas={len(records)} | arquivo={output_path}")

    return output_path
