"""Entrypoint da transformação Bronze → Silver.

Localiza os arquivos Parquet mais recentes de cada fonte na Bronze,
aplica o transformer correspondente e persiste o resultado na Silver.

Uso:
    python -m pipeline.bronze_to_silver.run_transformation
    python -m pipeline.bronze_to_silver.run_transformation --bronze-path storage/bronze --silver-path storage/silver
"""

import argparse
import sys
from pathlib import Path

from loguru import logger

from pipeline.bronze_to_silver.google_ads import GoogleAdsTransformer
from pipeline.bronze_to_silver.meta_ads import MetaAdsTransformer
from pipeline.bronze_to_silver.silver_writer import write_silver
from pipeline.bronze_to_silver.tiktok_ads import TikTokAdsTransformer

# ─── Configuração do logger ───────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="DEBUG",
    colorize=True,
)
logger.add(
    "storage/logs/transformation_{time:YYYYMMDD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
)


def find_latest_bronze_file(bronze_root: str, source_name: str) -> Path | None:
    """Localiza o arquivo Parquet mais recente de uma fonte na Bronze.

    Busca pelo arquivo com maior timestamp no nome, garantindo que
    sempre processamos os dados mais atuais disponíveis.

    Args:
        bronze_root: Caminho raiz da Bronze.
        source_name: Nome da fonte (ex: 'google_ads').

    Returns:
        Path do arquivo mais recente, ou None se não houver arquivos.
    """
    source_dir = Path(bronze_root) / source_name
    if not source_dir.exists():
        logger.warning(f"Diretório Bronze não encontrado: {source_dir}")
        return None

    parquet_files = sorted(source_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"Nenhum arquivo Parquet em: {source_dir}")
        return None

    latest = parquet_files[-1]
    logger.debug(f"Arquivo Bronze selecionado: {latest.name}")
    return latest


def build_transformers(silver_root: str) -> list:
    """Instancia todos os transformers com o caminho Silver configurado.

    Args:
        silver_root: Caminho raiz da camada Silver.

    Returns:
        Lista de transformers prontos para execução.
    """
    return [
        GoogleAdsTransformer(silver_root=silver_root),
        MetaAdsTransformer(silver_root=silver_root),
        TikTokAdsTransformer(silver_root=silver_root),
    ]


def run_transformation(bronze_root: str, silver_root: str) -> dict[str, Path | None]:
    """Executa a transformação Bronze → Silver para todas as fontes.

    Para cada transformer, localiza o arquivo Bronze mais recente da fonte,
    transforma para o schema Silver e persiste. Falhas em uma fonte não
    interrompem o processamento das demais.

    Args:
        bronze_root: Caminho raiz da camada Bronze.
        silver_root: Caminho raiz da camada Silver.

    Returns:
        Dicionário mapeando nome da fonte para o Path Silver gerado (ou None).
    """
    logger.info("=" * 60)
    logger.info("Iniciando transformação Bronze → Silver")
    logger.info("=" * 60)

    transformers = build_transformers(silver_root=silver_root)
    results: dict[str, Path | None] = {}

    for transformer in transformers:
        source = transformer.source_name

        bronze_file = find_latest_bronze_file(bronze_root, source)
        if bronze_file is None:
            logger.warning(f"Pulando fonte sem dados na Bronze: {source}")
            results[source] = None
            continue

        df_silver = transformer.run(bronze_file=bronze_file)

        if df_silver is None:
            results[source] = None
            continue

        try:
            output_path = write_silver(
                df=df_silver,
                source_name=source,
                silver_root=silver_root,
            )
            results[source] = output_path
        except Exception as exc:
            logger.error(f"Falha ao salvar Silver | fonte={source} | erro={exc}")
            results[source] = None

    # ─── Sumário final ────────────────────────────────────────────
    successful = [s for s, p in results.items() if p is not None]
    failed = [s for s, p in results.items() if p is None]

    logger.info("=" * 60)
    logger.info(f"Transformação finalizada | sucesso={len(successful)} | falhas={len(failed)}")
    for source, path in results.items():
        status = "✓" if path else "✗"
        logger.info(f"  {status} {source}: {path or 'FALHOU'}")
    logger.info("=" * 60)

    return results


def parse_args() -> argparse.Namespace:
    """Processa argumentos de linha de comando.

    Returns:
        Namespace com bronze_path e silver_path.
    """
    parser = argparse.ArgumentParser(description="Transformação Bronze → Silver para todas as fontes de Ads.")
    parser.add_argument(
        "--bronze-path",
        default="storage/bronze",
        help="Caminho raiz da camada Bronze (padrão: storage/bronze).",
    )
    parser.add_argument(
        "--silver-path",
        default="storage/silver",
        help="Caminho raiz da camada Silver (padrão: storage/silver).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_transformation(bronze_root=args.bronze_path, silver_root=args.silver_path)
