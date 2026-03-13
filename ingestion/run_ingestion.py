"""Entrypoint da camada de ingestão.

Orquestra a extração de todas as fontes de Ads configuradas e
persiste os dados brutos na camada Bronze. Pode ser executado
diretamente ou chamado pelo orquestrador (Dagster) na Fase 8.

Uso:
    python -m ingestion.run_ingestion
    python -m ingestion.run_ingestion --start-date 2026-03-01 --end-date 2026-03-07
"""

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

from loguru import logger

from ingestion.extractors.google_ads import GoogleAdsExtractor
from ingestion.extractors.meta_ads import MetaAdsExtractor
from ingestion.extractors.tiktok_ads import TikTokAdsExtractor
from ingestion.settings import settings

# ─── Configuração do logger ───────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="DEBUG",
    colorize=True,
)
logger.add(
    "storage/logs/ingestion_{time:YYYYMMDD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
)


def build_extractors() -> list:
    """Instancia todos os extratores configurados com as settings do ambiente.

    Returns:
        Lista de extratores prontos para execução.
    """
    return [
        GoogleAdsExtractor(
            api_url=settings.google_ads_api_url,
            bronze_root=settings.bronze_path,
            http_timeout=settings.http_timeout,
        ),
        MetaAdsExtractor(
            api_url=settings.meta_ads_api_url,
            bronze_root=settings.bronze_path,
            http_timeout=settings.http_timeout,
        ),
        TikTokAdsExtractor(
            api_url=settings.tiktok_ads_api_url,
            bronze_root=settings.bronze_path,
            http_timeout=settings.http_timeout,
        ),
    ]


def run_ingestion(start_date: date, end_date: date) -> dict[str, Path | None]:
    """Executa a ingestão de todas as fontes para o intervalo informado.

    Para cada extrator, chama o método `run` que faz a requisição HTTP
    e persiste na Bronze. Se uma fonte falhar, as demais continuam.

    Args:
        start_date: Data inicial do intervalo de extração.
        end_date: Data final do intervalo de extração.

    Returns:
        Dicionário mapeando nome da fonte para o Path gerado (ou None
        se a extração falhou).
    """
    logger.info("=" * 60)
    logger.info(f"Iniciando ingestão | período={start_date} → {end_date}")
    logger.info("=" * 60)

    extractors = build_extractors()
    results: dict[str, Path | None] = {}

    for extractor in extractors:
        output_path = extractor.run(start_date=start_date, end_date=end_date)
        results[extractor.source_name] = output_path

    # ─── Sumário final ────────────────────────────────────────────
    successful = [src for src, path in results.items() if path is not None]
    failed = [src for src, path in results.items() if path is None]

    logger.info("=" * 60)
    logger.info(f"Ingestão finalizada | sucesso={len(successful)} | falhas={len(failed)}")

    if failed:
        logger.warning(f"Fontes com falha: {failed}")

    for source, path in results.items():
        status = "✓" if path else "✗"
        logger.info(f"  {status} {source}: {path or 'FALHOU'}")

    logger.info("=" * 60)

    return results


def parse_args() -> argparse.Namespace:
    """Processa argumentos de linha de comando.

    Returns:
        Namespace com start_date e end_date como objetos date.
    """
    parser = argparse.ArgumentParser(description="Ingestão de dados brutos das APIs de Ads para a camada Bronze.")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=7),
        help="Data inicial no formato YYYY-MM-DD (padrão: 7 dias atrás).",
    )
    parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=date.today(),
        help="Data final no formato YYYY-MM-DD (padrão: hoje).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_ingestion(start_date=args.start_date, end_date=args.end_date)
