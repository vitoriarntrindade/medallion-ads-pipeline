"""Entrypoint CLI da validação de qualidade da camada Silver.

Localiza os arquivos Silver mais recentes de cada fonte e executa
a validação com Great Expectations. Retorna exit code 1 se qualquer
fonte falhar, tornando-o compatível com pipelines CI/CD.

Uso:
    python -m pipeline.validation.run_validation
    python -m pipeline.validation.run_validation --silver-path storage/silver
"""

import argparse
import sys
from pathlib import Path

from loguru import logger

from pipeline.validation.validator import ValidationReport, validate_silver_file

# ─── Configuração do logger ───────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="DEBUG",
    colorize=True,
)
logger.add(
    "storage/logs/validation_{time:YYYYMMDD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
)

_SOURCES = ["google_ads", "meta_ads", "tiktok_ads"]


def find_latest_silver_file(silver_root: str, source_name: str) -> Path | None:
    """Localiza o arquivo Parquet mais recente de uma fonte na Silver.

    Args:
        silver_root: Caminho raiz da camada Silver.
        source_name: Nome da fonte (ex: 'google_ads').

    Returns:
        Path do arquivo mais recente, ou None se não houver arquivos.
    """
    source_dir = Path(silver_root) / source_name
    if not source_dir.exists():
        logger.warning(f"Diretório Silver não encontrado: {source_dir}")
        return None

    parquet_files = sorted(source_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"Nenhum arquivo Parquet em: {source_dir}")
        return None

    return parquet_files[-1]


def run_validation(silver_root: str) -> dict[str, ValidationReport | None]:
    """Executa validação GX para todos os arquivos Silver mais recentes.

    Args:
        silver_root: Caminho raiz da camada Silver.

    Returns:
        Dicionário mapeando nome da fonte para o ValidationReport (ou None
        se não houver arquivo Silver para a fonte).
    """
    logger.info("=" * 60)
    logger.info("Iniciando validação Silver — Great Expectations")
    logger.info("=" * 60)

    reports: dict[str, ValidationReport | None] = {}

    for source in _SOURCES:
        silver_file = find_latest_silver_file(silver_root, source)
        if silver_file is None:
            logger.warning(f"Pulando fonte sem Silver file: {source}")
            reports[source] = None
            continue

        reports[source] = validate_silver_file(silver_file)

    # ── Sumário final ─────────────────────────────────────────────────────────
    validated = [s for s, r in reports.items() if r is not None]
    failed_sources = [s for s, r in reports.items() if r is not None and not r.success]

    logger.info("=" * 60)
    logger.info(
        f"Validação finalizada | fontes validadas={len(validated)} | "
        f"aprovadas={len(validated) - len(failed_sources)} | reprovadas={len(failed_sources)}"
    )
    if failed_sources:
        logger.warning(f"Fontes com falhas: {failed_sources}")
    logger.info("=" * 60)

    return reports


def parse_args() -> argparse.Namespace:
    """Processa argumentos de linha de comando.

    Returns:
        Namespace com silver_path.
    """
    parser = argparse.ArgumentParser(description="Validação de qualidade de dados da camada Silver.")
    parser.add_argument(
        "--silver-path",
        default="storage/silver",
        help="Caminho raiz da camada Silver (padrão: storage/silver).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    reports = run_validation(silver_root=args.silver_path)

    # Exit code 1 se qualquer fonte validada tiver falhado
    any_failed = any(r is not None and not r.success for r in reports.values())
    sys.exit(1 if any_failed else 0)
