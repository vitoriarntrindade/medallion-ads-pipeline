"""Entrypoint CLI da carga Gold → PostgreSQL.

Localiza os arquivos Parquet Gold mais recentes de cada tabela,
cria as tabelas no banco se necessário e executa o upsert.

Uso:
    python -m pipeline.gold_to_postgres.run_loader
    python -m pipeline.gold_to_postgres.run_loader --gold-path storage/gold
"""

import argparse
import sys
from pathlib import Path

import sqlalchemy as sa
from loguru import logger

from pipeline.gold_to_postgres.loader import create_tables, load_gold_parquet
from pipeline.gold_to_postgres.schema import TABLES
from pipeline.gold_to_postgres.settings import get_settings

# ─── Configuração do logger ───────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="DEBUG",
    colorize=True,
)
logger.add(
    "storage/logs/postgres_loader_{time:YYYYMMDD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
)


def find_latest_gold_file(gold_root: str, table_name: str) -> Path | None:
    """Localiza o arquivo Parquet Gold mais recente de uma tabela.

    Args:
        gold_root: Caminho raiz da camada Gold.
        table_name: Nome lógico da tabela (ex: 'daily_summary').

    Returns:
        Path do arquivo mais recente, ou None se não houver arquivos.
    """
    table_dir = Path(gold_root) / table_name
    if not table_dir.exists():
        logger.warning(f"Diretório Gold não encontrado: {table_dir}")
        return None

    parquet_files = sorted(table_dir.glob("*.parquet"))
    if not parquet_files:
        logger.warning(f"Nenhum Parquet em: {table_dir}")
        return None

    return parquet_files[-1]


def run_loader(gold_root: str, dsn: str | None = None) -> dict[str, int | None]:
    """Executa a carga de todas as tabelas Gold no PostgreSQL.

    Para cada tabela Gold, localiza o Parquet mais recente e executa
    upsert. Uma falha em uma tabela não interrompe as demais.

    Args:
        gold_root: Caminho raiz da camada Gold.
        dsn: DSN de conexão. Se None, usa as configurações do ambiente.

    Returns:
        Dicionário mapeando nome da tabela para linhas inseridas (ou None em falha).
    """
    logger.info("=" * 60)
    logger.info("Iniciando carga Gold → PostgreSQL")
    logger.info("=" * 60)

    connection_dsn = dsn or get_settings().dsn
    engine = sa.create_engine(connection_dsn)

    try:
        create_tables(engine)
    except Exception as exc:
        logger.error(f"Falha ao criar tabelas: {exc}")
        return {t: None for t in TABLES}

    results: dict[str, int | None] = {}

    for table_name in TABLES:
        parquet_path = find_latest_gold_file(gold_root, table_name)
        if parquet_path is None:
            logger.warning(f"Sem arquivo Gold para '{table_name}'. Pulando.")
            results[table_name] = None
            continue

        try:
            rows = load_gold_parquet(engine, table_name, parquet_path)
            results[table_name] = rows
        except Exception as exc:
            logger.error(f"Falha na carga | tabela={table_name} | erro={exc}")
            results[table_name] = None

    # ── Sumário ───────────────────────────────────────────────────────────────
    successful = {t: r for t, r in results.items() if r is not None}
    failed = [t for t, r in results.items() if r is None]

    logger.info("=" * 60)
    logger.info(f"Carga finalizada | sucesso={len(successful)} | falhas={len(failed)}")
    for table, rows in results.items():
        status = "✓" if rows is not None else "✗"
        logger.info(f"  {status} {table}: {rows if rows is not None else 'FALHOU'} linhas")
    logger.info("=" * 60)

    engine.dispose()
    return results


def parse_args() -> argparse.Namespace:
    """Processa argumentos de linha de comando.

    Returns:
        Namespace com gold_path.
    """
    parser = argparse.ArgumentParser(description="Carga das tabelas Gold no PostgreSQL.")
    parser.add_argument(
        "--gold-path",
        default="storage/gold",
        help="Caminho raiz da camada Gold (padrão: storage/gold).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    results = run_loader(gold_root=args.gold_path)

    any_failed = any(r is None for r in results.values())
    sys.exit(1 if any_failed else 0)
