"""Entrypoint CLI da agregação Silver → Gold.

Lê os arquivos Silver mais recentes de cada fonte, gera as três
tabelas Gold e persiste em Parquet. Retorna exit code 1 se nenhum
dado Silver estiver disponível.

Uso:
    python -m pipeline.silver_to_gold.run_gold
    python -m pipeline.silver_to_gold.run_gold --silver-path storage/silver --gold-path storage/gold
"""

import argparse
import sys
from pathlib import Path

from loguru import logger

from pipeline.silver_to_gold.aggregations import (
    build_campaign_summary,
    build_daily_summary,
    build_source_comparison,
    load_silver_files,
)
from pipeline.silver_to_gold.gold_writer import write_gold

# ─── Configuração do logger ───────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
    level="DEBUG",
    colorize=True,
)
logger.add(
    "storage/logs/gold_{time:YYYYMMDD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="INFO",
    rotation="1 day",
    retention="7 days",
    encoding="utf-8",
)


def run_gold(silver_root: str, gold_root: str) -> dict[str, Path | None]:
    """Executa a agregação Silver → Gold para as três tabelas.

    Carrega todos os arquivos Silver disponíveis, gera as agregações
    e persiste cada tabela Gold em Parquet. Uma falha em uma tabela
    não interrompe as demais.

    Args:
        silver_root: Caminho raiz da camada Silver.
        gold_root: Caminho raiz da camada Gold.

    Returns:
        Dicionário mapeando nome da tabela para o Path gerado (ou None em falha).
    """
    logger.info("=" * 60)
    logger.info("Iniciando agregação Silver → Gold")
    logger.info("=" * 60)

    # ── Carrega e unifica todos os arquivos Silver ────────────────────────────
    df_silver = load_silver_files(silver_root)

    if df_silver.is_empty():
        logger.error("Nenhum arquivo Silver encontrado. Abortando agregação Gold.")
        return {"daily_summary": None, "campaign_summary": None, "source_comparison": None}

    logger.info(f"Silver carregado | linhas={len(df_silver)} | fontes={df_silver['source'].n_unique()}")

    # ── Tabelas e suas funções de agregação ───────────────────────────────────
    builders = {
        "daily_summary": build_daily_summary,
        "campaign_summary": build_campaign_summary,
        "source_comparison": build_source_comparison,
    }

    results: dict[str, Path | None] = {}

    for table_name, builder_fn in builders.items():
        try:
            df_gold = builder_fn(df_silver)
            output_path = write_gold(df=df_gold, table_name=table_name, gold_root=gold_root)
            results[table_name] = output_path
        except Exception as exc:
            logger.error(f"Falha ao gerar tabela Gold | tabela={table_name} | erro={exc}")
            results[table_name] = None

    # ── Sumário ───────────────────────────────────────────────────────────────
    successful = [t for t, p in results.items() if p is not None]
    failed = [t for t, p in results.items() if p is None]

    logger.info("=" * 60)
    logger.info(f"Gold finalizado | geradas={len(successful)} | falhas={len(failed)}")
    for table, path in results.items():
        status = "✓" if path else "✗"
        logger.info(f"  {status} {table}: {path or 'FALHOU'}")
    logger.info("=" * 60)

    return results


def parse_args() -> argparse.Namespace:
    """Processa argumentos de linha de comando.

    Returns:
        Namespace com silver_path e gold_path.
    """
    parser = argparse.ArgumentParser(description="Agregação Silver → Gold para as três tabelas de métricas.")
    parser.add_argument(
        "--silver-path",
        default="storage/silver",
        help="Caminho raiz da camada Silver (padrão: storage/silver).",
    )
    parser.add_argument(
        "--gold-path",
        default="storage/gold",
        help="Caminho raiz da camada Gold (padrão: storage/gold).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    results = run_gold(silver_root=args.silver_path, gold_root=args.gold_path)

    any_failed = any(p is None for p in results.values())
    sys.exit(1 if any_failed else 0)
