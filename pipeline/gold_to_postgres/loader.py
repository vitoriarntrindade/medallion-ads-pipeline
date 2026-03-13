"""Loader de dados Gold → PostgreSQL.

Implementa upsert idempotente usando INSERT ... ON CONFLICT DO UPDATE,
garantindo que re-execuções do pipeline atualizem os valores existentes
em vez de duplicar linhas.

Cada tabela tem sua chave natural definida no schema.py e usada
para a cláusula ON CONFLICT.
"""

from pathlib import Path

import polars as pl
import sqlalchemy as sa
from loguru import logger
from sqlalchemy.dialects.postgresql import insert as pg_insert

from pipeline.gold_to_postgres.schema import TABLES, metadata

# Colunas que compõem a chave natural de cada tabela (usadas no ON CONFLICT)
_CONFLICT_KEYS: dict[str, list[str]] = {
    "daily_summary": ["date", "source"],
    "campaign_summary": ["source", "campaign_name"],
    "source_comparison": ["source"],
}

# Colunas que devem ser atualizadas no ON CONFLICT (todas exceto as de chave e loaded_at)
_UPDATE_COLS: dict[str, list[str]] = {
    "daily_summary": ["impressions", "clicks", "cost_brl", "conversions", "ctr_pct", "cpc_brl", "cpm_brl", "cpa_brl"],
    "campaign_summary": [
        "impressions",
        "clicks",
        "cost_brl",
        "conversions",
        "ctr_pct",
        "cpc_brl",
        "cpm_brl",
        "cpa_brl",
    ],
    "source_comparison": [
        "impressions",
        "clicks",
        "cost_brl",
        "conversions",
        "ctr_pct",
        "cpc_brl",
        "cpm_brl",
        "cpa_brl",
    ],
}


def create_tables(engine: sa.Engine) -> None:
    """Cria todas as tabelas Gold no banco se ainda não existirem.

    Usa CREATE TABLE IF NOT EXISTS — seguro para re-execuções.

    Args:
        engine: Engine SQLAlchemy conectado ao PostgreSQL.
    """
    metadata.create_all(engine, checkfirst=True)
    logger.info("Tabelas Gold verificadas/criadas no PostgreSQL.")


def upsert_table(
    engine: sa.Engine,
    table_name: str,
    df: pl.DataFrame,
) -> int:
    """Executa upsert de um DataFrame Gold em uma tabela PostgreSQL.

    Converte o DataFrame Polars para lista de dicts e executa
    INSERT ... ON CONFLICT (chave_natural) DO UPDATE SET ...
    em lote dentro de uma única transação.

    Args:
        engine: Engine SQLAlchemy conectado ao PostgreSQL.
        table_name: Nome lógico da tabela ('daily_summary', etc).
        df: DataFrame Gold com os dados a inserir/atualizar.

    Returns:
        Número de linhas processadas.

    Raises:
        KeyError: Se table_name não for uma tabela Gold conhecida.
        sqlalchemy.exc.SQLAlchemyError: Em caso de falha na transação.
    """
    if table_name not in TABLES:
        raise KeyError(f"Tabela Gold desconhecida: '{table_name}'. Esperado: {list(TABLES)}")

    if df.is_empty():
        logger.warning(f"DataFrame vazio para '{table_name}'. Nada inserido.")
        return 0

    table = TABLES[table_name]
    conflict_keys = _CONFLICT_KEYS[table_name]
    update_cols = _UPDATE_COLS[table_name]

    # Converte para lista de dicts — tipos Python nativos compatíveis com SA
    records = df.to_pandas().to_dict(orient="records")

    stmt = pg_insert(table).values(records)
    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=conflict_keys,
        set_={col: stmt.excluded[col] for col in update_cols},
    )

    with engine.begin() as conn:
        conn.execute(upsert_stmt)

    logger.info(f"Upsert concluído | tabela=gold_{table_name} | linhas={len(records)}")
    return len(records)


def load_gold_parquet(
    engine: sa.Engine,
    table_name: str,
    parquet_path: Path,
) -> int:
    """Lê um arquivo Parquet Gold e executa upsert no PostgreSQL.

    Combina a leitura do Parquet com o upsert em uma única operação
    conveniente para uso no entrypoint CLI.

    Args:
        engine: Engine SQLAlchemy conectado ao PostgreSQL.
        table_name: Nome lógico da tabela Gold.
        parquet_path: Caminho do arquivo Parquet Gold a carregar.

    Returns:
        Número de linhas processadas.
    """
    logger.info(f"Lendo Parquet Gold | tabela={table_name} | arquivo={parquet_path.name}")
    df = pl.read_parquet(parquet_path)
    return upsert_table(engine, table_name, df)
