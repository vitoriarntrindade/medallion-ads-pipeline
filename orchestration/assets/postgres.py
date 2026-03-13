"""Asset Dagster de carga Gold → PostgreSQL.

Lê os Parquet Gold mais recentes e executa upsert nas tabelas
do PostgreSQL via SQLAlchemy. Recebe o engine via PostgresResource
injetado pelo Dagster.
"""

from dagster import AssetExecutionContext, asset

from observability.metrics import Timer, postgres_metadata
from orchestration.resources.postgres import PostgresResource
from pipeline.gold_to_postgres.loader import create_tables
from pipeline.gold_to_postgres.run_loader import run_loader


@asset(
    name="postgres_load",
    group_name="serving",
    description="Carrega as tabelas Gold no PostgreSQL via upsert idempotente.",
    deps=["gold_data"],
)
def postgres_load(
    context: AssetExecutionContext,
    postgres: PostgresResource,
) -> None:
    """Carrega as tabelas Gold no PostgreSQL.

    Usa o PostgresResource injetado para obter o DSN de conexão,
    garantindo que o mesmo engine seja reutilizado entre assets
    dentro de uma execução.

    Args:
        context: Contexto de execução do Dagster com logger e metadados.
        postgres: Resource com configurações e engine PostgreSQL.

    Raises:
        RuntimeError: Se nenhuma tabela Gold for carregada com sucesso.
    """
    gold_root = "storage/gold"

    context.log.info("Iniciando carga Gold → PostgreSQL")

    engine = postgres.get_engine()
    create_tables(engine)
    engine.dispose()

    with Timer() as t:
        results = run_loader(gold_root=gold_root, dsn=postgres.dsn)

    successful = {t: r for t, r in results.items() if r is not None}
    missing = [t for t, r in results.items() if r is None]

    for table, rows in successful.items():
        context.log.info(f"  ✓ {table}: {rows} linhas carregadas")

    if missing:
        context.log.warning(f"Tabelas sem arquivo Gold: {missing}")

    if not successful:
        raise RuntimeError("Nenhuma tabela Gold carregada no PostgreSQL.")

    total = sum(successful.values())
    context.log.info(f"Carga concluída | total={total} linhas | tabelas={len(successful)}")
    context.add_output_metadata(postgres_metadata(results, t.elapsed))
