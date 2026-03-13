"""Asset Dagster da camada Silver — transformação Bronze → Silver.

Lê os arquivos Parquet Bronze mais recentes de cada fonte, aplica
as transformações de schema e normalização, e persiste na Silver.
"""

from dagster import AssetExecutionContext, asset

from observability.metrics import Timer, transformation_metadata
from pipeline.bronze_to_silver.run_transformation import run_transformation


@asset(
    name="silver_data",
    group_name="transformation",
    description="Dados normalizados no schema Silver (Parquet), prontos para agregação.",
    deps=["bronze_data"],
)
def silver_data(context: AssetExecutionContext) -> None:
    """Transforma os dados Bronze para o schema Silver unificado.

    Localiza automaticamente os arquivos Bronze mais recentes de cada
    fonte e aplica o transformer correspondente. A falha em uma fonte
    não interrompe as demais.

    Args:
        context: Contexto de execução do Dagster com logger e metadados.
    """
    bronze_root = "storage/bronze"
    silver_root = "storage/silver"

    context.log.info("Iniciando transformação Bronze → Silver")

    with Timer() as t:
        results = run_transformation(bronze_root=bronze_root, silver_root=silver_root)

    successful = [src for src, path in results.items() if path is not None]
    failed = [src for src, path in results.items() if path is None]

    context.log.info(f"Silver concluído | sucesso={len(successful)} | falhas={len(failed)}")

    if failed:
        context.log.warning(f"Fontes sem Silver: {failed}")

    for source, path in results.items():
        if path:
            context.log.info(f"  ✓ {source} → {path}")

    context.add_output_metadata(transformation_metadata(results, t.elapsed))
