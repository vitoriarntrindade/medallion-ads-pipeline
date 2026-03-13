"""Asset Dagster da camada Gold — agregações Silver → Gold.

Lê todos os arquivos Silver disponíveis, gera as três tabelas de
agregação (daily_summary, campaign_summary, source_comparison) e
persiste em Parquet na Gold.
"""

from dagster import AssetExecutionContext, asset

from observability.metrics import Timer, gold_metadata
from pipeline.silver_to_gold.run_gold import run_gold


@asset(
    name="gold_data",
    group_name="aggregation",
    description="Tabelas Gold agregadas (daily, campaign, source) em Parquet.",
    deps=["silver_validation"],
)
def gold_data(context: AssetExecutionContext) -> None:
    """Agrega os dados Silver nas três tabelas Gold.

    Depende de silver_validation para garantir que só dados válidos
    chegam à camada Gold. Uma falha em uma tabela não interrompe
    a geração das demais.

    Args:
        context: Contexto de execução do Dagster com logger e metadados.

    Raises:
        RuntimeError: Se nenhuma tabela Gold for gerada com sucesso.
    """
    silver_root = "storage/silver"
    gold_root = "storage/gold"

    context.log.info("Iniciando agregação Silver → Gold")

    with Timer() as t:
        results = run_gold(silver_root=silver_root, gold_root=gold_root)

    successful = [t for t, p in results.items() if p is not None]
    failed = [t for t, p in results.items() if p is None]

    for table, path in results.items():
        if path:
            context.log.info(f"  ✓ {table} → {path}")
        else:
            context.log.warning(f"  ✗ {table}: falhou")

    if not successful:
        raise RuntimeError("Nenhuma tabela Gold gerada. Verifique os dados Silver.")

    context.log.info(f"Gold concluído | geradas={len(successful)} | falhas={len(failed)}")
    context.add_output_metadata(gold_metadata(results, t.elapsed))
