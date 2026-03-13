"""Asset Dagster da camada Bronze — ingestão das APIs de Ads.

Executa a extração de dados de todas as fontes (Google Ads, Meta Ads,
TikTok Ads) e persiste os resultados brutos em Parquet na Bronze.
"""

from datetime import date, timedelta

from dagster import AssetExecutionContext, asset

from ingestion.run_ingestion import run_ingestion
from observability.metrics import Timer, ingestion_metadata


@asset(
    name="bronze_data",
    group_name="ingestion",
    description="Dados brutos das APIs de Ads persistidos na camada Bronze (Parquet).",
)
def bronze_data(context: AssetExecutionContext) -> None:
    """Extrai dados de todas as fontes de Ads e salva na camada Bronze.

    Executa a ingestão para os últimos 7 dias por padrão. Cada fonte
    é processada independentemente — falhas em uma não bloqueiam as demais.

    Args:
        context: Contexto de execução do Dagster com logger e metadados.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=7)

    context.log.info(f"Iniciando ingestão | período={start_date} → {end_date}")

    with Timer() as t:
        results = run_ingestion(start_date=start_date, end_date=end_date)

    successful = [src for src, path in results.items() if path is not None]
    failed = [src for src, path in results.items() if path is None]

    context.log.info(f"Bronze concluído | sucesso={len(successful)} | falhas={len(failed)}")

    if failed:
        context.log.warning(f"Fontes sem dados Bronze: {failed}")

    for source, path in results.items():
        if path:
            context.log.info(f"  ✓ {source} → {path}")

    context.add_output_metadata(ingestion_metadata(results, t.elapsed))
