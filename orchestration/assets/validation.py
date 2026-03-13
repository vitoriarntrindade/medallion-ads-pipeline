"""Asset Dagster de validação da camada Silver com Great Expectations.

Executa as expectativas de qualidade de dados contra os arquivos
Silver mais recentes. O asset falha se qualquer fonte reprovar
nas validações, bloqueando a progressão para a Gold.
"""

from dagster import AssetExecutionContext, asset

from pipeline.validation.run_validation import run_validation


@asset(
    name="silver_validation",
    group_name="quality",
    description="Relatório de qualidade GX da camada Silver. Falha se dados inválidos.",
    deps=["silver_data"],
)
def silver_validation(context: AssetExecutionContext) -> None:
    """Valida a qualidade dos dados Silver com Great Expectations.

    Para cada fonte, executa o conjunto de expectativas definido em
    silver_suite.py. Se qualquer fonte reprovar, lança exceção para
    sinalizar ao Dagster que o pipeline deve parar.

    Args:
        context: Contexto de execução do Dagster com logger e metadados.

    Raises:
        ValueError: Se qualquer fonte Silver reprovar nas validações.
    """
    silver_root = "storage/silver"

    context.log.info("Iniciando validação Silver — Great Expectations")

    reports = run_validation(silver_root=silver_root)

    validated = [(src, r) for src, r in reports.items() if r is not None]
    failed_sources = [src for src, r in validated if not r.success]

    for source, report in validated:
        status = "✓ PASSOU" if report.success else "✗ FALHOU"
        context.log.info(
            f"  {status} | {source} | {report.passed}/{report.total_expectations} expectativas"
        )

    if failed_sources:
        raise ValueError(
            f"Validação Silver falhou para: {failed_sources}. Verifique os dados Bronze antes de prosseguir."
        )

    context.log.info("Validação Silver aprovada para todas as fontes.")
