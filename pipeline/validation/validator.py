"""Validador de qualidade para arquivos Parquet da camada Silver.

Orquestra o Great Expectations em modo Ephemeral (sem DataContext em disco),
valida um DataFrame contra a silver_suite e devolve um resultado estruturado.

Uso típico:
    from pipeline.validation.validator import validate_silver_file, ValidationReport

    report = validate_silver_file(Path("storage/silver/google_ads/google_ads_20260312.parquet"))
    if not report.success:
        for failure in report.failures:
            print(failure)
"""

from dataclasses import dataclass, field
from pathlib import Path

import great_expectations as gx
import polars as pl
from loguru import logger

from pipeline.validation.silver_suite import build_silver_suite


@dataclass
class ExpectationFailure:
    """Representa uma Expectation que falhou na validação.

    Attributes:
        expectation_type: Tipo da Expectation (ex: 'expect_column_values_to_not_be_null').
        column: Coluna avaliada (quando aplicável).
        description: Descrição legível da regra.
        unexpected_count: Número de valores que violaram a regra.
        unexpected_percent: Percentual de valores que violaram a regra.
    """

    expectation_type: str
    column: str | None
    description: str
    unexpected_count: int | None
    unexpected_percent: float | None

    def __str__(self) -> str:
        """Formata a falha como string legível para logs."""
        col_info = f" | coluna='{self.column}'" if self.column else ""
        count_info = (
            f" | {self.unexpected_count} valores inválidos ({self.unexpected_percent:.1f}%)"
            if self.unexpected_count is not None
            else ""
        )
        return f"FALHOU: {self.expectation_type}{col_info}{count_info} — {self.description}"


@dataclass
class ValidationReport:
    """Resultado agregado de uma validação de arquivo Silver.

    Attributes:
        source_file: Caminho do arquivo validado.
        success: True se todas as Expectations passaram.
        total_expectations: Número total de regras avaliadas.
        passed: Número de regras que passaram.
        failed: Número de regras que falharam.
        failures: Lista detalhada de falhas.
    """

    source_file: str
    success: bool
    total_expectations: int
    passed: int
    failed: int
    failures: list[ExpectationFailure] = field(default_factory=list)

    def summary(self) -> str:
        """Retorna um resumo de uma linha para uso em logs.

        Returns:
            String formatada com os contadores principais.
        """
        status = "✓ PASSOU" if self.success else "✗ FALHOU"
        return (
            f"{status} | arquivo={self.source_file} "
            f"| total={self.total_expectations} "
            f"| passou={self.passed} | falhou={self.failed}"
        )


def _extract_failures(gx_results: list) -> list[ExpectationFailure]:
    """Extrai falhas de uma lista de resultados GX.

    Args:
        gx_results: Lista de ExpectationValidationResult do GX.

    Returns:
        Lista de ExpectationFailure para as Expectations que falharam.
    """
    failures = []
    for r in gx_results:
        if r.success:
            continue

        cfg = r.expectation_config
        result_dict = r.result or {}

        # Tenta extrair o nome da coluna do kwargs da Expectation
        kwargs = cfg.kwargs if hasattr(cfg, "kwargs") else {}
        column = kwargs.get("column") or kwargs.get("column_A")

        failures.append(
            ExpectationFailure(
                expectation_type=cfg.type,
                column=column,
                description=cfg.description or "",
                unexpected_count=result_dict.get("unexpected_count"),
                unexpected_percent=result_dict.get("unexpected_percent"),
            )
        )
    return failures


def validate_silver_file(silver_file: Path) -> ValidationReport:
    """Valida um arquivo Parquet Silver contra a suite de qualidade.

    Carrega o arquivo, converte para Pandas (necessário para o GX Pandas
    datasource), executa todas as Expectations e retorna um ValidationReport
    estruturado. Não lança exceções — qualquer erro interno é capturado e
    refletido no relatório.

    Args:
        silver_file: Caminho do arquivo .parquet na camada Silver.

    Returns:
        ValidationReport com o resultado completo da validação.
    """
    logger.info(f"Iniciando validação | arquivo={silver_file.name}")

    try:
        df_polars = pl.read_parquet(silver_file)
        df_pandas = df_polars.to_pandas()
    except Exception as exc:
        logger.error(f"Falha ao ler arquivo Silver: {exc}")
        return ValidationReport(
            source_file=str(silver_file),
            success=False,
            total_expectations=0,
            passed=0,
            failed=1,
            failures=[
                ExpectationFailure(
                    expectation_type="file_read_error",
                    column=None,
                    description=str(exc),
                    unexpected_count=None,
                    unexpected_percent=None,
                )
            ],
        )

    # ── Monta o contexto GX efêmero ──────────────────────────────────────────
    ctx = gx.get_context(mode="ephemeral")

    data_source = ctx.data_sources.add_pandas("silver_ds")
    data_asset = data_source.add_dataframe_asset("silver_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("silver_batch")

    suite = build_silver_suite(ctx)

    validation_definition = ctx.validation_definitions.add(
        gx.ValidationDefinition(
            name=f"validate_{silver_file.stem}",
            data=batch_definition,
            suite=suite,
        )
    )

    # ── Executa a validação ──────────────────────────────────────────────────
    gx_result = validation_definition.run(batch_parameters={"dataframe": df_pandas})

    # ── Monta o relatório estruturado ────────────────────────────────────────
    all_results = gx_result.results
    failures = _extract_failures(all_results)

    report = ValidationReport(
        source_file=str(silver_file),
        success=gx_result.success,
        total_expectations=len(all_results),
        passed=len(all_results) - len(failures),
        failed=len(failures),
        failures=failures,
    )

    # ── Log do resultado ─────────────────────────────────────────────────────
    if report.success:
        logger.success(report.summary())
    else:
        logger.warning(report.summary())
        for failure in report.failures:
            logger.warning(f"  {failure}")

    return report
