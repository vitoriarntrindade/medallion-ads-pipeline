"""Testes da camada de validação com Great Expectations (Phase 5)."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pipeline.validation.run_validation import find_latest_silver_file, run_validation
from pipeline.validation.silver_suite import SUITE_NAME, build_silver_suite
from pipeline.validation.validator import (
    ExpectationFailure,
    ValidationReport,
    validate_silver_file,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def valid_silver_df() -> pl.DataFrame:
    """DataFrame Silver 100% válido conforme as Expectations."""
    return pl.DataFrame(
        {
            "date": [date(2026, 3, 1), date(2026, 3, 2)],
            "source": ["google_ads", "google_ads"],
            "campaign_name": ["Brand Awareness Q1", "Retargeting Summer"],
            "ad_group_name": ["Audience 18-34", "Lookalike 1%"],
            "impressions": [100_000, 200_000],
            "clicks": [5_000, 10_000],
            "cost_brl": [2_500.0, 5_000.0],
            "conversions": [250, 500],
            "source_file": ["bronze/google_ads_20260312.parquet"] * 2,
            "transformed_at": ["2026-03-12T10:00:00"] * 2,
        }
    )


@pytest.fixture
def silver_parquet(tmp_path: Path, valid_silver_df: pl.DataFrame) -> Path:
    """Arquivo Parquet Silver válido salvo em tmp_path."""
    source_dir = tmp_path / "google_ads"
    source_dir.mkdir()
    file_path = source_dir / "google_ads_20260312_100000.parquet"
    valid_silver_df.write_parquet(file_path)
    return file_path


# ─── Testes de build_silver_suite ─────────────────────────────────────────────


class TestBuildSilverSuite:
    """Testes da função que constrói a ExpectationSuite."""

    @pytest.fixture
    def ctx(self):
        """DataContext GX efêmero isolado por teste."""
        import great_expectations as gx

        return gx.get_context(mode="ephemeral")

    def test_returns_expectation_suite(self, ctx) -> None:
        """Deve retornar uma instância de ExpectationSuite."""
        import great_expectations as gx

        suite = build_silver_suite(ctx)
        assert isinstance(suite, gx.ExpectationSuite)

    def test_suite_has_correct_name(self, ctx) -> None:
        """O nome da suite deve ser SUITE_NAME."""
        suite = build_silver_suite(ctx)
        assert suite.name == SUITE_NAME

    def test_suite_has_expectations(self, ctx) -> None:
        """A suite deve ter pelo menos 10 Expectations definidas."""
        suite = build_silver_suite(ctx)
        assert len(suite.expectations) >= 10

    def test_suite_includes_not_null_for_date(self, ctx) -> None:
        """Deve haver uma Expectation de não-nulidade para 'date'."""
        suite = build_silver_suite(ctx)
        types = [e.expectation_type for e in suite.expectations]
        assert "expect_column_values_to_not_be_null" in types

    def test_suite_includes_set_check_for_source(self, ctx) -> None:
        """Deve haver uma Expectation de conjunto de valores para 'source'."""
        suite = build_silver_suite(ctx)
        types = [e.expectation_type for e in suite.expectations]
        assert "expect_column_values_to_be_in_set" in types


# ─── Testes de validate_silver_file ───────────────────────────────────────────


class TestValidateSilverFile:
    """Testes do validador principal."""

    def test_valid_file_returns_success(self, silver_parquet: Path) -> None:
        """Arquivo Silver válido deve retornar ValidationReport com success=True."""
        report = validate_silver_file(silver_parquet)
        assert report.success is True

    def test_returns_validation_report_type(self, silver_parquet: Path) -> None:
        """Deve retornar uma instância de ValidationReport."""
        report = validate_silver_file(silver_parquet)
        assert isinstance(report, ValidationReport)

    def test_valid_file_has_no_failures(self, silver_parquet: Path) -> None:
        """Arquivo Silver válido não deve ter nenhuma falha na lista."""
        report = validate_silver_file(silver_parquet)
        assert report.failures == []

    def test_valid_file_passed_equals_total(self, silver_parquet: Path) -> None:
        """Para um arquivo válido, passed deve ser igual a total_expectations."""
        report = validate_silver_file(silver_parquet)
        assert report.passed == report.total_expectations
        assert report.failed == 0

    def test_source_file_path_in_report(self, silver_parquet: Path) -> None:
        """O relatório deve registrar o caminho do arquivo validado."""
        report = validate_silver_file(silver_parquet)
        assert str(silver_parquet) in report.source_file

    def test_invalid_source_value_fails(self, tmp_path: Path) -> None:
        """
        Arquivo com 'source' desconhecida deve gerar ValidationReport com success=False.
        """

        bad_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["unknown_platform"],  # ← inválido
                "campaign_name": ["Test"],
                "ad_group_name": ["Group"],
                "impressions": [1000],
                "clicks": [100],
                "cost_brl": [50.0],
                "conversions": [10],
                "source_file": ["bronze/file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        bad_file = tmp_path / "bad.parquet"
        bad_df.write_parquet(bad_file)

        report = validate_silver_file(bad_file)
        assert report.success is False
        assert report.failed > 0

    def test_null_values_detected(self, tmp_path: Path) -> None:
        """Arquivo com valores nulos em colunas obrigatórias deve falhar."""
        null_df = pl.DataFrame(
            {
                "date": [None],
                "source": ["google_ads"],
                "campaign_name": [None],  # ← nulo inválido
                "ad_group_name": ["Group"],
                "impressions": [1000],
                "clicks": [100],
                "cost_brl": [50.0],
                "conversions": [10],
                "source_file": ["bronze/file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        null_file = tmp_path / "null.parquet"
        null_df.write_parquet(null_file)

        report = validate_silver_file(null_file)
        assert report.success is False

    def test_clicks_greater_than_impressions_fails(self, tmp_path: Path) -> None:
        """clicks > impressions deve violar a Expectation de consistência."""
        inconsistent_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["google_ads"],
                "campaign_name": ["Test"],
                "ad_group_name": ["Group"],
                "impressions": [100],
                "clicks": [999],  # ← maior que impressions: inválido
                "cost_brl": [50.0],
                "conversions": [10],
                "source_file": ["bronze/file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        bad_file = tmp_path / "inconsistent.parquet"
        inconsistent_df.write_parquet(bad_file)

        report = validate_silver_file(bad_file)
        assert report.success is False

    def test_conversions_greater_than_clicks_fails(self, tmp_path: Path) -> None:
        """conversions > clicks deve violar a Expectation de consistência."""
        inconsistent_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["google_ads"],
                "campaign_name": ["Test"],
                "ad_group_name": ["Group"],
                "impressions": [10_000],
                "clicks": [100],
                "cost_brl": [50.0],
                "conversions": [999],  # ← maior que clicks: inválido
                "source_file": ["bronze/file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        bad_file = tmp_path / "inconsistent2.parquet"
        inconsistent_df.write_parquet(bad_file)

        report = validate_silver_file(bad_file)
        assert report.success is False

    def test_negative_cost_fails(self, tmp_path: Path) -> None:
        """cost_brl negativo deve violar a Expectation de faixa."""
        bad_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["google_ads"],
                "campaign_name": ["Test"],
                "ad_group_name": ["Group"],
                "impressions": [1000],
                "clicks": [100],
                "cost_brl": [-1.0],  # ← negativo: inválido
                "conversions": [10],
                "source_file": ["bronze/file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        bad_file = tmp_path / "neg_cost.parquet"
        bad_df.write_parquet(bad_file)

        report = validate_silver_file(bad_file)
        assert report.success is False

    def test_file_not_found_returns_failure_report(self, tmp_path: Path) -> None:
        """
        Arquivo inexistente deve retornar um ValidationReport de falha sem lançar exceção.
        """
        missing_file = tmp_path / "nao_existe.parquet"
        report = validate_silver_file(missing_file)
        assert report.success is False
        assert report.failed >= 1

    def test_failure_summary_string_is_readable(self, tmp_path: Path) -> None:
        """O summary() de um ValidationReport deve ser uma string não-vazia."""
        bad_df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["unknown"],
                "campaign_name": ["Test"],
                "ad_group_name": ["Group"],
                "impressions": [1000],
                "clicks": [100],
                "cost_brl": [50.0],
                "conversions": [10],
                "source_file": ["file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        f = tmp_path / "bad2.parquet"
        bad_df.write_parquet(f)
        report = validate_silver_file(f)
        summary = report.summary()
        assert isinstance(summary, str)
        assert len(summary) > 0
        assert "FALHOU" in summary


# ─── Testes de ExpectationFailure ─────────────────────────────────────────────


class TestExpectationFailure:
    """Testes do dataclass ExpectationFailure."""

    def test_str_includes_expectation_type(self) -> None:
        """__str__ deve incluir o tipo da Expectation."""
        failure = ExpectationFailure(
            expectation_type="expect_column_values_to_not_be_null",
            column="date",
            description="Não pode ser nulo.",
            unexpected_count=5,
            unexpected_percent=10.0,
        )
        assert "expect_column_values_to_not_be_null" in str(failure)

    def test_str_includes_column_name(self) -> None:
        """__str__ deve incluir o nome da coluna quando disponível."""
        failure = ExpectationFailure(
            expectation_type="expect_column_values_to_not_be_null",
            column="campaign_name",
            description="Não pode ser nulo.",
            unexpected_count=1,
            unexpected_percent=50.0,
        )
        assert "campaign_name" in str(failure)

    def test_str_without_column_does_not_crash(self) -> None:
        """__str__ não deve lançar exceção quando column é None."""
        failure = ExpectationFailure(
            expectation_type="expect_table_columns_to_match_set",
            column=None,
            description="Colunas obrigatórias devem existir.",
            unexpected_count=None,
            unexpected_percent=None,
        )
        result = str(failure)
        assert isinstance(result, str)


# ─── Testes de find_latest_silver_file e run_validation ───────────────────────


class TestRunValidation:
    """Testes do entrypoint de validação."""

    def test_find_latest_returns_none_for_missing_dir(self, tmp_path: Path) -> None:
        """Deve retornar None quando o diretório da fonte não existe."""
        result = find_latest_silver_file(str(tmp_path), "google_ads")
        assert result is None

    def test_find_latest_returns_most_recent_file(self, tmp_path: Path) -> None:
        """Deve retornar o arquivo com maior nome (mais recente)."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        (source_dir / "google_ads_20260310_080000.parquet").touch()
        (source_dir / "google_ads_20260312_143000.parquet").touch()

        result = find_latest_silver_file(str(tmp_path), "google_ads")
        assert result is not None
        assert result.name == "google_ads_20260312_143000.parquet"

    def test_run_validation_returns_none_for_missing_sources(self, tmp_path: Path) -> None:
        """Deve retornar None para fontes sem Silver file."""
        reports = run_validation(silver_root=str(tmp_path))
        assert reports.get("google_ads") is None
        assert reports.get("meta_ads") is None
        assert reports.get("tiktok_ads") is None

    def test_run_validation_returns_report_for_valid_file(self, tmp_path: Path, valid_silver_df: pl.DataFrame) -> None:
        """Deve retornar ValidationReport para fontes com Silver válido."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        valid_silver_df.write_parquet(source_dir / "google_ads_20260312_100000.parquet")

        reports = run_validation(silver_root=str(tmp_path))

        assert reports["google_ads"] is not None
        assert reports["google_ads"].success is True  # type: ignore[union-attr]

    def test_run_validation_dict_has_all_three_keys(self, tmp_path: Path) -> None:
        """O dicionário de resultado deve sempre ter as três chaves."""
        reports = run_validation(silver_root=str(tmp_path))
        assert set(reports.keys()) == {"google_ads", "meta_ads", "tiktok_ads"}
