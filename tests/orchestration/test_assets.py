"""Testes unitários dos assets e resources Dagster.

Valida o comportamento dos assets sem executar I/O real — usa mocks
para isolar as funções de pipeline subjacentes. Os testes de integração
completa são feitos manualmente via `dagster dev`.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetExecutionContext,
    build_asset_context,
    materialize,
)

from orchestration.assets.bronze import bronze_data
from orchestration.assets.gold import gold_data
from orchestration.assets.postgres import postgres_load
from orchestration.assets.silver import silver_data
from orchestration.assets.validation import silver_validation
from orchestration.resources.postgres import PostgresResource

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _mock_context() -> AssetExecutionContext:
    """Cria um contexto de execução de teste para assets Dagster."""
    return build_asset_context()


# ─── Testes do PostgresResource ───────────────────────────────────────────────


class TestPostgresResource:
    """Testes do resource de conexão com PostgreSQL."""

    def test_dsn_is_built_correctly(self) -> None:
        """O DSN deve ser montado corretamente a partir dos campos."""
        resource = PostgresResource(
            host="db-host",
            port=5433,
            database="mydb",
            user="admin",
            password="secret",
        )
        assert resource.dsn == "postgresql+psycopg2://admin:secret@db-host:5433/mydb"

    def test_default_values(self) -> None:
        """Os defaults devem corresponder ao ambiente local de desenvolvimento."""
        resource = PostgresResource()
        assert resource.host == "localhost"
        assert resource.port == 5432
        assert resource.database == "ad_analytics"

    def test_get_engine_returns_sa_engine(self) -> None:
        """get_engine() deve retornar um SQLAlchemy Engine válido."""
        import sqlalchemy as sa

        resource = PostgresResource()
        engine = resource.get_engine()
        assert isinstance(engine, sa.Engine)
        engine.dispose()


# ─── Testes do asset bronze_data ──────────────────────────────────────────────


class TestBronzeDataAsset:
    """Testes do asset de ingestão Bronze."""

    def test_materializes_successfully(self) -> None:
        """Deve materializar sem erros quando a ingestão tem sucesso."""
        mock_results = {
            "google_ads": Path("storage/bronze/google_ads/file.parquet"),
            "meta_ads": Path("storage/bronze/meta_ads/file.parquet"),
            "tiktok_ads": Path("storage/bronze/tiktok_ads/file.parquet"),
        }
        with patch(
            "orchestration.assets.bronze.run_ingestion",
            return_value=mock_results,
        ):
            result = materialize([bronze_data])
            assert result.success

    def test_logs_warning_for_failed_sources(self) -> None:
        """Deve logar warning quando alguma fonte falha, mas não lança exceção."""
        mock_results = {
            "google_ads": Path("storage/bronze/google_ads/file.parquet"),
            "meta_ads": None,  # falhou
            "tiktok_ads": None,  # falhou
        }
        with patch(
            "orchestration.assets.bronze.run_ingestion",
            return_value=mock_results,
        ):
            result = materialize([bronze_data])
            # Bronze não falha mesmo com fontes parciais
            assert result.success


# ─── Testes do asset silver_data ──────────────────────────────────────────────


class TestSilverDataAsset:
    """Testes do asset de transformação Silver."""

    def test_materializes_successfully(self) -> None:
        """Deve materializar sem erros quando a transformação tem sucesso."""
        mock_results = {
            "google_ads": Path("storage/silver/google_ads/file.parquet"),
            "meta_ads": Path("storage/silver/meta_ads/file.parquet"),
            "tiktok_ads": Path("storage/silver/tiktok_ads/file.parquet"),
        }
        with patch(
            "orchestration.assets.silver.run_transformation",
            return_value=mock_results,
        ):
            result = materialize([silver_data])
            assert result.success


# ─── Testes do asset silver_validation ───────────────────────────────────────


class TestSilverValidationAsset:
    """Testes do asset de validação Great Expectations."""

    def _make_report(self, success: bool, total: int = 10, passed: int = 10):
        """Cria um mock de ValidationReport."""
        report = MagicMock()
        report.success = success
        report.evaluated_expectations = total
        report.successful_expectations = passed
        return report

    def test_passes_when_all_sources_valid(self) -> None:
        """Não deve lançar exceção quando todas as fontes passam."""
        mock_reports = {
            "google_ads": self._make_report(success=True),
            "meta_ads": self._make_report(success=True),
            "tiktok_ads": self._make_report(success=True),
        }
        with patch(
            "orchestration.assets.validation.run_validation",
            return_value=mock_reports,
        ):
            result = materialize([silver_validation])
            assert result.success

    def test_fails_when_any_source_invalid(self) -> None:
        """Deve lançar ValueError quando qualquer fonte reprova nas validações."""
        mock_reports = {
            "google_ads": self._make_report(success=True),
            "meta_ads": self._make_report(success=False, passed=5),
            "tiktok_ads": self._make_report(success=True),
        }
        with patch(
            "orchestration.assets.validation.run_validation",
            return_value=mock_reports,
        ):
            with pytest.raises(ValueError, match="meta_ads"):
                materialize([silver_validation])

    def test_skips_sources_without_silver_file(self) -> None:
        """Deve tolerar fontes sem arquivo Silver (None no report)."""
        mock_reports = {
            "google_ads": self._make_report(success=True),
            "meta_ads": None,  # sem arquivo Silver
            "tiktok_ads": None,  # sem arquivo Silver
        }
        with patch(
            "orchestration.assets.validation.run_validation",
            return_value=mock_reports,
        ):
            result = materialize([silver_validation])
            assert result.success


# ─── Testes do asset gold_data ────────────────────────────────────────────────


class TestGoldDataAsset:
    """Testes do asset de agregação Gold."""

    def test_materializes_when_all_tables_generated(self) -> None:
        """Deve materializar sem erros quando todas as tabelas são geradas."""
        mock_results = {
            "daily_summary": Path("storage/gold/daily_summary/file.parquet"),
            "campaign_summary": Path("storage/gold/campaign_summary/file.parquet"),
            "source_comparison": Path("storage/gold/source_comparison/file.parquet"),
        }
        with patch(
            "orchestration.assets.gold.run_gold",
            return_value=mock_results,
        ):
            result = materialize([gold_data])
            assert result.success

    def test_fails_when_no_tables_generated(self) -> None:
        """Deve lançar RuntimeError quando nenhuma tabela Gold é gerada."""
        mock_results = {
            "daily_summary": None,
            "campaign_summary": None,
            "source_comparison": None,
        }
        with patch(
            "orchestration.assets.gold.run_gold",
            return_value=mock_results,
        ):
            with pytest.raises(RuntimeError, match="Nenhuma tabela Gold"):
                materialize([gold_data])

    def test_partial_success_does_not_fail(self) -> None:
        """Deve materializar mesmo com falha em tabelas individuais."""
        mock_results = {
            "daily_summary": Path("storage/gold/daily_summary/file.parquet"),
            "campaign_summary": None,  # falhou
            "source_comparison": Path("storage/gold/source_comparison/file.parquet"),
        }
        with patch(
            "orchestration.assets.gold.run_gold",
            return_value=mock_results,
        ):
            result = materialize([gold_data])
            assert result.success


# ─── Testes do asset postgres_load ───────────────────────────────────────────


class TestPostgresLoadAsset:
    """Testes do asset de carga Gold → PostgreSQL."""

    def _make_resource(self) -> PostgresResource:
        return PostgresResource(
            host="localhost",
            port=5432,
            database="test_db",
            user="postgres",
            password="postgres",
        )

    def test_materializes_when_all_tables_loaded(self) -> None:
        """Deve materializar sem erros quando todas as tabelas são carregadas."""
        mock_results = {
            "daily_summary": 10,
            "campaign_summary": 5,
            "source_comparison": 3,
        }
        with (
            patch("orchestration.assets.postgres.create_tables"),
            patch(
                "orchestration.assets.postgres.run_loader",
                return_value=mock_results,
            ),
        ):
            result = materialize(
                [postgres_load],
                resources={"postgres": self._make_resource()},
            )
            assert result.success

    def test_fails_when_no_tables_loaded(self) -> None:
        """Deve lançar RuntimeError quando nenhuma tabela é carregada."""
        mock_results = {
            "daily_summary": None,
            "campaign_summary": None,
            "source_comparison": None,
        }
        with (
            patch("orchestration.assets.postgres.create_tables"),
            patch(
                "orchestration.assets.postgres.run_loader",
                return_value=mock_results,
            ),
        ):
            with pytest.raises(RuntimeError, match="Nenhuma tabela Gold carregada"):
                materialize(
                    [postgres_load],
                    resources={"postgres": self._make_resource()},
                )

    def test_run_loader_receives_correct_dsn(self) -> None:
        """O run_loader deve receber o DSN gerado pelo resource."""
        resource = self._make_resource()
        captured_dsn = []

        def capture_dsn(gold_root: str, dsn: str | None = None):
            captured_dsn.append(dsn)
            return {"daily_summary": 2, "campaign_summary": 1, "source_comparison": 1}

        with (
            patch("orchestration.assets.postgres.create_tables"),
            patch(
                "orchestration.assets.postgres.run_loader",
                side_effect=capture_dsn,
            ),
        ):
            materialize(
                [postgres_load],
                resources={"postgres": resource},
            )

        assert len(captured_dsn) == 1
        assert captured_dsn[0] == resource.dsn
