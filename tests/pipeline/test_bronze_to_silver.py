"""Testes unitários dos transformers Bronze → Silver."""

from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

from pipeline.bronze_to_silver.base_transformer import TransformationError
from pipeline.bronze_to_silver.google_ads import GoogleAdsTransformer
from pipeline.bronze_to_silver.meta_ads import MetaAdsTransformer
from pipeline.bronze_to_silver.schema import SILVER_COLUMN_ORDER, SilverRow
from pipeline.bronze_to_silver.silver_writer import write_silver
from pipeline.bronze_to_silver.tiktok_ads import TikTokAdsTransformer

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def google_ads_df() -> pl.DataFrame:
    """DataFrame simulando dados brutos do Google Ads na Bronze."""
    return pl.DataFrame(
        {
            "date": ["2026-03-01", "2026-03-02"],
            "campaign_name": ["Brand Awareness Q1", "Retargeting Summer"],
            "ad_group_name": ["Audience 18-34", "Lookalike 1%"],
            "impressions": [100_000, 200_000],
            "clicks": [5_000, 10_000],
            "cost_micros": [2_500_000_000, 5_000_000_000],  # 2500 e 5000 BRL
            "conversions": [250.0, 500.0],
            "currency": ["BRL", "BRL"],
            "_source": ["google_ads", "google_ads"],
            "_ingested_at": ["2026-03-12", "2026-03-12"],
        }
    )


@pytest.fixture
def meta_ads_df() -> pl.DataFrame:
    """DataFrame simulando dados brutos do Meta Ads na Bronze."""
    return pl.DataFrame(
        {
            "date_start": ["01/03/2026", "02/03/2026"],
            "date_stop": ["01/03/2026", "02/03/2026"],
            "campaign_name": ["Brand Awareness Q1", "Lead Gen B2B"],
            "adset_name": ["Broad Match", "Lookalike 1%"],
            "reach": [80_000, 150_000],
            "link_clicks": [4_000, 8_000],
            "spend": [1_200.50, 2_400.75],
            "results": [200, 400],
            "objective": ["CONVERSIONS", "LEAD_GENERATION"],
            "currency": ["BRL", "BRL"],
            "_source": ["meta_ads", "meta_ads"],
            "_ingested_at": ["2026-03-12", "2026-03-12"],
        }
    )


@pytest.fixture
def tiktok_ads_df() -> pl.DataFrame:
    """DataFrame simulando dados brutos do TikTok Ads na Bronze."""
    return pl.DataFrame(
        {
            "stat_time": ["2026-03-01 00:00:00", "2026-03-02 00:00:00"],
            "campaign_title": ["Holiday Sale", "App Install Campaign"],
            "ad_group_title": ["Interest Tech", "Remarketing Cart"],
            "views": [60_000, 120_000],
            "engagements": [3_000, 6_000],
            "total_cost": ["900.25", "1800.50"],
            "conversions_count": [150, 300],
            "campaign_objective": ["TRAFFIC", "APP_INSTALL"],
            "country_code": ["BR", "BR"],
            "_source": ["tiktok_ads", "tiktok_ads"],
            "_ingested_at": ["2026-03-12", "2026-03-12"],
        }
    )


# ─── Testes do SilverRow (schema) ─────────────────────────────────────────────


class TestSilverRowSchema:
    """Testes de validação do schema Pydantic da Silver."""

    def test_valid_row_is_accepted(self) -> None:
        """Deve aceitar uma linha válida sem erros."""
        row = SilverRow(
            date=date(2026, 3, 1),
            source="google_ads",
            campaign_name="Brand Awareness",
            ad_group_name="Audience 18-34",
            impressions=100_000,
            clicks=5_000,
            cost_brl=2_500.0,
            conversions=250,
            source_file="storage/bronze/google_ads/google_ads_20260312.parquet",
            transformed_at="2026-03-12T10:00:00",
        )
        assert row.source == "google_ads"

    def test_rejects_unknown_source(self) -> None:
        """Deve rejeitar fontes não reconhecidas."""
        with pytest.raises(Exception, match="Fonte desconhecida"):
            SilverRow(
                date=date(2026, 3, 1),
                source="unknown_platform",
                campaign_name="Test",
                ad_group_name="Test",
                impressions=1000,
                clicks=50,
                cost_brl=10.0,
                conversions=5,
                source_file="file.parquet",
                transformed_at="2026-03-12T10:00:00",
            )

    def test_rejects_negative_impressions(self) -> None:
        """Deve rejeitar impressões negativas."""
        with pytest.raises(Exception):
            SilverRow(
                date=date(2026, 3, 1),
                source="google_ads",
                campaign_name="Test",
                ad_group_name="Test",
                impressions=-1,
                clicks=50,
                cost_brl=10.0,
                conversions=5,
                source_file="file.parquet",
                transformed_at="2026-03-12T10:00:00",
            )

    def test_rejects_negative_cost(self) -> None:
        """Deve rejeitar custo negativo."""
        with pytest.raises(Exception):
            SilverRow(
                date=date(2026, 3, 1),
                source="meta_ads",
                campaign_name="Test",
                ad_group_name="Test",
                impressions=1000,
                clicks=50,
                cost_brl=-0.01,
                conversions=5,
                source_file="file.parquet",
                transformed_at="2026-03-12T10:00:00",
            )

    def test_cost_is_rounded_to_six_decimals(self) -> None:
        """cost_brl deve ser arredondado em 6 casas decimais."""
        row = SilverRow(
            date=date(2026, 3, 1),
            source="tiktok_ads",
            campaign_name="Test",
            ad_group_name="Test",
            impressions=1000,
            clicks=50,
            cost_brl=10.123456789,
            conversions=5,
            source_file="file.parquet",
            transformed_at="2026-03-12T10:00:00",
        )
        assert row.cost_brl == round(10.123456789, 6)


# ─── Testes do GoogleAdsTransformer ──────────────────────────────────────────


class TestGoogleAdsTransformer:
    """Testes do transformer de Google Ads."""

    def test_source_name_is_google_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'google_ads'."""
        assert GoogleAdsTransformer(str(tmp_path)).source_name == "google_ads"

    def test_transform_returns_dataframe(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """transform() deve retornar um DataFrame."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        assert isinstance(result, pl.DataFrame)

    def test_transform_produces_correct_columns(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """O resultado deve conter as colunas canônicas da Silver (sem metadados)."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        expected = {
            "date",
            "source",
            "campaign_name",
            "ad_group_name",
            "impressions",
            "clicks",
            "cost_brl",
            "conversions",
        }
        assert expected.issubset(set(result.columns))

    def test_transform_converts_cost_micros_to_brl(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """cost_micros deve ser dividido por 1.000.000 para gerar cost_brl."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        assert result["cost_brl"][0] == pytest.approx(2_500.0)
        assert result["cost_brl"][1] == pytest.approx(5_000.0)

    def test_transform_parses_date_string(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """O campo date deve ser do tipo pl.Date após a transformação."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        assert result["date"].dtype == pl.Date
        assert result["date"][0] == date(2026, 3, 1)

    def test_transform_casts_conversions_to_int(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """conversions deve ser Int64, não Float64."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        assert result["conversions"].dtype == pl.Int64

    def test_transform_adds_source_literal(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """A coluna source deve ser preenchida com 'google_ads'."""
        result = GoogleAdsTransformer(str(tmp_path)).transform(google_ads_df, "fake.parquet")
        assert all(v == "google_ads" for v in result["source"].to_list())

    def test_transform_raises_for_missing_columns(self, tmp_path: Path) -> None:
        """Deve lançar TransformationError quando faltar colunas obrigatórias."""
        incomplete_df = pl.DataFrame({"date": ["2026-03-01"], "campaign_name": ["Test"]})
        with pytest.raises(TransformationError):
            GoogleAdsTransformer(str(tmp_path)).transform(incomplete_df, "fake.parquet")

    def test_run_returns_dataframe_with_metadata(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """run() deve retornar DataFrame com source_file e transformed_at."""
        bronze_file = tmp_path / "google_ads_test.parquet"
        google_ads_df.write_parquet(bronze_file)

        result = GoogleAdsTransformer(str(tmp_path)).run(bronze_file)

        assert result is not None
        assert "source_file" in result.columns
        assert "transformed_at" in result.columns

    def test_run_enforces_column_order(self, tmp_path: Path, google_ads_df: pl.DataFrame) -> None:
        """run() deve retornar colunas na ordem canônica SILVER_COLUMN_ORDER."""
        bronze_file = tmp_path / "google_ads_test.parquet"
        google_ads_df.write_parquet(bronze_file)

        result = GoogleAdsTransformer(str(tmp_path)).run(bronze_file)

        assert result is not None
        assert result.columns == SILVER_COLUMN_ORDER


# ─── Testes do MetaAdsTransformer ────────────────────────────────────────────


class TestMetaAdsTransformer:
    """Testes do transformer de Meta Ads."""

    def test_source_name_is_meta_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'meta_ads'."""
        assert MetaAdsTransformer(str(tmp_path)).source_name == "meta_ads"

    def test_transform_renames_reach_to_impressions(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """reach deve ser renomeado para impressions."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert "impressions" in result.columns
        assert "reach" not in result.columns
        assert result["impressions"][0] == 80_000

    def test_transform_renames_link_clicks_to_clicks(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """link_clicks deve ser renomeado para clicks."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert "clicks" in result.columns
        assert "link_clicks" not in result.columns

    def test_transform_renames_spend_to_cost_brl(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """spend deve ser renomeado para cost_brl."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert "cost_brl" in result.columns
        assert result["cost_brl"][0] == pytest.approx(1_200.50)

    def test_transform_renames_results_to_conversions(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """results deve ser renomeado para conversions."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert "conversions" in result.columns
        assert "results" not in result.columns

    def test_transform_parses_european_date_format(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """date_start em DD/MM/YYYY deve ser parseado corretamente."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert result["date"].dtype == pl.Date
        assert result["date"][0] == date(2026, 3, 1)

    def test_transform_renames_adset_name_to_ad_group_name(self, tmp_path: Path, meta_ads_df: pl.DataFrame) -> None:
        """adset_name deve ser renomeado para ad_group_name."""
        result = MetaAdsTransformer(str(tmp_path)).transform(meta_ads_df, "fake.parquet")
        assert "ad_group_name" in result.columns
        assert "adset_name" not in result.columns

    def test_transform_raises_for_missing_columns(self, tmp_path: Path) -> None:
        """Deve lançar TransformationError quando faltar colunas obrigatórias."""
        incomplete_df = pl.DataFrame({"date_start": ["01/03/2026"]})
        with pytest.raises(TransformationError):
            MetaAdsTransformer(str(tmp_path)).transform(incomplete_df, "fake.parquet")


# ─── Testes do TikTokAdsTransformer ──────────────────────────────────────────


class TestTikTokAdsTransformer:
    """Testes do transformer de TikTok Ads."""

    def test_source_name_is_tiktok_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'tiktok_ads'."""
        assert TikTokAdsTransformer(str(tmp_path)).source_name == "tiktok_ads"

    def test_transform_extracts_date_from_timestamp(self, tmp_path: Path, tiktok_ads_df: pl.DataFrame) -> None:
        """stat_time (YYYY-MM-DD HH:MM:SS) deve gerar apenas a data."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert result["date"].dtype == pl.Date
        assert result["date"][0] == date(2026, 3, 1)

    def test_transform_renames_views_to_impressions(self, tmp_path: Path, tiktok_ads_df: pl.DataFrame) -> None:
        """views deve ser renomeado para impressions."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert "impressions" in result.columns
        assert "views" not in result.columns
        assert result["impressions"][0] == 60_000

    def test_transform_renames_engagements_to_clicks(self, tmp_path: Path, tiktok_ads_df: pl.DataFrame) -> None:
        """engagements deve ser renomeado para clicks."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert "clicks" in result.columns
        assert "engagements" not in result.columns

    def test_transform_parses_total_cost_string_to_float(self, tmp_path: Path, tiktok_ads_df: pl.DataFrame) -> None:
        """total_cost (string) deve ser convertido para float em cost_brl."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert result["cost_brl"].dtype == pl.Float64
        assert result["cost_brl"][0] == pytest.approx(900.25)

    def test_transform_renames_campaign_title_to_campaign_name(
        self, tmp_path: Path, tiktok_ads_df: pl.DataFrame
    ) -> None:
        """campaign_title deve ser renomeado para campaign_name."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert "campaign_name" in result.columns
        assert "campaign_title" not in result.columns

    def test_transform_renames_conversions_count_to_conversions(
        self, tmp_path: Path, tiktok_ads_df: pl.DataFrame
    ) -> None:
        """conversions_count deve ser renomeado para conversions."""
        result = TikTokAdsTransformer(str(tmp_path)).transform(tiktok_ads_df, "fake.parquet")
        assert "conversions" in result.columns
        assert "conversions_count" not in result.columns

    def test_transform_raises_for_missing_columns(self, tmp_path: Path) -> None:
        """Deve lançar TransformationError quando faltar colunas obrigatórias."""
        incomplete_df = pl.DataFrame({"stat_time": ["2026-03-01 00:00:00"]})
        with pytest.raises(TransformationError):
            TikTokAdsTransformer(str(tmp_path)).transform(incomplete_df, "fake.parquet")


# ─── Testes do silver_writer ──────────────────────────────────────────────────


class TestWriteSilver:
    """Testes do writer da camada Silver."""

    @pytest.fixture
    def silver_df(self) -> pl.DataFrame:
        """DataFrame no schema Silver para uso nos testes."""
        return pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["google_ads"],
                "campaign_name": ["Brand Awareness"],
                "ad_group_name": ["Audience 18-34"],
                "impressions": [100_000],
                "clicks": [5_000],
                "cost_brl": [2_500.0],
                "conversions": [250],
                "source_file": ["storage/bronze/google_ads/google_ads_20260312.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )

    def test_creates_parquet_file(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve criar um arquivo .parquet no diretório Silver."""
        output = write_silver(silver_df, "google_ads", str(tmp_path))
        assert output.exists()
        assert output.suffix == ".parquet"

    def test_creates_source_subdirectory(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve criar um subdiretório com o nome da fonte."""
        write_silver(silver_df, "google_ads", str(tmp_path))
        assert (tmp_path / "google_ads").is_dir()

    def test_parquet_preserves_row_count(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """O arquivo Parquet deve ter o mesmo número de linhas que o DataFrame."""
        output = write_silver(silver_df, "google_ads", str(tmp_path))
        assert len(pl.read_parquet(output)) == len(silver_df)

    def test_raises_for_empty_dataframe(self, tmp_path: Path) -> None:
        """Deve lançar ValueError para DataFrame vazio."""
        empty_df = pl.DataFrame({"date": [], "source": []})
        with pytest.raises(ValueError, match="vazio"):
            write_silver(empty_df, "google_ads", str(tmp_path))

    def test_filename_contains_timestamp(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """O nome do arquivo deve conter o timestamp formatado."""
        fixed_ts = datetime(2026, 3, 12, 14, 30, 0)
        output = write_silver(silver_df, "google_ads", str(tmp_path), fixed_ts)
        assert "20260312_143000" in output.name
