"""Testes do entrypoint run_transformation (find_latest_bronze_file, run_transformation)."""

from pathlib import Path

import polars as pl
import pytest

from pipeline.bronze_to_silver.run_transformation import (
    find_latest_bronze_file,
    run_transformation,
)

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def google_bronze_df() -> pl.DataFrame:
    """Bronze DataFrame mínimo compatível com GoogleAdsTransformer."""
    return pl.DataFrame(
        {
            "date": ["2026-03-01", "2026-03-02"],
            "campaign_name": ["Brand Awareness Q1", "Retargeting Summer"],
            "ad_group_name": ["Audience 18-34", "Lookalike 1%"],
            "impressions": [100_000, 200_000],
            "clicks": [5_000, 10_000],
            "cost_micros": [2_500_000_000, 5_000_000_000],
            "conversions": [250.0, 500.0],
            "currency": ["BRL", "BRL"],
            "_source": ["google_ads", "google_ads"],
            "_ingested_at": ["2026-03-12", "2026-03-12"],
        }
    )


@pytest.fixture
def meta_bronze_df() -> pl.DataFrame:
    """Bronze DataFrame mínimo compatível com MetaAdsTransformer."""
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
def tiktok_bronze_df() -> pl.DataFrame:
    """Bronze DataFrame mínimo compatível com TikTokAdsTransformer."""
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


# ─── Testes de find_latest_bronze_file ────────────────────────────────────────


class TestFindLatestBronzeFile:
    """Testes para a função find_latest_bronze_file."""

    def test_returns_none_when_directory_does_not_exist(self, tmp_path: Path) -> None:
        """Deve retornar None quando o diretório da fonte não existe."""
        result = find_latest_bronze_file(str(tmp_path), "google_ads")
        assert result is None

    def test_returns_none_when_directory_is_empty(self, tmp_path: Path) -> None:
        """Deve retornar None quando o diretório existe mas não tem .parquet."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        result = find_latest_bronze_file(str(tmp_path), "google_ads")
        assert result is None

    def test_returns_single_file_when_only_one_exists(self, tmp_path: Path) -> None:
        """Deve retornar o único arquivo .parquet quando apenas um existir."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        parquet_file = source_dir / "google_ads_20260312_100000.parquet"
        parquet_file.touch()

        result = find_latest_bronze_file(str(tmp_path), "google_ads")
        assert result == parquet_file

    def test_returns_lexicographically_latest_file(self, tmp_path: Path) -> None:
        """Deve retornar o arquivo com o maior nome (mais recente por ordenação lexicográfica)."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        files = [
            "google_ads_20260310_080000.parquet",
            "google_ads_20260311_120000.parquet",
            "google_ads_20260312_143000.parquet",  # ← mais recente
        ]
        for name in files:
            (source_dir / name).touch()

        result = find_latest_bronze_file(str(tmp_path), "google_ads")
        assert result is not None
        assert result.name == "google_ads_20260312_143000.parquet"

    def test_ignores_non_parquet_files(self, tmp_path: Path) -> None:
        """Deve ignorar arquivos que não sejam .parquet."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        (source_dir / "google_ads_20260312.parquet").touch()
        (source_dir / "google_ads_20260312.csv").touch()
        (source_dir / ".gitkeep").touch()

        result = find_latest_bronze_file(str(tmp_path), "google_ads")
        assert result is not None
        assert result.suffix == ".parquet"


# ─── Testes de run_transformation ─────────────────────────────────────────────


class TestRunTransformation:
    """Testes do orquestrador run_transformation."""

    def test_returns_none_for_all_sources_when_bronze_empty(self, tmp_path: Path) -> None:
        """Deve retornar None para todas as fontes quando não há Bronze files."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        bronze_root.mkdir()

        results = run_transformation(str(bronze_root), str(silver_root))

        assert isinstance(results, dict)
        assert results.get("google_ads") is None
        assert results.get("meta_ads") is None
        assert results.get("tiktok_ads") is None

    def test_transforms_google_ads_and_writes_silver(self, tmp_path: Path, google_bronze_df: pl.DataFrame) -> None:
        """Deve transformar o arquivo Bronze do Google Ads e gravar na Silver."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        google_dir = bronze_root / "google_ads"
        google_dir.mkdir(parents=True)
        google_bronze_df.write_parquet(google_dir / "google_ads_20260312_100000.parquet")

        results = run_transformation(str(bronze_root), str(silver_root))

        assert results.get("google_ads") is not None
        assert Path(results["google_ads"]).exists()  # type: ignore[arg-type]

    def test_transforms_meta_ads_and_writes_silver(self, tmp_path: Path, meta_bronze_df: pl.DataFrame) -> None:
        """Deve transformar o arquivo Bronze do Meta Ads e gravar na Silver."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        meta_dir = bronze_root / "meta_ads"
        meta_dir.mkdir(parents=True)
        meta_bronze_df.write_parquet(meta_dir / "meta_ads_20260312_100000.parquet")

        results = run_transformation(str(bronze_root), str(silver_root))

        assert results.get("meta_ads") is not None
        assert Path(results["meta_ads"]).exists()  # type: ignore[arg-type]

    def test_transforms_tiktok_ads_and_writes_silver(self, tmp_path: Path, tiktok_bronze_df: pl.DataFrame) -> None:
        """Deve transformar o arquivo Bronze do TikTok Ads e gravar na Silver."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        tiktok_dir = bronze_root / "tiktok_ads"
        tiktok_dir.mkdir(parents=True)
        tiktok_bronze_df.write_parquet(tiktok_dir / "tiktok_ads_20260312_100000.parquet")

        results = run_transformation(str(bronze_root), str(silver_root))

        assert results.get("tiktok_ads") is not None
        assert Path(results["tiktok_ads"]).exists()  # type: ignore[arg-type]

    def test_silver_parquet_has_canonical_columns(self, tmp_path: Path, google_bronze_df: pl.DataFrame) -> None:
        """O arquivo Silver deve conter exatamente as colunas do SILVER_COLUMN_ORDER."""
        from pipeline.bronze_to_silver.schema import SILVER_COLUMN_ORDER

        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        google_dir = bronze_root / "google_ads"
        google_dir.mkdir(parents=True)
        google_bronze_df.write_parquet(google_dir / "google_ads_20260312_100000.parquet")

        results = run_transformation(str(bronze_root), str(silver_root))
        silver_file = Path(results["google_ads"])  # type: ignore[arg-type]

        df = pl.read_parquet(silver_file)
        assert df.columns == SILVER_COLUMN_ORDER

    def test_partial_success_when_one_source_missing(self, tmp_path: Path, google_bronze_df: pl.DataFrame) -> None:
        """Deve processar fontes disponíveis mesmo que outras não tenham Bronze files."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        google_dir = bronze_root / "google_ads"
        google_dir.mkdir(parents=True)
        google_bronze_df.write_parquet(google_dir / "google_ads_20260312_100000.parquet")

        results = run_transformation(str(bronze_root), str(silver_root))

        assert results.get("google_ads") is not None
        assert results.get("meta_ads") is None
        assert results.get("tiktok_ads") is None

    def test_returns_dict_with_all_three_sources(self, tmp_path: Path) -> None:
        """O dicionário de resultado deve conter as três chaves mesmo sem dados."""
        bronze_root = tmp_path / "bronze"
        silver_root = tmp_path / "silver"
        bronze_root.mkdir()

        results = run_transformation(str(bronze_root), str(silver_root))

        assert set(results.keys()) == {"google_ads", "meta_ads", "tiktok_ads"}
