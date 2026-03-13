"""Testes da camada Gold — agregações Silver → Gold (Phase 6)."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pipeline.silver_to_gold.aggregations import (
    build_campaign_summary,
    build_daily_summary,
    build_source_comparison,
    load_silver_files,
)
from pipeline.silver_to_gold.gold_writer import GOLD_TABLES, write_gold
from pipeline.silver_to_gold.run_gold import run_gold

# ─── Fixture base ─────────────────────────────────────────────────────────────


@pytest.fixture
def silver_df() -> pl.DataFrame:
    """DataFrame Silver realista com duas fontes e múltiplos dias."""
    return pl.DataFrame(
        {
            "date": [
                date(2026, 3, 1),
                date(2026, 3, 1),
                date(2026, 3, 2),
                date(2026, 3, 1),
                date(2026, 3, 2),
            ],
            "source": [
                "google_ads",
                "google_ads",
                "google_ads",
                "meta_ads",
                "meta_ads",
            ],
            "campaign_name": [
                "Brand Q1",
                "Retargeting",
                "Brand Q1",
                "Brand Q1",
                "Lead Gen",
            ],
            "ad_group_name": ["Grp A", "Grp B", "Grp A", "Grp X", "Grp Y"],
            "impressions": [100_000, 50_000, 80_000, 60_000, 40_000],
            "clicks": [5_000, 2_000, 4_000, 3_000, 1_500],
            "cost_brl": [2_500.0, 1_000.0, 2_000.0, 1_200.0, 600.0],
            "conversions": [250, 100, 200, 150, 75],
            "source_file": ["file.parquet"] * 5,
            "transformed_at": ["2026-03-12T10:00:00"] * 5,
        }
    )


# ─── Testes de build_daily_summary ────────────────────────────────────────────


class TestBuildDailySummary:
    """Testes da agregação diária."""

    def test_returns_dataframe(self, silver_df: pl.DataFrame) -> None:
        """Deve retornar um DataFrame."""
        result = build_daily_summary(silver_df)
        assert isinstance(result, pl.DataFrame)

    def test_groups_by_date_and_source(self, silver_df: pl.DataFrame) -> None:
        """Deve ter uma linha por (date, source)."""
        result = build_daily_summary(silver_df)
        # 2 fontes × 2 datas, mas meta_ads só tem 1 linha no dia 1
        assert len(result) == 4  # (google/01, google/02, meta/01, meta/02)

    def test_sums_impressions_per_day(self, silver_df: pl.DataFrame) -> None:
        """Impressions devem ser somadas por (date, source)."""
        result = build_daily_summary(silver_df)
        # google_ads dia 01: 100_000 + 50_000 = 150_000
        row = result.filter((pl.col("source") == "google_ads") & (pl.col("date") == date(2026, 3, 1)))
        assert row["impressions"][0] == 150_000

    def test_contains_derived_metrics(self, silver_df: pl.DataFrame) -> None:
        """Deve conter as quatro métricas derivadas."""
        result = build_daily_summary(silver_df)
        for col in ("ctr_pct", "cpc_brl", "cpm_brl", "cpa_brl"):
            assert col in result.columns

    def test_ctr_is_correct(self, silver_df: pl.DataFrame) -> None:
        """CTR deve ser clicks / impressions * 100."""
        result = build_daily_summary(silver_df)
        row = result.filter((pl.col("source") == "google_ads") & (pl.col("date") == date(2026, 3, 1)))
        # clicks=7000, impressions=150000 → CTR = 7000/150000 * 100 ≈ 4.6667
        expected_ctr = round(7_000 / 150_000 * 100, 4)
        assert row["ctr_pct"][0] == pytest.approx(expected_ctr, rel=1e-3)

    def test_cpc_is_correct(self, silver_df: pl.DataFrame) -> None:
        """CPC deve ser cost_brl / clicks."""
        result = build_daily_summary(silver_df)
        row = result.filter((pl.col("source") == "google_ads") & (pl.col("date") == date(2026, 3, 1)))
        # cost=3500, clicks=7000 → CPC = 0.5
        assert row["cpc_brl"][0] == pytest.approx(0.5, rel=1e-3)

    def test_cpm_is_correct(self, silver_df: pl.DataFrame) -> None:
        """CPM deve ser cost_brl / impressions * 1000."""
        result = build_daily_summary(silver_df)
        row = result.filter((pl.col("source") == "google_ads") & (pl.col("date") == date(2026, 3, 1)))
        # cost=3500, impressions=150000 → CPM = 3500/150000 * 1000 ≈ 23.3333
        expected_cpm = round(3_500 / 150_000 * 1000, 4)
        assert row["cpm_brl"][0] == pytest.approx(expected_cpm, rel=1e-3)

    def test_raises_for_empty_dataframe(self) -> None:
        """Deve lançar ValueError para DataFrame vazio."""
        with pytest.raises(ValueError, match="vazio"):
            build_daily_summary(pl.DataFrame())

    def test_raises_for_missing_columns(self) -> None:
        """Deve lançar ValueError se faltar colunas obrigatórias."""
        incomplete = pl.DataFrame({"date": [date(2026, 3, 1)], "source": ["google_ads"]})
        with pytest.raises(ValueError, match="ausentes"):
            build_daily_summary(incomplete)

    def test_result_is_sorted_by_date_and_source(self, silver_df: pl.DataFrame) -> None:
        """Resultado deve estar ordenado por (date, source)."""
        result = build_daily_summary(silver_df)
        dates = result["date"].to_list()
        assert dates == sorted(dates)


# ─── Testes de build_campaign_summary ────────────────────────────────────────


class TestBuildCampaignSummary:
    """Testes da agregação por campanha."""

    def test_groups_by_source_and_campaign(self, silver_df: pl.DataFrame) -> None:
        """Deve ter uma linha por (source, campaign_name)."""
        result = build_campaign_summary(silver_df)
        # google: Brand Q1, Retargeting; meta: Brand Q1, Lead Gen → 4 linhas
        assert len(result) == 4

    def test_accumulates_across_days(self, silver_df: pl.DataFrame) -> None:
        """Deve acumular métricas de todos os dias para a mesma campanha."""
        result = build_campaign_summary(silver_df)
        # google/Brand Q1: dia01=100_000 + dia02=80_000 = 180_000
        row = result.filter((pl.col("source") == "google_ads") & (pl.col("campaign_name") == "Brand Q1"))
        assert row["impressions"][0] == 180_000

    def test_contains_derived_metrics(self, silver_df: pl.DataFrame) -> None:
        """Deve conter as quatro métricas derivadas."""
        result = build_campaign_summary(silver_df)
        for col in ("ctr_pct", "cpc_brl", "cpm_brl", "cpa_brl"):
            assert col in result.columns

    def test_raises_for_empty_dataframe(self) -> None:
        """Deve lançar ValueError para DataFrame vazio."""
        with pytest.raises(ValueError, match="vazio"):
            build_campaign_summary(pl.DataFrame())


# ─── Testes de build_source_comparison ───────────────────────────────────────


class TestBuildSourceComparison:
    """Testes da agregação por fonte."""

    def test_one_row_per_source(self, silver_df: pl.DataFrame) -> None:
        """Deve ter exatamente uma linha por fonte."""
        result = build_source_comparison(silver_df)
        assert len(result) == 2  # google_ads e meta_ads

    def test_sorted_by_cost_descending(self, silver_df: pl.DataFrame) -> None:
        """Deve estar ordenado por cost_brl decrescente."""
        result = build_source_comparison(silver_df)
        costs = result["cost_brl"].to_list()
        assert costs == sorted(costs, reverse=True)

    def test_google_ads_total_cost(self, silver_df: pl.DataFrame) -> None:
        """Custo total do Google Ads deve ser soma de todas as linhas."""
        result = build_source_comparison(silver_df)
        row = result.filter(pl.col("source") == "google_ads")
        # 2500 + 1000 + 2000 = 5500
        assert row["cost_brl"][0] == pytest.approx(5_500.0)

    def test_contains_derived_metrics(self, silver_df: pl.DataFrame) -> None:
        """Deve conter as quatro métricas derivadas."""
        result = build_source_comparison(silver_df)
        for col in ("ctr_pct", "cpc_brl", "cpm_brl", "cpa_brl"):
            assert col in result.columns

    def test_raises_for_empty_dataframe(self) -> None:
        """Deve lançar ValueError para DataFrame vazio."""
        with pytest.raises(ValueError, match="vazio"):
            build_source_comparison(pl.DataFrame())


# ─── Testes de divisão por zero nas métricas ─────────────────────────────────


class TestDerivedMetricsEdgeCases:
    """Testes de comportamento de métricas em casos extremos."""

    @pytest.fixture
    def zero_clicks_df(self) -> pl.DataFrame:
        """DataFrame com clicks e conversions zerados (sem interações)."""
        return pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["tiktok_ads"],
                "campaign_name": ["Awareness"],
                "ad_group_name": ["Broad"],
                "impressions": [10_000],
                "clicks": [0],
                "cost_brl": [500.0],
                "conversions": [0],
                "source_file": ["file.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )

    def test_ctr_is_zero_when_no_impressions(self) -> None:
        """CTR deve ser 0.0 quando impressions = 0 (divisão por zero protegida)."""
        df = pl.DataFrame(
            {
                "date": [date(2026, 3, 1)],
                "source": ["google_ads"],
                "campaign_name": ["Test"],
                "ad_group_name": ["Grp"],
                "impressions": [0],
                "clicks": [0],
                "cost_brl": [0.0],
                "conversions": [0],
                "source_file": ["f.parquet"],
                "transformed_at": ["2026-03-12T10:00:00"],
            }
        )
        result = build_daily_summary(df)
        assert result["ctr_pct"][0] == 0.0

    def test_cpc_is_zero_when_no_clicks(self, zero_clicks_df: pl.DataFrame) -> None:
        """CPC deve ser 0.0 quando clicks = 0 (divisão por zero protegida)."""
        result = build_daily_summary(zero_clicks_df)
        assert result["cpc_brl"][0] == 0.0

    def test_cpa_is_zero_when_no_conversions(self, zero_clicks_df: pl.DataFrame) -> None:
        """CPA deve ser 0.0 quando conversions = 0 (divisão por zero protegida)."""
        result = build_daily_summary(zero_clicks_df)
        assert result["cpa_brl"][0] == 0.0


# ─── Testes de write_gold ─────────────────────────────────────────────────────


class TestWriteGold:
    """Testes do writer da camada Gold."""

    @pytest.fixture
    def gold_df(self) -> pl.DataFrame:
        """DataFrame Gold mínimo para testes de escrita."""
        return pl.DataFrame(
            {
                "source": ["google_ads"],
                "impressions": [100_000],
                "clicks": [5_000],
                "cost_brl": [2_500.0],
                "conversions": [250],
                "ctr_pct": [5.0],
                "cpc_brl": [0.5],
                "cpm_brl": [25.0],
                "cpa_brl": [10.0],
            }
        )

    def test_creates_parquet_file(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """Deve criar um arquivo .parquet."""
        output = write_gold(gold_df, "source_comparison", str(tmp_path))
        assert output.exists()
        assert output.suffix == ".parquet"

    def test_creates_table_subdirectory(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """Deve criar um subdiretório com o nome da tabela."""
        write_gold(gold_df, "source_comparison", str(tmp_path))
        assert (tmp_path / "source_comparison").is_dir()

    def test_filename_contains_timestamp(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """O nome do arquivo deve conter o timestamp formatado."""
        from datetime import datetime

        ts = datetime(2026, 3, 12, 15, 0, 0)
        output = write_gold(gold_df, "source_comparison", str(tmp_path), ts)
        assert "20260312_150000" in output.name

    def test_parquet_preserves_row_count(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """O arquivo salvo deve ter o mesmo número de linhas do DataFrame."""
        output = write_gold(gold_df, "source_comparison", str(tmp_path))
        assert len(pl.read_parquet(output)) == len(gold_df)

    def test_raises_for_empty_dataframe(self, tmp_path: Path) -> None:
        """Deve lançar ValueError para DataFrame vazio."""
        with pytest.raises(ValueError, match="vazio"):
            write_gold(pl.DataFrame(), "daily_summary", str(tmp_path))

    def test_raises_for_unknown_table_name(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """Deve lançar ValueError para nome de tabela desconhecido."""
        with pytest.raises(ValueError, match="desconhecida"):
            write_gold(gold_df, "tabela_inexistente", str(tmp_path))

    def test_all_known_tables_are_accepted(self, tmp_path: Path, gold_df: pl.DataFrame) -> None:
        """Deve aceitar todos os nomes de tabela definidos em GOLD_TABLES."""
        for table in GOLD_TABLES:
            output = write_gold(gold_df, table, str(tmp_path))
            assert output.exists()


# ─── Testes de load_silver_files ──────────────────────────────────────────────


class TestLoadSilverFiles:
    """Testes do carregador de arquivos Silver."""

    def test_returns_empty_dataframe_when_no_files(self, tmp_path: Path) -> None:
        """Deve retornar DataFrame vazio quando não há arquivos Silver."""
        result = load_silver_files(str(tmp_path))
        assert result.is_empty()

    def test_loads_single_source(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve carregar corretamente um único arquivo Silver."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        silver_df.filter(pl.col("source") == "google_ads").write_parquet(source_dir / "google_ads_20260312.parquet")

        result = load_silver_files(str(tmp_path))
        assert not result.is_empty()
        assert set(result["source"].unique().to_list()) == {"google_ads"}

    def test_loads_and_concatenates_multiple_sources(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve concatenar arquivos de múltiplas fontes em um único DataFrame."""
        for source in ["google_ads", "meta_ads"]:
            source_dir = tmp_path / source
            source_dir.mkdir()
            silver_df.filter(pl.col("source") == source).write_parquet(source_dir / f"{source}_20260312.parquet")

        result = load_silver_files(str(tmp_path))
        assert set(result["source"].unique().to_list()) == {"google_ads", "meta_ads"}

    def test_ignores_missing_source_directories(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve ignorar silenciosamente fontes sem diretório."""
        source_dir = tmp_path / "google_ads"
        source_dir.mkdir()
        silver_df.filter(pl.col("source") == "google_ads").write_parquet(source_dir / "google_ads_20260312.parquet")
        # meta_ads e tiktok_ads não existem — não deve lançar exceção

        result = load_silver_files(str(tmp_path))
        assert not result.is_empty()


# ─── Testes de run_gold ───────────────────────────────────────────────────────


class TestRunGold:
    """Testes do entrypoint de agregação Gold."""

    def test_returns_none_for_all_tables_when_no_silver(self, tmp_path: Path) -> None:
        """Deve retornar None para todas as tabelas quando não há Silver."""
        silver_root = tmp_path / "silver"
        gold_root = tmp_path / "gold"
        silver_root.mkdir()

        results = run_gold(str(silver_root), str(gold_root))

        assert results["daily_summary"] is None
        assert results["campaign_summary"] is None
        assert results["source_comparison"] is None

    def test_generates_all_three_gold_tables(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Deve gerar as três tabelas Gold quando Silver está disponível."""
        silver_root = tmp_path / "silver"
        gold_root = tmp_path / "gold"

        for source in ["google_ads", "meta_ads"]:
            source_dir = silver_root / source
            source_dir.mkdir(parents=True)
            silver_df.filter(pl.col("source") == source).write_parquet(source_dir / f"{source}_20260312.parquet")

        results = run_gold(str(silver_root), str(gold_root))

        assert results["daily_summary"] is not None
        assert results["campaign_summary"] is not None
        assert results["source_comparison"] is not None

    def test_gold_files_are_readable_parquets(self, tmp_path: Path, silver_df: pl.DataFrame) -> None:
        """Os arquivos Gold gerados devem ser Parquet válidos e legíveis."""
        silver_root = tmp_path / "silver"
        gold_root = tmp_path / "gold"

        source_dir = silver_root / "google_ads"
        source_dir.mkdir(parents=True)
        silver_df.filter(pl.col("source") == "google_ads").write_parquet(source_dir / "google_ads_20260312.parquet")

        results = run_gold(str(silver_root), str(gold_root))

        for table, path in results.items():
            if path is not None:
                df = pl.read_parquet(path)
                assert not df.is_empty(), f"Tabela {table} gerada vazia"

    def test_result_dict_has_all_three_keys(self, tmp_path: Path) -> None:
        """O dicionário de resultado deve sempre conter as três chaves."""
        silver_root = tmp_path / "silver"
        gold_root = tmp_path / "gold"
        silver_root.mkdir()

        results = run_gold(str(silver_root), str(gold_root))

        assert set(results.keys()) == {"daily_summary", "campaign_summary", "source_comparison"}
