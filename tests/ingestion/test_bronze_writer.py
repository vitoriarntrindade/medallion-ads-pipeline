"""Testes unitários do bronze_writer."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from ingestion.bronze_writer import write_bronze


@pytest.fixture
def sample_records() -> list[dict]:
    """Retorna uma lista de registros mock para uso nos testes.

    Returns:
        Lista com 3 registros representando dados brutos de uma fonte.
    """
    return [
        {
            "date": "2026-03-01",
            "campaign_name": "Test Campaign",
            "impressions": 10_000,
            "clicks": 500,
            "cost_micros": 1_500_000_000,
            "_source": "google_ads",
            "_ingested_at": "2026-03-12",
        },
        {
            "date": "2026-03-02",
            "campaign_name": "Test Campaign",
            "impressions": 8_000,
            "clicks": 400,
            "cost_micros": 1_200_000_000,
            "_source": "google_ads",
            "_ingested_at": "2026-03-12",
        },
        {
            "date": "2026-03-03",
            "campaign_name": "Another Campaign",
            "impressions": 12_000,
            "clicks": 600,
            "cost_micros": 1_800_000_000,
            "_source": "google_ads",
            "_ingested_at": "2026-03-12",
        },
    ]


class TestWriteBronze:
    """Testes do writer de dados brutos na camada Bronze."""

    def test_creates_parquet_file(self, tmp_path: Path, sample_records: list) -> None:
        """Deve criar um arquivo .parquet no diretório de destino."""
        output = write_bronze(sample_records, "google_ads", str(tmp_path))
        assert output.exists()
        assert output.suffix == ".parquet"

    def test_creates_source_subdirectory(self, tmp_path: Path, sample_records: list) -> None:
        """Deve criar um subdiretório com o nome da fonte."""
        write_bronze(sample_records, "google_ads", str(tmp_path))
        assert (tmp_path / "google_ads").is_dir()

    def test_filename_contains_source_name(self, tmp_path: Path, sample_records: list) -> None:
        """O nome do arquivo deve conter o nome da fonte."""
        output = write_bronze(sample_records, "meta_ads", str(tmp_path))
        assert "meta_ads" in output.name

    def test_filename_contains_timestamp(self, tmp_path: Path, sample_records: list) -> None:
        """O nome do arquivo deve conter um timestamp."""
        fixed_ts = datetime(2026, 3, 12, 10, 30, 0)
        output = write_bronze(sample_records, "google_ads", str(tmp_path), ingestion_timestamp=fixed_ts)
        assert "20260312_103000" in output.name

    def test_parquet_has_correct_row_count(self, tmp_path: Path, sample_records: list) -> None:
        """O arquivo Parquet deve ter o mesmo número de linhas que os records."""
        output = write_bronze(sample_records, "google_ads", str(tmp_path))
        df = pl.read_parquet(output)
        assert len(df) == len(sample_records)

    def test_parquet_has_correct_columns(self, tmp_path: Path, sample_records: list) -> None:
        """O arquivo Parquet deve conter todas as colunas dos records."""
        output = write_bronze(sample_records, "google_ads", str(tmp_path))
        df = pl.read_parquet(output)
        for column in sample_records[0].keys():
            assert column in df.columns

    def test_parquet_preserves_data_values(self, tmp_path: Path, sample_records: list) -> None:
        """Os valores no Parquet devem ser idênticos aos records originais."""
        output = write_bronze(sample_records, "google_ads", str(tmp_path))
        df = pl.read_parquet(output)
        assert df["campaign_name"].to_list()[0] == "Test Campaign"
        assert df["impressions"].to_list()[0] == 10_000

    def test_raises_for_empty_records(self, tmp_path: Path) -> None:
        """Deve lançar ValueError para lista de records vazia."""
        with pytest.raises(ValueError, match="Nenhum registro"):
            write_bronze([], "google_ads", str(tmp_path))

    def test_creates_directory_if_not_exists(self, tmp_path: Path, sample_records: list) -> None:
        """Deve criar os diretórios necessários caso não existam."""
        deep_path = tmp_path / "nested" / "bronze"
        write_bronze(sample_records, "tiktok_ads", str(deep_path))
        assert (deep_path / "tiktok_ads").is_dir()

    def test_different_sources_create_different_directories(self, tmp_path: Path, sample_records: list) -> None:
        """Fontes diferentes devem criar subdiretórios separados."""
        write_bronze(sample_records, "google_ads", str(tmp_path))
        write_bronze(sample_records, "meta_ads", str(tmp_path))
        assert (tmp_path / "google_ads").is_dir()
        assert (tmp_path / "meta_ads").is_dir()

    def test_successive_writes_create_separate_files(self, tmp_path: Path, sample_records: list) -> None:
        """Escritas sucessivas não devem sobrescrever arquivos anteriores."""
        ts1 = datetime(2026, 3, 12, 10, 0, 0)
        ts2 = datetime(2026, 3, 12, 10, 0, 1)
        out1 = write_bronze(sample_records, "google_ads", str(tmp_path), ts1)
        out2 = write_bronze(sample_records, "google_ads", str(tmp_path), ts2)
        assert out1 != out2
        assert out1.exists()
        assert out2.exists()
