"""Testes unitários do cliente HTTP e dos extratores."""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestion.extractors.google_ads import GoogleAdsExtractor
from ingestion.extractors.meta_ads import MetaAdsExtractor
from ingestion.extractors.tiktok_ads import TikTokAdsExtractor
from ingestion.http_client import HttpClientError, fetch_json

# ─── Testes do http_client ────────────────────────────────────────────────────


class TestFetchJson:
    """Testes do cliente HTTP fetch_json."""

    def test_returns_parsed_json_on_success(self) -> None:
        """Deve retornar o corpo da resposta como dict em caso de sucesso."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"source": "google_ads", "rows": []}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.get", return_value=mock_response):
            result = fetch_json("http://fake-url/report")

        assert result == {"source": "google_ads", "rows": []}

    def test_raises_http_client_error_on_timeout(self) -> None:
        """Deve lançar HttpClientError quando ocorrer timeout."""
        import httpx

        with patch("httpx.get", side_effect=httpx.TimeoutException("timeout")):
            with pytest.raises(HttpClientError) as exc_info:
                fetch_json("http://fake-url/report")

        assert "Timeout" in str(exc_info.value)

    def test_raises_http_client_error_on_connect_error(self) -> None:
        """Deve lançar HttpClientError quando a conexão falhar."""
        import httpx

        with patch("httpx.get", side_effect=httpx.ConnectError("connection refused")):
            with pytest.raises(HttpClientError) as exc_info:
                fetch_json("http://fake-url/report")

        assert "conexão" in str(exc_info.value)

    def test_raises_http_client_error_on_4xx_status(self) -> None:
        """Deve lançar HttpClientError para respostas com status 4xx."""
        import httpx

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "not found", request=MagicMock(), response=mock_response
        )

        with patch("httpx.get", return_value=mock_response):
            with pytest.raises(HttpClientError) as exc_info:
                fetch_json("http://fake-url/report")

        assert exc_info.value.status_code == 404

    def test_http_client_error_stores_url(self) -> None:
        """HttpClientError deve armazenar a URL que gerou o erro."""
        import httpx

        target_url = "http://fake-url/report"
        with patch("httpx.get", side_effect=httpx.ConnectError("error")):
            with pytest.raises(HttpClientError) as exc_info:
                fetch_json(target_url)

        assert exc_info.value.url == target_url

    def test_passes_params_to_request(self) -> None:
        """Deve repassar os parâmetros de query para a requisição HTTP."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.get", return_value=mock_response) as mock_get:
            fetch_json("http://fake-url", params={"start_date": "2026-03-01"})

        mock_get.assert_called_once_with(
            "http://fake-url",
            params={"start_date": "2026-03-01"},
            timeout=30,
        )


# ─── Testes dos extratores ────────────────────────────────────────────────────


class TestGoogleAdsExtractor:
    """Testes do GoogleAdsExtractor."""

    def test_source_name_is_google_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'google_ads'."""
        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))
        assert extractor.source_name == "google_ads"

    def test_extract_records_calls_correct_endpoint(self, tmp_path: Path) -> None:
        """Deve chamar o endpoint /google-ads/report."""
        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))
        fake_payload = {"rows": [{"date": "2026-03-01", "impressions": 1000}]}

        with patch("ingestion.extractors.google_ads.fetch_json", return_value=fake_payload):
            records = extractor.extract_records(date(2026, 3, 1), date(2026, 3, 1))

        assert records == fake_payload["rows"]

    def test_run_returns_path_on_success(self, tmp_path: Path) -> None:
        """run() deve retornar o Path do arquivo gerado em caso de sucesso."""
        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))
        fake_records = [{"date": "2026-03-01", "impressions": 1000, "clicks": 50}]

        with patch("ingestion.extractors.google_ads.fetch_json", return_value={"rows": fake_records}):
            result = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        assert result is not None
        assert result.exists()
        assert result.suffix == ".parquet"

    def test_run_returns_none_on_http_error(self, tmp_path: Path) -> None:
        """run() deve retornar None quando a API falhar, sem propagar exceção."""
        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))

        with patch(
            "ingestion.extractors.google_ads.fetch_json",
            side_effect=HttpClientError("erro", url="http://fake"),
        ):
            result = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        assert result is None

    def test_run_returns_none_for_empty_response(self, tmp_path: Path) -> None:
        """run() deve retornar None quando a API retornar lista vazia."""
        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))

        with patch("ingestion.extractors.google_ads.fetch_json", return_value={"rows": []}):
            result = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        assert result is None

    def test_enriched_records_contain_source_metadata(self, tmp_path: Path) -> None:
        """Registros salvos devem conter _source e _ingested_at."""
        import polars as pl

        extractor = GoogleAdsExtractor("http://fake", str(tmp_path))
        fake_records = [{"date": "2026-03-01", "impressions": 1000}]

        with patch("ingestion.extractors.google_ads.fetch_json", return_value={"rows": fake_records}):
            output = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        df = pl.read_parquet(output)
        assert "_source" in df.columns
        assert "_ingested_at" in df.columns
        assert df["_source"][0] == "google_ads"


class TestMetaAdsExtractor:
    """Testes do MetaAdsExtractor."""

    def test_source_name_is_meta_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'meta_ads'."""
        extractor = MetaAdsExtractor("http://fake", str(tmp_path))
        assert extractor.source_name == "meta_ads"

    def test_extract_records_reads_data_key(self, tmp_path: Path) -> None:
        """Deve ler a chave 'data' da resposta, não 'rows'."""
        extractor = MetaAdsExtractor("http://fake", str(tmp_path))
        fake_payload = {"data": [{"date_start": "01/03/2026", "reach": 5000}]}

        with patch("ingestion.extractors.meta_ads.fetch_json", return_value=fake_payload):
            records = extractor.extract_records(date(2026, 3, 1), date(2026, 3, 1))

        assert records == fake_payload["data"]

    def test_run_returns_none_on_http_error(self, tmp_path: Path) -> None:
        """run() deve retornar None quando a API falhar."""
        extractor = MetaAdsExtractor("http://fake", str(tmp_path))

        with patch(
            "ingestion.extractors.meta_ads.fetch_json",
            side_effect=HttpClientError("erro", url="http://fake"),
        ):
            result = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        assert result is None


class TestTikTokAdsExtractor:
    """Testes do TikTokAdsExtractor."""

    def test_source_name_is_tiktok_ads(self, tmp_path: Path) -> None:
        """source_name deve ser 'tiktok_ads'."""
        extractor = TikTokAdsExtractor("http://fake", str(tmp_path))
        assert extractor.source_name == "tiktok_ads"

    def test_extract_records_reads_list_key(self, tmp_path: Path) -> None:
        """Deve ler a chave 'list' da resposta — diferente de Google e Meta."""
        extractor = TikTokAdsExtractor("http://fake", str(tmp_path))
        fake_payload = {"list": [{"stat_time": "2026-03-01 00:00:00", "views": 3000}]}

        with patch("ingestion.extractors.tiktok_ads.fetch_json", return_value=fake_payload):
            records = extractor.extract_records(date(2026, 3, 1), date(2026, 3, 1))

        assert records == fake_payload["list"]

    def test_run_returns_none_on_http_error(self, tmp_path: Path) -> None:
        """run() deve retornar None quando a API falhar."""
        extractor = TikTokAdsExtractor("http://fake", str(tmp_path))

        with patch(
            "ingestion.extractors.tiktok_ads.fetch_json",
            side_effect=HttpClientError("erro", url="http://fake"),
        ):
            result = extractor.run(date(2026, 3, 1), date(2026, 3, 1))

        assert result is None
