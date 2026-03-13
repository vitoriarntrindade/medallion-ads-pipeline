"""Testes dos endpoints e schemas da API mock do Google Ads."""

from fastapi.testclient import TestClient


class TestGoogleAdsHealthEndpoint:
    """Testes do endpoint de health check do Google Ads."""

    def test_health_returns_200(self, api_client: TestClient) -> None:
        """Deve retornar status 200 para o health check."""
        response = api_client.get("/google-ads/health")
        assert response.status_code == 200

    def test_health_returns_correct_source(self, api_client: TestClient) -> None:
        """Deve retornar o identificador correto da fonte."""
        response = api_client.get("/google-ads/health")
        assert response.json()["source"] == "google_ads"

    def test_health_returns_ok_status(self, api_client: TestClient) -> None:
        """Deve retornar status 'ok' no corpo da resposta."""
        response = api_client.get("/google-ads/health")
        assert response.json()["status"] == "ok"


class TestGoogleAdsReportEndpoint:
    """Testes do endpoint de relatório do Google Ads."""

    def test_report_returns_200_with_valid_dates(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 200 com intervalo de datas válido."""
        response = api_client.get("/google-ads/report", params=valid_date_range)
        assert response.status_code == 200

    def test_report_returns_correct_source_field(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 'google_ads' no campo source."""
        response = api_client.get("/google-ads/report", params=valid_date_range)
        assert response.json()["source"] == "google_ads"

    def test_report_total_rows_matches_rows_length(self, api_client: TestClient, valid_date_range: dict) -> None:
        """total_rows deve ser igual ao comprimento da lista rows."""
        response = api_client.get("/google-ads/report", params=valid_date_range)
        body = response.json()
        assert body["total_rows"] == len(body["rows"])

    def test_report_returns_one_row_per_day(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar exatamente 7 linhas para um intervalo de 7 dias."""
        response = api_client.get("/google-ads/report", params=valid_date_range)
        assert response.json()["total_rows"] == 7

    def test_report_single_day_returns_one_row(self, api_client: TestClient, single_day_range: dict) -> None:
        """Deve retornar exatamente 1 linha para um intervalo de 1 dia."""
        response = api_client.get("/google-ads/report", params=single_day_range)
        assert response.json()["total_rows"] == 1

    def test_report_row_contains_required_fields(self, api_client: TestClient, single_day_range: dict) -> None:
        """Cada linha deve conter todos os campos obrigatórios do schema."""
        response = api_client.get("/google-ads/report", params=single_day_range)
        row = response.json()["rows"][0]
        required_fields = {
            "date",
            "campaign_name",
            "ad_group_name",
            "impressions",
            "clicks",
            "cost_micros",
            "conversions",
            "currency",
        }
        assert required_fields.issubset(row.keys())

    def test_report_cost_is_in_micros(self, api_client: TestClient, single_day_range: dict) -> None:
        """cost_micros deve ser um inteiro representando o custo * 1.000.000."""
        response = api_client.get("/google-ads/report", params=single_day_range)
        row = response.json()["rows"][0]
        assert isinstance(row["cost_micros"], int)
        assert row["cost_micros"] > 0

    def test_report_date_format_is_iso(self, api_client: TestClient, single_day_range: dict) -> None:
        """O campo date deve estar no formato YYYY-MM-DD."""
        response = api_client.get("/google-ads/report", params=single_day_range)
        row = response.json()["rows"][0]
        from datetime import date

        parsed = date.fromisoformat(row["date"])  # lança ValueError se inválido
        assert parsed == date(2026, 3, 1)

    def test_report_numeric_fields_are_non_negative(self, api_client: TestClient, valid_date_range: dict) -> None:
        """impressions, clicks, cost_micros e conversions devem ser >= 0."""
        response = api_client.get("/google-ads/report", params=valid_date_range)
        for row in response.json()["rows"]:
            assert row["impressions"] >= 0
            assert row["clicks"] >= 0
            assert row["cost_micros"] >= 0
            assert row["conversions"] >= 0

    def test_report_currency_is_brl(self, api_client: TestClient, single_day_range: dict) -> None:
        """O campo currency deve ser 'BRL'."""
        response = api_client.get("/google-ads/report", params=single_day_range)
        assert response.json()["rows"][0]["currency"] == "BRL"

    def test_report_returns_422_for_inverted_dates(self, api_client: TestClient, inverted_date_range: dict) -> None:
        """Deve retornar 422 quando start_date for posterior a end_date."""
        response = api_client.get("/google-ads/report", params=inverted_date_range)
        assert response.status_code == 422

    def test_report_returns_422_for_exceeded_range(self, api_client: TestClient, exceeded_date_range: dict) -> None:
        """Deve retornar 422 quando o intervalo exceder 90 dias."""
        response = api_client.get("/google-ads/report", params=exceeded_date_range)
        assert response.status_code == 422

    def test_report_uses_default_dates_when_no_params(self, api_client: TestClient) -> None:
        """Deve retornar 200 mesmo sem parâmetros, usando datas padrão."""
        response = api_client.get("/google-ads/report")
        assert response.status_code == 200
        assert response.json()["total_rows"] > 0
