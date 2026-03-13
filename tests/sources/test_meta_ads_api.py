"""Testes dos endpoints e schemas da API mock do Meta Ads."""

from datetime import date

from fastapi.testclient import TestClient


class TestMetaAdsHealthEndpoint:
    """Testes do endpoint de health check do Meta Ads."""

    def test_health_returns_200(self, api_client: TestClient) -> None:
        """Deve retornar status 200 para o health check."""
        response = api_client.get("/meta-ads/health")
        assert response.status_code == 200

    def test_health_returns_correct_source(self, api_client: TestClient) -> None:
        """Deve retornar o identificador correto da fonte."""
        assert api_client.get("/meta-ads/health").json()["source"] == "meta_ads"

    def test_health_returns_ok_status(self, api_client: TestClient) -> None:
        """Deve retornar status 'ok' no corpo da resposta."""
        assert api_client.get("/meta-ads/health").json()["status"] == "ok"


class TestMetaAdsInsightsEndpoint:
    """Testes do endpoint de insights do Meta Ads."""

    def test_insights_returns_200_with_valid_dates(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 200 com intervalo de datas válido."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        assert response.status_code == 200

    def test_insights_returns_correct_source_field(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 'meta_ads' no campo source."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        assert response.json()["source"] == "meta_ads"

    def test_insights_data_field_not_rows(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Meta usa 'data' como chave da lista, não 'rows'."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        body = response.json()
        assert "data" in body
        assert "rows" not in body

    def test_insights_total_rows_matches_data_length(self, api_client: TestClient, valid_date_range: dict) -> None:
        """total_rows deve ser igual ao comprimento da lista data."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        body = response.json()
        assert body["total_rows"] == len(body["data"])

    def test_insights_returns_one_row_per_day(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar exatamente 7 linhas para um intervalo de 7 dias."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        assert response.json()["total_rows"] == 7

    def test_insights_row_contains_required_fields(self, api_client: TestClient, single_day_range: dict) -> None:
        """Cada linha deve conter todos os campos obrigatórios do schema."""
        response = api_client.get("/meta-ads/insights", params=single_day_range)
        row = response.json()["data"][0]
        required_fields = {
            "date_start",
            "date_stop",
            "campaign_name",
            "adset_name",
            "reach",
            "link_clicks",
            "spend",
            "results",
            "objective",
            "currency",
        }
        assert required_fields.issubset(row.keys())

    def test_insights_date_format_is_dd_mm_yyyy(self, api_client: TestClient, single_day_range: dict) -> None:
        """O campo date_start deve estar no formato DD/MM/YYYY (diferente do Google)."""
        response = api_client.get("/meta-ads/insights", params=single_day_range)
        row = response.json()["data"][0]
        parsed = date(
            int(row["date_start"][6:]),  # ano
            int(row["date_start"][3:5]),  # mês
            int(row["date_start"][:2]),  # dia
        )
        assert parsed == date(2026, 3, 1)

    def test_insights_spend_is_float(self, api_client: TestClient, single_day_range: dict) -> None:
        """spend deve ser float, não inteiro nem string."""
        response = api_client.get("/meta-ads/insights", params=single_day_range)
        row = response.json()["data"][0]
        assert isinstance(row["spend"], float)

    def test_insights_reach_not_impressions(self, api_client: TestClient, single_day_range: dict) -> None:
        """Meta usa 'reach', não 'impressions' — diferença de schema intencional."""
        response = api_client.get("/meta-ads/insights", params=single_day_range)
        row = response.json()["data"][0]
        assert "reach" in row
        assert "impressions" not in row

    def test_insights_link_clicks_not_clicks(self, api_client: TestClient, single_day_range: dict) -> None:
        """Meta usa 'link_clicks', não 'clicks' — diferença de schema intencional."""
        response = api_client.get("/meta-ads/insights", params=single_day_range)
        row = response.json()["data"][0]
        assert "link_clicks" in row
        assert "clicks" not in row

    def test_insights_numeric_fields_are_non_negative(self, api_client: TestClient, valid_date_range: dict) -> None:
        """reach, link_clicks, spend e results devem ser >= 0."""
        response = api_client.get("/meta-ads/insights", params=valid_date_range)
        for row in response.json()["data"]:
            assert row["reach"] >= 0
            assert row["link_clicks"] >= 0
            assert row["spend"] >= 0.0
            assert row["results"] >= 0

    def test_insights_returns_422_for_inverted_dates(self, api_client: TestClient, inverted_date_range: dict) -> None:
        """Deve retornar 422 quando start_date for posterior a end_date."""
        response = api_client.get("/meta-ads/insights", params=inverted_date_range)
        assert response.status_code == 422

    def test_insights_returns_422_for_exceeded_range(self, api_client: TestClient, exceeded_date_range: dict) -> None:
        """Deve retornar 422 quando o intervalo exceder 90 dias."""
        response = api_client.get("/meta-ads/insights", params=exceeded_date_range)
        assert response.status_code == 422

    def test_insights_uses_default_dates_when_no_params(self, api_client: TestClient) -> None:
        """Deve retornar 200 mesmo sem parâmetros, usando datas padrão."""
        response = api_client.get("/meta-ads/insights")
        assert response.status_code == 200
        assert response.json()["total_rows"] > 0
