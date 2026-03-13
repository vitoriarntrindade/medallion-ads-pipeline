"""Testes dos endpoints e schemas da API mock do TikTok Ads."""

from fastapi.testclient import TestClient


class TestTikTokAdsHealthEndpoint:
    """Testes do endpoint de health check do TikTok Ads."""

    def test_health_returns_200(self, api_client: TestClient) -> None:
        """Deve retornar status 200 para o health check."""
        response = api_client.get("/tiktok-ads/health")
        assert response.status_code == 200

    def test_health_returns_correct_source(self, api_client: TestClient) -> None:
        """Deve retornar o identificador correto da fonte."""
        assert api_client.get("/tiktok-ads/health").json()["source"] == "tiktok_ads"

    def test_health_returns_ok_status(self, api_client: TestClient) -> None:
        """Deve retornar status 'ok' no corpo da resposta."""
        assert api_client.get("/tiktok-ads/health").json()["status"] == "ok"


class TestTikTokAdsStatsEndpoint:
    """Testes do endpoint de estatísticas do TikTok Ads."""

    def test_stats_returns_200_with_valid_dates(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 200 com intervalo de datas válido."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        assert response.status_code == 200

    def test_stats_returns_correct_source_field(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar 'tiktok_ads' no campo source."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        assert response.json()["source"] == "tiktok_ads"

    def test_stats_list_field_not_rows_or_data(self, api_client: TestClient, valid_date_range: dict) -> None:
        """TikTok usa 'list' como chave — diferente de Google ('rows') e Meta ('data')."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        body = response.json()
        assert "list" in body
        assert "rows" not in body
        assert "data" not in body

    def test_stats_contains_request_id(self, api_client: TestClient, valid_date_range: dict) -> None:
        """TikTok retorna um request_id único — campo exclusivo desta fonte."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        body = response.json()
        assert "request_id" in body
        assert isinstance(body["request_id"], str)
        assert len(body["request_id"]) > 0

    def test_stats_request_id_is_different_each_call(self, api_client: TestClient, valid_date_range: dict) -> None:
        """request_id deve ser único a cada requisição (UUID v4)."""
        r1 = api_client.get("/tiktok-ads/stats", params=valid_date_range).json()
        r2 = api_client.get("/tiktok-ads/stats", params=valid_date_range).json()
        assert r1["request_id"] != r2["request_id"]

    def test_stats_total_rows_matches_list_length(self, api_client: TestClient, valid_date_range: dict) -> None:
        """total_rows deve ser igual ao comprimento da lista 'list'."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        body = response.json()
        assert body["total_rows"] == len(body["list"])

    def test_stats_returns_one_row_per_day(self, api_client: TestClient, valid_date_range: dict) -> None:
        """Deve retornar exatamente 7 linhas para um intervalo de 7 dias."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        assert response.json()["total_rows"] == 7

    def test_stats_row_contains_required_fields(self, api_client: TestClient, single_day_range: dict) -> None:
        """Cada linha deve conter todos os campos obrigatórios do schema."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        required_fields = {
            "stat_time",
            "campaign_title",
            "ad_group_title",
            "views",
            "engagements",
            "total_cost",
            "conversions_count",
            "campaign_objective",
            "country_code",
        }
        assert required_fields.issubset(row.keys())

    def test_stats_total_cost_is_string(self, api_client: TestClient, single_day_range: dict) -> None:
        """total_cost deve ser string — diferença crítica de schema vs Google e Meta."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        assert isinstance(row["total_cost"], str)

    def test_stats_total_cost_is_parseable_as_float(self, api_client: TestClient, single_day_range: dict) -> None:
        """total_cost como string deve ser conversível para float."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        parsed = float(row["total_cost"])
        assert parsed >= 0.0

    def test_stats_stat_time_is_timestamp_format(self, api_client: TestClient, single_day_range: dict) -> None:
        """stat_time deve estar no formato 'YYYY-MM-DD HH:MM:SS'."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        assert len(row["stat_time"]) == 19
        assert " " in row["stat_time"]  # separador de data e hora

    def test_stats_uses_views_not_impressions(self, api_client: TestClient, single_day_range: dict) -> None:
        """TikTok usa 'views', não 'impressions' nem 'reach'."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        assert "views" in row
        assert "impressions" not in row
        assert "reach" not in row

    def test_stats_uses_campaign_title_not_campaign_name(self, api_client: TestClient, single_day_range: dict) -> None:
        """TikTok usa 'campaign_title', não 'campaign_name'."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        row = response.json()["list"][0]
        assert "campaign_title" in row
        assert "campaign_name" not in row

    def test_stats_numeric_fields_are_non_negative(self, api_client: TestClient, valid_date_range: dict) -> None:
        """views, engagements e conversions_count devem ser >= 0."""
        response = api_client.get("/tiktok-ads/stats", params=valid_date_range)
        for row in response.json()["list"]:
            assert row["views"] >= 0
            assert row["engagements"] >= 0
            assert row["conversions_count"] >= 0
            assert float(row["total_cost"]) >= 0.0

    def test_stats_country_code_is_br(self, api_client: TestClient, single_day_range: dict) -> None:
        """O campo country_code deve ser 'BR'."""
        response = api_client.get("/tiktok-ads/stats", params=single_day_range)
        assert response.json()["list"][0]["country_code"] == "BR"

    def test_stats_returns_422_for_inverted_dates(self, api_client: TestClient, inverted_date_range: dict) -> None:
        """Deve retornar 422 quando start_date for posterior a end_date."""
        response = api_client.get("/tiktok-ads/stats", params=inverted_date_range)
        assert response.status_code == 422

    def test_stats_returns_422_for_exceeded_range(self, api_client: TestClient, exceeded_date_range: dict) -> None:
        """Deve retornar 422 quando o intervalo exceder 90 dias."""
        response = api_client.get("/tiktok-ads/stats", params=exceeded_date_range)
        assert response.status_code == 422

    def test_stats_uses_default_dates_when_no_params(self, api_client: TestClient) -> None:
        """Deve retornar 200 mesmo sem parâmetros, usando datas padrão."""
        response = api_client.get("/tiktok-ads/stats")
        assert response.status_code == 200
        assert response.json()["total_rows"] > 0
