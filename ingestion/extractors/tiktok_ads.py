"""Extrator de dados do TikTok Ads."""

from datetime import date

from ingestion.extractors.base import BaseExtractor
from ingestion.http_client import fetch_json


class TikTokAdsExtractor(BaseExtractor):
    """Extrator responsável por consumir a API mock do TikTok Ads.

    Consulta o endpoint /tiktok-ads/stats e retorna as linhas brutas
    das estatísticas, sem nenhuma transformação de schema.

    Attributes:
        api_url: URL base da API do TikTok Ads.
    """

    def __init__(self, api_url: str, bronze_root: str, http_timeout: int = 30) -> None:
        """Inicializa o extrator com a URL da API e configurações de storage.

        Args:
            api_url: URL base da API mock do TikTok Ads.
            bronze_root: Caminho raiz da camada Bronze.
            http_timeout: Timeout em segundos para requisições HTTP.
        """
        super().__init__(bronze_root=bronze_root, http_timeout=http_timeout)
        self.api_url = api_url.rstrip("/")

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'tiktok_ads'.
        """
        return "tiktok_ads"

    def extract_records(self, start_date: date, end_date: date) -> list[dict]:
        """Consulta o endpoint de stats do TikTok Ads e retorna os registros.

        Args:
            start_date: Data inicial das estatísticas (YYYY-MM-DD).
            end_date: Data final das estatísticas (YYYY-MM-DD).

        Returns:
            Lista de dicionários com as linhas brutas das estatísticas.

        Raises:
            HttpClientError: Se a requisição HTTP falhar.
        """
        url = f"{self.api_url}/stats"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        payload = fetch_json(url=url, params=params, timeout=self.http_timeout)
        return payload.get("list", [])
