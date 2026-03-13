"""Extrator de dados do Google Ads."""

from datetime import date

from ingestion.extractors.base import BaseExtractor
from ingestion.http_client import fetch_json


class GoogleAdsExtractor(BaseExtractor):
    """Extrator responsável por consumir a API mock do Google Ads.

    Consulta o endpoint /google-ads/report e retorna as linhas brutas
    do relatório, sem nenhuma transformação de schema.

    Attributes:
        api_url: URL base da API do Google Ads.
    """

    def __init__(self, api_url: str, bronze_root: str, http_timeout: int = 30) -> None:
        """Inicializa o extrator com a URL da API e configurações de storage.

        Args:
            api_url: URL base da API mock do Google Ads.
            bronze_root: Caminho raiz da camada Bronze.
            http_timeout: Timeout em segundos para requisições HTTP.
        """
        super().__init__(bronze_root=bronze_root, http_timeout=http_timeout)
        self.api_url = api_url.rstrip("/")

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'google_ads'.
        """
        return "google_ads"

    def extract_records(self, start_date: date, end_date: date) -> list[dict]:
        """Consulta o endpoint de relatório do Google Ads e retorna os registros.

        Args:
            start_date: Data inicial do relatório (YYYY-MM-DD).
            end_date: Data final do relatório (YYYY-MM-DD).

        Returns:
            Lista de dicionários com as linhas brutas do relatório.

        Raises:
            HttpClientError: Se a requisição HTTP falhar.
        """
        url = f"{self.api_url}/report"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        payload = fetch_json(url=url, params=params, timeout=self.http_timeout)
        return payload.get("rows", [])
