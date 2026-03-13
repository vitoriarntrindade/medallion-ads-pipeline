"""Extrator de dados do Meta Ads."""

from datetime import date

from ingestion.extractors.base import BaseExtractor
from ingestion.http_client import fetch_json


class MetaAdsExtractor(BaseExtractor):
    """Extrator responsável por consumir a API mock do Meta Ads.

    Consulta o endpoint /meta-ads/insights e retorna as linhas brutas
    dos insights, sem nenhuma transformação de schema.

    Attributes:
        api_url: URL base da API do Meta Ads.
    """

    def __init__(self, api_url: str, bronze_root: str, http_timeout: int = 30) -> None:
        """Inicializa o extrator com a URL da API e configurações de storage.

        Args:
            api_url: URL base da API mock do Meta Ads.
            bronze_root: Caminho raiz da camada Bronze.
            http_timeout: Timeout em segundos para requisições HTTP.
        """
        super().__init__(bronze_root=bronze_root, http_timeout=http_timeout)
        self.api_url = api_url.rstrip("/")

    @property
    def source_name(self) -> str:
        """Identificador da fonte.

        Returns:
            String 'meta_ads'.
        """
        return "meta_ads"

    def extract_records(self, start_date: date, end_date: date) -> list[dict]:
        """Consulta o endpoint de insights do Meta Ads e retorna os registros.

        Args:
            start_date: Data inicial dos insights (YYYY-MM-DD).
            end_date: Data final dos insights (YYYY-MM-DD).

        Returns:
            Lista de dicionários com as linhas brutas dos insights.

        Raises:
            HttpClientError: Se a requisição HTTP falhar.
        """
        url = f"{self.api_url}/insights"
        params = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }

        payload = fetch_json(url=url, params=params, timeout=self.http_timeout)
        return payload.get("data", [])
