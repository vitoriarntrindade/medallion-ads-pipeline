"""Classe base abstrata para todos os extratores de fontes de Ads.

Define o contrato que cada extrator deve implementar, garantindo
que todos sigam o mesmo padrão de interface independente da fonte.
"""

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path

from loguru import logger

from ingestion.bronze_writer import write_bronze
from ingestion.http_client import HttpClientError


class BaseExtractor(ABC):
    """Contrato base para extratores de dados de plataformas de Ads.

    Cada plataforma (Google, Meta, TikTok) deve implementar esta classe,
    sobrescrevendo `source_name` e o método `extract_records`.

    Attributes:
        bronze_root: Caminho raiz da camada Bronze.
        http_timeout: Timeout em segundos para requisições HTTP.
    """

    def __init__(self, bronze_root: str, http_timeout: int = 30) -> None:
        """Inicializa o extrator com configurações de armazenamento.

        Args:
            bronze_root: Caminho raiz da camada Bronze.
            http_timeout: Timeout em segundos para chamadas HTTP.
        """
        self.bronze_root = bronze_root
        self.http_timeout = http_timeout

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Nome identificador da fonte (ex: 'google_ads').

        Returns:
            String com o nome único da fonte.
        """
        ...

    @abstractmethod
    def extract_records(self, start_date: date, end_date: date) -> list[dict]:
        """Consulta a API da fonte e retorna os registros brutos.

        Args:
            start_date: Data inicial do intervalo de extração.
            end_date: Data final do intervalo de extração.

        Returns:
            Lista de dicionários com os registros brutos da fonte.

        Raises:
            HttpClientError: Se a comunicação com a API falhar.
        """
        ...

    def run(self, start_date: date, end_date: date) -> Path | None:
        """Executa o ciclo completo de extração e persistência na Bronze.

        Chama `extract_records`, adiciona metadados de ingestão e
        persiste o resultado via `write_bronze`. Em caso de falha de
        comunicação, loga o erro e retorna None sem propagar a exceção
        — permitindo que outras fontes continuem sendo processadas.

        Args:
            start_date: Data inicial do intervalo de extração.
            end_date: Data final do intervalo de extração.

        Returns:
            Path do arquivo Parquet gerado, ou None em caso de falha.
        """
        logger.info(f"Iniciando extração | fonte={self.source_name} | período={start_date} → {end_date}")

        try:
            records = self.extract_records(start_date, end_date)
        except HttpClientError as exc:
            logger.error(f"Falha na extração | fonte={self.source_name} | url={exc.url} | erro={exc}")
            return None

        if not records:
            logger.warning(f"Nenhum registro retornado | fonte={self.source_name}")
            return None

        # adiciona metadados de rastreabilidade em cada registro
        enriched_records = [
            {**record, "_source": self.source_name, "_ingested_at": str(date.today())} for record in records
        ]

        output_path = write_bronze(
            records=enriched_records,
            source_name=self.source_name,
            bronze_root=self.bronze_root,
        )

        logger.success(
            f"Extração concluída | fonte={self.source_name} | linhas={len(enriched_records)} | arquivo={output_path}"
        )

        return output_path
