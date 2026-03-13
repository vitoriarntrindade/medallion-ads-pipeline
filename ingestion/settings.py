"""Configurações do módulo de ingestão via pydantic-settings.

Lê variáveis de ambiente do arquivo .env automaticamente.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class IngestionSettings(BaseSettings):
    """Configurações de URLs das APIs e caminhos de armazenamento.

    Attributes:
        google_ads_api_url: URL base da API mock do Google Ads.
        meta_ads_api_url: URL base da API mock do Meta Ads.
        tiktok_ads_api_url: URL base da API mock do TikTok Ads.
        bronze_path: Caminho do diretório raiz da camada Bronze.
        http_timeout: Timeout em segundos para requisições HTTP.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    google_ads_api_url: str = Field(
        default="http://localhost:8080",
        description="URL base da API mock do Google Ads.",
    )
    meta_ads_api_url: str = Field(
        default="http://localhost:8080",
        description="URL base da API mock do Meta Ads.",
    )
    tiktok_ads_api_url: str = Field(
        default="http://localhost:8080",
        description="URL base da API mock do TikTok Ads.",
    )
    bronze_path: str = Field(
        default="storage/bronze",
        description="Caminho raiz da camada Bronze.",
    )
    http_timeout: int = Field(
        default=30,
        description="Timeout em segundos para requisições HTTP.",
    )


settings = IngestionSettings()
