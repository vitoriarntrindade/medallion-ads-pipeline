"""Configurações de conexão com o PostgreSQL via pydantic-settings.

Lê as variáveis de ambiente com prefixo POSTGRES_ e constrói
o DSN no formato esperado pelo SQLAlchemy.
"""

from functools import lru_cache

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseSettings):
    """Configurações do banco de dados PostgreSQL.

    Attributes:
        postgres_host: Host do servidor Postgres.
        postgres_port: Porta do servidor Postgres.
        postgres_db: Nome do banco de dados.
        postgres_user: Usuário do banco de dados.
        postgres_password: Senha do banco de dados.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ad_analytics"
    postgres_user: str = "postgres"
    postgres_password: str = "postgres"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dsn(self) -> str:
        """Constrói o DSN completo para SQLAlchemy + psycopg2.

        Returns:
            String de conexão no formato postgresql+psycopg2://user:pwd@host:port/db
        """
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> PostgresSettings:
    """Retorna a instância singleton das configurações Postgres.

    Returns:
        Instância cacheada de PostgresSettings.
    """
    return PostgresSettings()
