"""Resource Dagster para conexão com o PostgreSQL.

Encapsula o SQLAlchemy Engine como um recurso configurável,
permitindo que os assets recebam o engine via injeção de
dependência ao invés de instanciar conexões diretamente.
"""

import sqlalchemy as sa
from dagster import ConfigurableResource
from pydantic import computed_field


class PostgresResource(ConfigurableResource):
    """Resource que fornece um SQLAlchemy Engine para o PostgreSQL.

    Os campos são lidos das configurações do job Dagster, que por
    sua vez resolvem variáveis de ambiente com prefixo POSTGRES_.

    Attributes:
        host: Host do servidor PostgreSQL.
        port: Porta do servidor PostgreSQL.
        database: Nome do banco de dados.
        user: Usuário do banco de dados.
        password: Senha do banco de dados.
    """

    host: str = "localhost"
    port: int = 5432
    database: str = "ad_analytics"
    user: str = "postgres"
    password: str = "postgres"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dsn(self) -> str:
        """Constrói o DSN completo para SQLAlchemy + psycopg2.

        Returns:
            String de conexão no formato postgresql+psycopg2://...
        """
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self) -> sa.Engine:
        """Cria e retorna um SQLAlchemy Engine.

        Returns:
            Engine conectado ao PostgreSQL configurado.
        """
        return sa.create_engine(self.dsn)
