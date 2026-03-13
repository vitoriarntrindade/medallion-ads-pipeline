"""Fixtures compartilhadas entre todos os testes do projeto.

Define clientes de teste das APIs e dados reutilizáveis entre suites.
"""

import pytest
from fastapi.testclient import TestClient

from sources.main import app


@pytest.fixture(scope="session")
def api_client() -> TestClient:
    """Cria um TestClient do FastAPI reutilizável em toda a sessão de testes.

    Usar scope='session' evita recriar o cliente a cada teste,
    reduzindo o overhead de inicialização da aplicação.

    Yields:
        TestClient configurado com a aplicação FastAPI principal.
    """
    with TestClient(app) as client:
        yield client


@pytest.fixture
def valid_date_range() -> dict[str, str]:
    """Retorna um intervalo de datas válido para uso nos testes de API.

    Returns:
        Dicionário com start_date e end_date no formato YYYY-MM-DD.
    """
    return {"start_date": "2026-03-01", "end_date": "2026-03-07"}


@pytest.fixture
def single_day_range() -> dict[str, str]:
    """Retorna um intervalo de um único dia para testes pontuais.

    Returns:
        Dicionário com start_date e end_date iguais.
    """
    return {"start_date": "2026-03-01", "end_date": "2026-03-01"}


@pytest.fixture
def inverted_date_range() -> dict[str, str]:
    """Retorna um intervalo com datas invertidas para testar validação.

    Returns:
        Dicionário com start_date posterior a end_date.
    """
    return {"start_date": "2026-03-10", "end_date": "2026-03-01"}


@pytest.fixture
def exceeded_date_range() -> dict[str, str]:
    """Retorna um intervalo maior que 90 dias para testar limite.

    Returns:
        Dicionário com intervalo de 91 dias.
    """
    return {"start_date": "2025-01-01", "end_date": "2025-04-02"}
