"""Cliente HTTP centralizado para consumo das APIs de Ads.

Encapsula o httpx com timeout configurável e tratamento de erros
padronizado para toda a camada de ingestão.
"""

from typing import Any

import httpx
from loguru import logger


class HttpClientError(Exception):
    """Erro de comunicação HTTP durante a ingestão.

    Attributes:
        status_code: Código HTTP retornado pela API, se disponível.
        url: URL que originou o erro.
    """

    def __init__(self, message: str, url: str, status_code: int | None = None) -> None:
        """Inicializa o erro com contexto da requisição.

        Args:
            message: Descrição do erro.
            url: URL que originou o erro.
            status_code: Código HTTP retornado, se disponível.
        """
        super().__init__(message)
        self.url = url
        self.status_code = status_code


def fetch_json(url: str, params: dict[str, str] | None = None, timeout: int = 30) -> Any:
    """Realiza uma requisição GET e retorna o corpo JSON da resposta.

    Args:
        url: URL completa do endpoint a ser consultado.
        params: Parâmetros de query string opcionais.
        timeout: Timeout da requisição em segundos.

    Returns:
        Corpo da resposta deserializado como dict ou list.

    Raises:
        HttpClientError: Se a requisição falhar por timeout, erro de
            conexão ou status HTTP não-2xx.
    """
    logger.debug(f"GET {url} | params={params}")

    try:
        response = httpx.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()

    except httpx.TimeoutException:
        raise HttpClientError(
            message=f"Timeout ao acessar {url} após {timeout}s.",
            url=url,
        )
    except httpx.ConnectError:
        raise HttpClientError(
            message=f"Falha de conexão ao acessar {url}. A API está no ar?",
            url=url,
        )
    except httpx.HTTPStatusError as exc:
        raise HttpClientError(
            message=f"API retornou status {exc.response.status_code} para {url}.",
            url=url,
            status_code=exc.response.status_code,
        )
