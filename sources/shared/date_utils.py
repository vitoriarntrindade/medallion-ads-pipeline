"""Utilitários compartilhados para geração de datas nos dados mock."""

from datetime import date, timedelta


def generate_date_range(start_date: date, end_date: date) -> list[date]:
    """Gera uma lista de datas entre duas datas, inclusive.

    Args:
        start_date: Data inicial do intervalo.
        end_date: Data final do intervalo.

    Returns:
        Lista de datas do intervalo, em ordem crescente.

    Raises:
        ValueError: Se start_date for posterior a end_date.
    """
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) não pode ser posterior a end_date ({end_date}).")

    total_days = (end_date - start_date).days + 1
    return [start_date + timedelta(days=i) for i in range(total_days)]


def parse_date_query(date_str: str | None, fallback: date) -> date:
    """Converte uma string de data no formato YYYY-MM-DD para um objeto date.

    Se a string for None ou inválida, retorna o valor de fallback.

    Args:
        date_str: String de data no formato YYYY-MM-DD.
        fallback: Data a ser retornada em caso de ausência ou erro de parsing.

    Returns:
        Objeto date correspondente à string, ou fallback em caso de falha.
    """
    if not date_str:
        return fallback
    try:
        return date.fromisoformat(date_str)
    except ValueError:
        return fallback
