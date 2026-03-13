"""Testes unitários dos utilitários de data."""

from datetime import date

import pytest

from sources.shared.date_utils import generate_date_range, parse_date_query


class TestGenerateDateRange:
    """Testes da função generate_date_range."""

    def test_generates_correct_number_of_days(self) -> None:
        """Deve gerar exatamente N+1 datas para um intervalo de N dias."""
        result = generate_date_range(date(2026, 3, 1), date(2026, 3, 7))
        assert len(result) == 7

    def test_single_day_range_returns_one_date(self) -> None:
        """Deve retornar uma única data quando start == end."""
        result = generate_date_range(date(2026, 3, 1), date(2026, 3, 1))
        assert len(result) == 1
        assert result[0] == date(2026, 3, 1)

    def test_dates_are_in_ascending_order(self) -> None:
        """As datas devem estar em ordem crescente."""
        result = generate_date_range(date(2026, 3, 1), date(2026, 3, 5))
        assert result == sorted(result)

    def test_first_date_equals_start_date(self) -> None:
        """O primeiro elemento deve ser igual a start_date."""
        start = date(2026, 3, 1)
        result = generate_date_range(start, date(2026, 3, 5))
        assert result[0] == start

    def test_last_date_equals_end_date(self) -> None:
        """O último elemento deve ser igual a end_date."""
        end = date(2026, 3, 5)
        result = generate_date_range(date(2026, 3, 1), end)
        assert result[-1] == end

    def test_raises_value_error_for_inverted_range(self) -> None:
        """Deve lançar ValueError quando start_date > end_date."""
        with pytest.raises(ValueError, match="start_date"):
            generate_date_range(date(2026, 3, 10), date(2026, 3, 1))

    def test_all_elements_are_date_objects(self) -> None:
        """Todos os elementos da lista devem ser objetos date."""
        result = generate_date_range(date(2026, 3, 1), date(2026, 3, 3))
        assert all(isinstance(d, date) for d in result)

    def test_consecutive_dates_differ_by_one_day(self) -> None:
        """Datas consecutivas devem diferir por exatamente 1 dia."""
        from datetime import timedelta

        result = generate_date_range(date(2026, 3, 1), date(2026, 3, 5))
        for i in range(len(result) - 1):
            assert result[i + 1] - result[i] == timedelta(days=1)


class TestParseDateQuery:
    """Testes da função parse_date_query."""

    def test_parses_valid_iso_date(self) -> None:
        """Deve parsear corretamente uma string de data válida."""
        result = parse_date_query("2026-03-01", fallback=date.today())
        assert result == date(2026, 3, 1)

    def test_returns_fallback_for_none(self) -> None:
        """Deve retornar o fallback quando recebe None."""
        fallback = date(2026, 1, 1)
        result = parse_date_query(None, fallback=fallback)
        assert result == fallback

    def test_returns_fallback_for_invalid_format(self) -> None:
        """Deve retornar o fallback para formatos de data inválidos."""
        fallback = date(2026, 1, 1)
        result = parse_date_query("01/03/2026", fallback=fallback)
        assert result == fallback

    def test_returns_fallback_for_empty_string(self) -> None:
        """Deve retornar o fallback para string vazia."""
        fallback = date(2026, 1, 1)
        result = parse_date_query("", fallback=fallback)
        assert result == fallback

    def test_returns_fallback_for_garbage_string(self) -> None:
        """Deve retornar o fallback para strings sem sentido."""
        fallback = date(2026, 1, 1)
        result = parse_date_query("not-a-date", fallback=fallback)
        assert result == fallback
