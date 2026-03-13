"""Testes unitários dos utilitários de geração de métricas sintéticas."""

from sources.shared.faker_utils import (
    AD_SETS,
    CAMPAIGN_NAMES,
    pick_ad_set,
    pick_campaign,
    random_clicks,
    random_conversions,
    random_cost,
    random_impressions,
)


class TestRandomImpressions:
    """Testes do gerador de impressões."""

    def test_returns_integer(self) -> None:
        """Deve retornar um inteiro."""
        assert isinstance(random_impressions(), int)

    def test_within_default_bounds(self) -> None:
        """Deve retornar valor dentro dos limites padrão."""
        for _ in range(20):
            value = random_impressions()
            assert 1_000 <= value <= 500_000

    def test_within_custom_bounds(self) -> None:
        """Deve respeitar limites customizados."""
        for _ in range(20):
            value = random_impressions(min_val=100, max_val=200)
            assert 100 <= value <= 200


class TestRandomClicks:
    """Testes do gerador de cliques."""

    def test_returns_integer(self) -> None:
        """Deve retornar um inteiro."""
        assert isinstance(random_clicks(10_000), int)

    def test_clicks_at_least_one(self) -> None:
        """Deve retornar ao menos 1 clique."""
        assert random_clicks(1_000) >= 1

    def test_clicks_less_than_or_equal_to_impressions(self) -> None:
        """Cliques não devem exceder impressões."""
        impressions = 10_000
        for _ in range(20):
            assert random_clicks(impressions) <= impressions

    def test_clicks_scale_with_impressions(self) -> None:
        """Mais impressões devem gerar mais cliques em média."""
        low = sum(random_clicks(1_000) for _ in range(50)) / 50
        high = sum(random_clicks(100_000) for _ in range(50)) / 50
        assert high > low


class TestRandomCost:
    """Testes do gerador de custo."""

    def test_returns_float(self) -> None:
        """Deve retornar um float."""
        assert isinstance(random_cost(100), float)

    def test_cost_is_non_negative(self) -> None:
        """Custo deve ser >= 0."""
        assert random_cost(0) >= 0.0

    def test_cost_has_two_decimal_places(self) -> None:
        """Custo deve ter no máximo 2 casas decimais."""
        for _ in range(20):
            value = random_cost(100)
            assert round(value, 2) == value

    def test_cost_scales_with_clicks(self) -> None:
        """Mais cliques devem gerar custo maior em média."""
        low = sum(random_cost(10) for _ in range(50)) / 50
        high = sum(random_cost(1_000) for _ in range(50)) / 50
        assert high > low


class TestRandomConversions:
    """Testes do gerador de conversões."""

    def test_returns_integer(self) -> None:
        """Deve retornar um inteiro."""
        assert isinstance(random_conversions(100), int)

    def test_conversions_are_non_negative(self) -> None:
        """Conversões devem ser >= 0."""
        assert random_conversions(0) >= 0

    def test_conversions_do_not_exceed_clicks(self) -> None:
        """Conversões não devem exceder cliques."""
        clicks = 500
        for _ in range(20):
            assert random_conversions(clicks) <= clicks


class TestPickCampaign:
    """Testes do seletor de campanha."""

    def test_returns_string(self) -> None:
        """Deve retornar uma string."""
        assert isinstance(pick_campaign(), str)

    def test_returns_value_from_predefined_list(self) -> None:
        """Deve retornar apenas valores da lista CAMPAIGN_NAMES."""
        for _ in range(20):
            assert pick_campaign() in CAMPAIGN_NAMES


class TestPickAdSet:
    """Testes do seletor de ad set."""

    def test_returns_string(self) -> None:
        """Deve retornar uma string."""
        assert isinstance(pick_ad_set(), str)

    def test_returns_value_from_predefined_list(self) -> None:
        """Deve retornar apenas valores da lista AD_SETS."""
        for _ in range(20):
            assert pick_ad_set() in AD_SETS
