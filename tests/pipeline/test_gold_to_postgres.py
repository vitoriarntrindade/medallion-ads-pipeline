"""Testes de integração Gold → PostgreSQL usando Testcontainers.

Cada teste roda contra um PostgreSQL 16 real em container Docker,
garantindo paridade com o ambiente de produção. O container é
inicializado uma vez por sessão de testes (session-scoped fixture)
para minimizar overhead de startup.

Para rodar apenas estes testes:
    pytest tests/pipeline/test_gold_to_postgres.py -v

Para pular em ambientes sem Docker:
    pytest tests/ --ignore=tests/pipeline/test_gold_to_postgres.py
"""

from datetime import date
from pathlib import Path

import polars as pl
import pytest
import sqlalchemy as sa
from testcontainers.postgres import PostgresContainer

from pipeline.gold_to_postgres.loader import (
    create_tables,
    load_gold_parquet,
    upsert_table,
)
from pipeline.gold_to_postgres.run_loader import find_latest_gold_file, run_loader
from pipeline.gold_to_postgres.schema import TABLES

# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def postgres_container():
    """Sobe um container PostgreSQL 16 compartilhado por toda a sessão de testes.

    Usar scope='session' evita o custo de subir/derrubar o container
    para cada teste individualmente.
    """
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest.fixture(scope="session")
def engine(postgres_container: PostgresContainer) -> sa.Engine:
    """Cria o engine SQLAlchemy conectado ao container de testes."""
    return sa.create_engine(postgres_container.get_connection_url())


@pytest.fixture(autouse=True)
def clean_tables(engine: sa.Engine) -> None:
    """Limpa todas as tabelas Gold antes de cada teste.

    Garante isolamento: cada teste começa com o banco vazio,
    independente da ordem de execução.
    """
    create_tables(engine)
    with engine.begin() as conn:
        for table in reversed(list(TABLES.values())):
            conn.execute(table.delete())


# ─── DataFrames de teste ──────────────────────────────────────────────────────


@pytest.fixture
def daily_df() -> pl.DataFrame:
    """DataFrame Gold daily_summary com duas linhas."""
    return pl.DataFrame(
        {
            "date": [date(2026, 3, 1), date(2026, 3, 2)],
            "source": ["google_ads", "meta_ads"],
            "impressions": [100_000, 60_000],
            "clicks": [5_000, 3_000],
            "cost_brl": [2_500.0, 1_200.0],
            "conversions": [250, 150],
            "ctr_pct": [5.0, 5.0],
            "cpc_brl": [0.5, 0.4],
            "cpm_brl": [25.0, 20.0],
            "cpa_brl": [10.0, 8.0],
        }
    )


@pytest.fixture
def campaign_df() -> pl.DataFrame:
    """DataFrame Gold campaign_summary com duas linhas."""
    return pl.DataFrame(
        {
            "source": ["google_ads", "meta_ads"],
            "campaign_name": ["Brand Q1", "Lead Gen"],
            "impressions": [180_000, 100_000],
            "clicks": [9_000, 4_500],
            "cost_brl": [4_500.0, 1_800.0],
            "conversions": [450, 225],
            "ctr_pct": [5.0, 4.5],
            "cpc_brl": [0.5, 0.4],
            "cpm_brl": [25.0, 18.0],
            "cpa_brl": [10.0, 8.0],
        }
    )


@pytest.fixture
def source_df() -> pl.DataFrame:
    """DataFrame Gold source_comparison com duas linhas."""
    return pl.DataFrame(
        {
            "source": ["google_ads", "meta_ads"],
            "impressions": [230_000, 100_000],
            "clicks": [11_000, 4_500],
            "cost_brl": [5_500.0, 1_800.0],
            "conversions": [550, 225],
            "ctr_pct": [4.78, 4.5],
            "cpc_brl": [0.5, 0.4],
            "cpm_brl": [23.9, 18.0],
            "cpa_brl": [10.0, 8.0],
        }
    )


# ─── Testes de create_tables ──────────────────────────────────────────────────


class TestCreateTables:
    """Testes da criação de tabelas no PostgreSQL."""

    def test_creates_all_three_tables(self, engine: sa.Engine) -> None:
        """As três tabelas Gold devem existir após create_tables()."""
        insp = sa.inspect(engine)
        table_names = insp.get_table_names()
        assert "gold_daily_summary" in table_names
        assert "gold_campaign_summary" in table_names
        assert "gold_source_comparison" in table_names

    def test_create_tables_is_idempotent(self, engine: sa.Engine) -> None:
        """Chamar create_tables() duas vezes não deve lançar exceção."""
        create_tables(engine)  # segunda chamada — deve ser no-op
        insp = sa.inspect(engine)
        assert "gold_daily_summary" in insp.get_table_names()


# ─── Testes de upsert_table ───────────────────────────────────────────────────


class TestUpsertTable:
    """Testes do upsert Gold → PostgreSQL."""

    def test_inserts_rows_into_daily_summary(self, engine: sa.Engine, daily_df: pl.DataFrame) -> None:
        """Deve inserir linhas na tabela gold_daily_summary."""
        rows = upsert_table(engine, "daily_summary", daily_df)
        assert rows == 2

    def test_inserted_rows_are_queryable(self, engine: sa.Engine, daily_df: pl.DataFrame) -> None:
        """Linhas inseridas devem ser recuperáveis via SELECT."""
        upsert_table(engine, "daily_summary", daily_df)

        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT COUNT(*) FROM gold_daily_summary"))
            assert result.scalar() == 2

    def test_upsert_updates_existing_row(self, engine: sa.Engine, daily_df: pl.DataFrame) -> None:
        """Segunda inserção com mesma chave deve atualizar, não duplicar."""
        upsert_table(engine, "daily_summary", daily_df)

        # Atualiza impressions da primeira linha
        updated = daily_df.with_columns(
            pl.when(pl.col("source") == "google_ads")
            .then(pl.lit(999_999))
            .otherwise(pl.col("impressions"))
            .alias("impressions")
        )
        upsert_table(engine, "daily_summary", updated)

        with engine.connect() as conn:
            count = conn.execute(sa.text("SELECT COUNT(*) FROM gold_daily_summary")).scalar()
            impressions = conn.execute(
                sa.text("SELECT impressions FROM gold_daily_summary WHERE source = 'google_ads'")
            ).scalar()

        assert count == 2  # não duplicou
        assert impressions == 999_999  # valor atualizado

    def test_upsert_campaign_summary(self, engine: sa.Engine, campaign_df: pl.DataFrame) -> None:
        """Deve inserir corretamente na tabela gold_campaign_summary."""
        rows = upsert_table(engine, "campaign_summary", campaign_df)
        assert rows == 2

    def test_upsert_source_comparison(self, engine: sa.Engine, source_df: pl.DataFrame) -> None:
        """Deve inserir corretamente na tabela gold_source_comparison."""
        rows = upsert_table(engine, "source_comparison", source_df)
        assert rows == 2

    def test_returns_zero_for_empty_dataframe(self, engine: sa.Engine) -> None:
        """Deve retornar 0 e não inserir nada para DataFrame vazio."""
        rows = upsert_table(engine, "daily_summary", pl.DataFrame())
        assert rows == 0

        with engine.connect() as conn:
            count = conn.execute(sa.text("SELECT COUNT(*) FROM gold_daily_summary")).scalar()
        assert count == 0

    def test_raises_for_unknown_table(self, engine: sa.Engine, daily_df: pl.DataFrame) -> None:
        """Deve lançar KeyError para nome de tabela desconhecido."""
        with pytest.raises(KeyError, match="desconhecida"):
            upsert_table(engine, "tabela_invalida", daily_df)

    def test_correct_cost_value_is_stored(self, engine: sa.Engine, daily_df: pl.DataFrame) -> None:
        """O valor de cost_brl deve ser armazenado com precisão."""
        upsert_table(engine, "daily_summary", daily_df)

        with engine.connect() as conn:
            cost = conn.execute(sa.text("SELECT cost_brl FROM gold_daily_summary WHERE source = 'google_ads'")).scalar()

        assert float(cost) == pytest.approx(2_500.0)


# ─── Testes de load_gold_parquet ──────────────────────────────────────────────


class TestLoadGoldParquet:
    """Testes do loader que lê Parquet e insere no Postgres."""

    def test_loads_parquet_into_postgres(self, engine: sa.Engine, daily_df: pl.DataFrame, tmp_path: Path) -> None:
        """Deve ler o Parquet e inserir as linhas corretamente."""
        parquet_path = tmp_path / "daily_summary_20260312.parquet"
        daily_df.write_parquet(parquet_path)

        rows = load_gold_parquet(engine, "daily_summary", parquet_path)
        assert rows == 2

    def test_parquet_data_matches_database(self, engine: sa.Engine, daily_df: pl.DataFrame, tmp_path: Path) -> None:
        """Os dados do Parquet devem estar íntegros após a carga."""
        parquet_path = tmp_path / "daily_summary_20260312.parquet"
        daily_df.write_parquet(parquet_path)
        load_gold_parquet(engine, "daily_summary", parquet_path)

        with engine.connect() as conn:
            result = conn.execute(sa.text("SELECT source, clicks FROM gold_daily_summary ORDER BY source")).fetchall()

        assert result[0] == ("google_ads", 5_000)
        assert result[1] == ("meta_ads", 3_000)


# ─── Testes de find_latest_gold_file e run_loader ─────────────────────────────


class TestFindLatestGoldFile:
    """Testes da função utilitária de localização de arquivos Gold."""

    def test_returns_none_for_missing_directory(self, tmp_path: Path) -> None:
        """Deve retornar None quando o diretório não existe."""
        result = find_latest_gold_file(str(tmp_path), "daily_summary")
        assert result is None

    def test_returns_most_recent_file(self, tmp_path: Path) -> None:
        """Deve retornar o arquivo com maior nome lexicográfico."""
        table_dir = tmp_path / "daily_summary"
        table_dir.mkdir()
        (table_dir / "daily_summary_20260310_080000.parquet").touch()
        (table_dir / "daily_summary_20260312_143000.parquet").touch()

        result = find_latest_gold_file(str(tmp_path), "daily_summary")
        assert result is not None
        assert result.name == "daily_summary_20260312_143000.parquet"


class TestRunLoader:
    """Testes do entrypoint de carga run_loader."""

    def test_loads_all_three_tables(
        self,
        engine: sa.Engine,
        postgres_container: PostgresContainer,
        daily_df: pl.DataFrame,
        campaign_df: pl.DataFrame,
        source_df: pl.DataFrame,
        tmp_path: Path,
    ) -> None:
        """Deve carregar as três tabelas Gold no PostgreSQL."""
        gold_root = tmp_path / "gold"

        # Cria os arquivos Parquet Gold
        for table_name, df in [
            ("daily_summary", daily_df),
            ("campaign_summary", campaign_df),
            ("source_comparison", source_df),
        ]:
            table_dir = gold_root / table_name
            table_dir.mkdir(parents=True)
            df.write_parquet(table_dir / f"{table_name}_20260312_100000.parquet")

        results = run_loader(
            gold_root=str(gold_root),
            dsn=postgres_container.get_connection_url(),
        )

        assert results["daily_summary"] == 2
        assert results["campaign_summary"] == 2
        assert results["source_comparison"] == 2

    def test_returns_none_when_no_gold_files(
        self,
        postgres_container: PostgresContainer,
        tmp_path: Path,
    ) -> None:
        """Deve retornar None para tabelas sem arquivo Gold."""
        results = run_loader(
            gold_root=str(tmp_path),
            dsn=postgres_container.get_connection_url(),
        )
        assert results["daily_summary"] is None
        assert results["campaign_summary"] is None
        assert results["source_comparison"] is None

    def test_result_is_idempotent(
        self,
        engine: sa.Engine,
        postgres_container: PostgresContainer,
        daily_df: pl.DataFrame,
        tmp_path: Path,
    ) -> None:
        """Executar run_loader duas vezes não deve duplicar linhas."""
        gold_root = tmp_path / "gold"
        table_dir = gold_root / "daily_summary"
        table_dir.mkdir(parents=True)
        daily_df.write_parquet(table_dir / "daily_summary_20260312_100000.parquet")

        dsn = postgres_container.get_connection_url()
        run_loader(gold_root=str(gold_root), dsn=dsn)
        run_loader(gold_root=str(gold_root), dsn=dsn)

        with engine.connect() as conn:
            count = conn.execute(sa.text("SELECT COUNT(*) FROM gold_daily_summary")).scalar()

        assert count == 2  # não duplicou na segunda execução
