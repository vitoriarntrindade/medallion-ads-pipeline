"""Definição das tabelas Gold no PostgreSQL via SQLAlchemy Core.

Usamos SQLAlchemy Core (sem ORM) — a abordagem mais adequada para
pipelines de dados onde operamos em lote sobre DataFrames, não em
objetos individuais.

Cada tabela tem:
- Colunas de métricas correspondentes ao schema Gold
- Chave primária composta pela chave natural de negócio
- Timestamps de controle (loaded_at) gerenciados pelo banco
"""

import sqlalchemy as sa

metadata = sa.MetaData()

# ── daily_summary: uma linha por (date, source) ───────────────────────────────
daily_summary = sa.Table(
    "gold_daily_summary",
    metadata,
    sa.Column("date", sa.Date, nullable=False),
    sa.Column("source", sa.String(50), nullable=False),
    sa.Column("impressions", sa.BigInteger, nullable=False),
    sa.Column("clicks", sa.BigInteger, nullable=False),
    sa.Column("cost_brl", sa.Numeric(14, 6), nullable=False),
    sa.Column("conversions", sa.BigInteger, nullable=False),
    sa.Column("ctr_pct", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpc_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpm_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpa_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("loaded_at", sa.DateTime, server_default=sa.func.now(), onupdate=sa.func.now()),
    sa.PrimaryKeyConstraint("date", "source", name="pk_gold_daily_summary"),
)

# ── campaign_summary: uma linha por (source, campaign_name) ──────────────────
campaign_summary = sa.Table(
    "gold_campaign_summary",
    metadata,
    sa.Column("source", sa.String(50), nullable=False),
    sa.Column("campaign_name", sa.String(255), nullable=False),
    sa.Column("impressions", sa.BigInteger, nullable=False),
    sa.Column("clicks", sa.BigInteger, nullable=False),
    sa.Column("cost_brl", sa.Numeric(14, 6), nullable=False),
    sa.Column("conversions", sa.BigInteger, nullable=False),
    sa.Column("ctr_pct", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpc_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpm_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpa_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("loaded_at", sa.DateTime, server_default=sa.func.now(), onupdate=sa.func.now()),
    sa.PrimaryKeyConstraint("source", "campaign_name", name="pk_gold_campaign_summary"),
)

# ── source_comparison: uma linha por source ────────────────────────────────────
source_comparison = sa.Table(
    "gold_source_comparison",
    metadata,
    sa.Column("source", sa.String(50), nullable=False),
    sa.Column("impressions", sa.BigInteger, nullable=False),
    sa.Column("clicks", sa.BigInteger, nullable=False),
    sa.Column("cost_brl", sa.Numeric(14, 6), nullable=False),
    sa.Column("conversions", sa.BigInteger, nullable=False),
    sa.Column("ctr_pct", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpc_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpm_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("cpa_brl", sa.Numeric(10, 4), nullable=False),
    sa.Column("loaded_at", sa.DateTime, server_default=sa.func.now(), onupdate=sa.func.now()),
    sa.PrimaryKeyConstraint("source", name="pk_gold_source_comparison"),
)

# Mapeamento nome lógico → objeto Table (usado pelo loader)
TABLES: dict[str, sa.Table] = {
    "daily_summary": daily_summary,
    "campaign_summary": campaign_summary,
    "source_comparison": source_comparison,
}
