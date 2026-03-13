"""Funções de agregação Silver → Gold.

Cada função recebe um DataFrame Silver unificado (todas as fontes combinadas)
e retorna uma tabela Gold específica, calculando as métricas de negócio
padrão de mídia paga.

Métricas calculadas:
    CTR  = clicks / impressions * 100
    CPC  = cost_brl / clicks
    CPM  = cost_brl / impressions * 1000
    CPA  = cost_brl / conversions

Divisões por zero são tratadas com `fill_nan(0.0)` para garantir que
linhas com clicks ou impressions zero não quebrem o pipeline.
"""

import polars as pl


def build_daily_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Agrega métricas por fonte e data (granularidade diária).

    Produz uma linha por combinação (source, date) com os totais brutos
    e as métricas derivadas de eficiência calculadas sobre os totais.

    Args:
        df: DataFrame Silver com todas as fontes.

    Returns:
        DataFrame Gold com colunas:
            date, source, impressions, clicks, cost_brl, conversions,
            ctr_pct, cpc_brl, cpm_brl, cpa_brl.

    Raises:
        ValueError: Se o DataFrame estiver vazio ou faltar colunas obrigatórias.
    """
    _validate_input(df, required=["date", "source", "impressions", "clicks", "cost_brl", "conversions"])

    agg = (
        df.group_by(["date", "source"])
        .agg(
            pl.sum("impressions").alias("impressions"),
            pl.sum("clicks").alias("clicks"),
            pl.sum("cost_brl").alias("cost_brl"),
            pl.sum("conversions").alias("conversions"),
        )
        .sort(["date", "source"])
    )

    return _add_derived_metrics(agg)


def build_campaign_summary(df: pl.DataFrame) -> pl.DataFrame:
    """Agrega métricas por fonte e campanha (acumulado do período).

    Produz uma linha por combinação (source, campaign_name) com totais
    acumulados de todo o período disponível na Silver.

    Args:
        df: DataFrame Silver com todas as fontes.

    Returns:
        DataFrame Gold com colunas:
            source, campaign_name, impressions, clicks, cost_brl,
            conversions, ctr_pct, cpc_brl, cpm_brl, cpa_brl.

    Raises:
        ValueError: Se o DataFrame estiver vazio ou faltar colunas obrigatórias.
    """
    _validate_input(df, required=["source", "campaign_name", "impressions", "clicks", "cost_brl", "conversions"])

    agg = (
        df.group_by(["source", "campaign_name"])
        .agg(
            pl.sum("impressions").alias("impressions"),
            pl.sum("clicks").alias("clicks"),
            pl.sum("cost_brl").alias("cost_brl"),
            pl.sum("conversions").alias("conversions"),
        )
        .sort(["source", "campaign_name"])
    )

    return _add_derived_metrics(agg)


def build_source_comparison(df: pl.DataFrame) -> pl.DataFrame:
    """Agrega métricas por fonte para comparação entre plataformas.

    Produz uma linha por fonte com totais globais e métricas de eficiência,
    ordenadas por custo decrescente (maior investimento primeiro).

    Args:
        df: DataFrame Silver com todas as fontes.

    Returns:
        DataFrame Gold com colunas:
            source, impressions, clicks, cost_brl, conversions,
            ctr_pct, cpc_brl, cpm_brl, cpa_brl.

    Raises:
        ValueError: Se o DataFrame estiver vazio ou faltar colunas obrigatórias.
    """
    _validate_input(df, required=["source", "impressions", "clicks", "cost_brl", "conversions"])

    agg = (
        df.group_by("source")
        .agg(
            pl.sum("impressions").alias("impressions"),
            pl.sum("clicks").alias("clicks"),
            pl.sum("cost_brl").alias("cost_brl"),
            pl.sum("conversions").alias("conversions"),
        )
        .sort("cost_brl", descending=True)
    )

    return _add_derived_metrics(agg)


def load_silver_files(silver_root: str) -> pl.DataFrame:
    """Lê e unifica todos os arquivos Parquet Silver disponíveis.

    Varre cada subdiretório de fonte dentro do silver_root, seleciona
    o arquivo mais recente de cada fonte e os concatena em um único
    DataFrame. Fontes sem arquivo são silenciosamente ignoradas.

    Args:
        silver_root: Caminho raiz da camada Silver.

    Returns:
        DataFrame unificado com dados de todas as fontes disponíveis,
        ou DataFrame vazio se nenhum arquivo for encontrado.
    """
    from pathlib import Path

    sources = ["google_ads", "meta_ads", "tiktok_ads"]
    frames: list[pl.DataFrame] = []

    for source in sources:
        source_dir = Path(silver_root) / source
        if not source_dir.exists():
            continue

        parquet_files = sorted(source_dir.glob("*.parquet"))
        if not parquet_files:
            continue

        latest = parquet_files[-1]
        frames.append(pl.read_parquet(latest))

    if not frames:
        return pl.DataFrame()

    return pl.concat(frames)


# ─── Helpers internos ─────────────────────────────────────────────────────────


def _validate_input(df: pl.DataFrame, required: list[str]) -> None:
    """Valida que o DataFrame não está vazio e contém as colunas necessárias.

    Args:
        df: DataFrame a validar.
        required: Lista de nomes de colunas obrigatórias.

    Raises:
        ValueError: Se vazio ou faltando colunas.
    """
    if df.is_empty():
        raise ValueError("DataFrame Silver está vazio. Nenhuma agregação pode ser gerada.")

    missing = set(required) - set(df.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes no DataFrame Silver: {missing}")


def _add_derived_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Adiciona as métricas derivadas de eficiência ao DataFrame agregado.

    Trata divisões por zero substituindo NaN por 0.0, garantindo que
    linhas com impressions ou clicks zerados não propaguem valores inválidos.

    Args:
        df: DataFrame já agregado com colunas brutas (impressions, clicks,
            cost_brl, conversions).

    Returns:
        DataFrame original com as colunas de métricas adicionadas:
        ctr_pct, cpc_brl, cpm_brl, cpa_brl.
    """
    return df.with_columns(
        # CTR: clicks / impressions * 100  (NaN e Inf → 0.0)
        pl.when(pl.col("impressions") > 0)
        .then(pl.col("clicks") / pl.col("impressions") * 100.0)
        .otherwise(0.0)
        .round(4)
        .alias("ctr_pct"),
        # CPC: cost / clicks  (Inf → 0.0)
        pl.when(pl.col("clicks") > 0)
        .then(pl.col("cost_brl") / pl.col("clicks"))
        .otherwise(0.0)
        .round(4)
        .alias("cpc_brl"),
        # CPM: cost / impressions * 1000  (Inf → 0.0)
        pl.when(pl.col("impressions") > 0)
        .then(pl.col("cost_brl") / pl.col("impressions") * 1000.0)
        .otherwise(0.0)
        .round(4)
        .alias("cpm_brl"),
        # CPA: cost / conversions  (Inf → 0.0)
        pl.when(pl.col("conversions") > 0)
        .then(pl.col("cost_brl") / pl.col("conversions"))
        .otherwise(0.0)
        .round(4)
        .alias("cpa_brl"),
    )
