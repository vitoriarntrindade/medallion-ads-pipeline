"""Testes da camada de observabilidade — metrics e health check."""

from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from observability.health_check import LayerStatus, app
from observability.metrics import Timer, ingestion_metadata, postgres_metadata

# ─── Timer ────────────────────────────────────────────────────────────────────


def test_timer_measures_elapsed_time() -> None:
    """Timer deve registrar tempo decorrido após o bloco with."""
    with Timer() as t:
        pass
    assert t.elapsed >= 0.0
    assert isinstance(t.elapsed, float)


# ─── Metrics ──────────────────────────────────────────────────────────────────


def test_ingestion_metadata_counts_sources(tmp_path: Path) -> None:
    """ingestion_metadata deve contar fontes ok e falhas corretamente."""
    results = {
        "google_ads": tmp_path / "file.parquet",
        "meta_ads": None,
        "tiktok_ads": tmp_path / "file2.parquet",
    }
    meta = ingestion_metadata(results, duration=1.5)

    assert meta["fontes_ok"].value == 2
    assert meta["fontes_falha"].value == 1
    assert meta["duration_seconds"].value == 1.5


def test_postgres_metadata_sums_total_rows() -> None:
    """postgres_metadata deve somar o total de linhas corretamente."""
    results = {
        "daily_summary": 10,
        "campaign_summary": 5,
        "source_comparison": None,  # falhou
    }
    meta = postgres_metadata(results, duration=0.8)

    assert meta["total_rows_upserted"].value == 15
    assert meta["rows_daily_summary"].value == 10
    assert "rows_source_comparison" not in meta


# ─── Health Check ─────────────────────────────────────────────────────────────


client = TestClient(app)


def test_health_liveness_returns_ok() -> None:
    """/health deve retornar status ok."""
    response = client.get("/health/")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_health_detail_degraded_without_storage(tmp_path: Path) -> None:
    """/health/detail deve retornar degraded quando storage não existe."""
    pg_ok = LayerStatus(ok=True, message="ok")
    with patch("observability.health_check._STORAGE_ROOT", tmp_path / "nonexistent"):
        with patch("observability.health_check._check_postgres", return_value=pg_ok):
            response = client.get("/health/detail")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


def test_health_detail_healthy_with_recent_files(tmp_path: Path) -> None:
    """/health/detail deve retornar healthy quando todas as camadas têm arquivos recentes."""
    for layer in ["bronze", "silver", "gold"]:
        layer_dir = tmp_path / layer / "source"
        layer_dir.mkdir(parents=True)
        (layer_dir / "data.parquet").write_bytes(b"fake")

    pg_ok = LayerStatus(ok=True, message="ok")
    with patch("observability.health_check._STORAGE_ROOT", tmp_path):
        with patch("observability.health_check._check_postgres", return_value=pg_ok):
            response = client.get("/health/detail")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["bronze"]["ok"] is True
    assert data["silver"]["ok"] is True
    assert data["gold"]["ok"] is True
