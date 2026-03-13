"""Coleta de métricas de execução do pipeline para o Dagster.

Fornece funções que constroem dicionários de metadata compatíveis
com `context.add_output_metadata()`, tornando as métricas visíveis
diretamente na UI do Dagster após cada materialização.
"""

import time
from pathlib import Path

from dagster import MetadataValue


def file_metadata(path: Path | None) -> dict:
    """Retorna metadata de um arquivo Parquet gerado.

    Args:
        path: Path do arquivo, ou None se não foi gerado.

    Returns:
        Dict com tamanho em KB, path e status.
    """
    if path is None:
        return {"status": MetadataValue.text("falhou")}

    size_kb = round(path.stat().st_size / 1024, 2) if path.exists() else 0
    return {
        "path": MetadataValue.path(str(path)),
        "size_kb": MetadataValue.float(size_kb),
        "status": MetadataValue.text("ok"),
    }


def ingestion_metadata(results: dict[str, Path | None], duration: float) -> dict:
    """Metadata de uma execução de ingestão Bronze.

    Args:
        results: Mapeamento fonte → Path gerado (ou None).
        duration: Duração total em segundos.

    Returns:
        Dict de metadata para `add_output_metadata()`.
    """
    successful = [src for src, p in results.items() if p is not None]
    failed = [src for src, p in results.items() if p is None]

    return {
        "fontes_ok": MetadataValue.int(len(successful)),
        "fontes_falha": MetadataValue.int(len(failed)),
        "duration_seconds": MetadataValue.float(round(duration, 2)),
        "fontes": MetadataValue.json({src: str(p) if p else "falhou" for src, p in results.items()}),
    }


def transformation_metadata(results: dict[str, Path | None], duration: float) -> dict:
    """Metadata de uma execução de transformação Bronze → Silver.

    Args:
        results: Mapeamento fonte → Path Silver gerado (ou None).
        duration: Duração total em segundos.

    Returns:
        Dict de metadata para `add_output_metadata()`.
    """
    successful = [src for src, p in results.items() if p is not None]

    return {
        "fontes_transformadas": MetadataValue.int(len(successful)),
        "duration_seconds": MetadataValue.float(round(duration, 2)),
        **{f"path_{src}": MetadataValue.path(str(p)) for src, p in results.items() if p},
    }


def gold_metadata(results: dict[str, Path | None], duration: float) -> dict:
    """Metadata de uma execução de agregação Silver → Gold.

    Args:
        results: Mapeamento tabela → Path Gold gerado (ou None).
        duration: Duração total em segundos.

    Returns:
        Dict de metadata para `add_output_metadata()`.
    """
    return {
        "tabelas_geradas": MetadataValue.int(sum(1 for p in results.values() if p)),
        "duration_seconds": MetadataValue.float(round(duration, 2)),
        **{f"path_{t}": MetadataValue.path(str(p)) for t, p in results.items() if p},
    }


def postgres_metadata(results: dict[str, int | None], duration: float) -> dict:
    """Metadata de uma execução de carga Gold → PostgreSQL.

    Args:
        results: Mapeamento tabela → linhas inseridas (ou None).
        duration: Duração total em segundos.

    Returns:
        Dict de metadata para `add_output_metadata()`.
    """
    total = sum(r for r in results.values() if r is not None)

    return {
        "total_rows_upserted": MetadataValue.int(total),
        "duration_seconds": MetadataValue.float(round(duration, 2)),
        **{f"rows_{t}": MetadataValue.int(r) for t, r in results.items() if r is not None},
    }


class Timer:
    """Cronômetro de contexto para medir duração de blocos de código.

    Uso:
        with Timer() as t:
            ...fazer algo...
        print(t.elapsed)  # segundos
    """

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_) -> None:
        self.elapsed = round(time.perf_counter() - self._start, 3)
