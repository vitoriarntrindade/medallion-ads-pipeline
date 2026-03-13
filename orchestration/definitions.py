"""Definitions Dagster — entrypoint do dagster dev.

Registra todos os assets, resources e jobs do pipeline de
Ads Analytics. Este arquivo é referenciado pelo dagster.yaml
e pelo comando dagster dev.

Para iniciar a UI local:
    dagster dev -f orchestration/definitions.py
"""

import os

from dagster import Definitions, load_assets_from_modules

from orchestration.assets import bronze, gold, postgres, silver, validation
from orchestration.jobs import daily_schedule, full_pipeline_job
from orchestration.resources.postgres import PostgresResource

# ─── Assets ───────────────────────────────────────────────────────────────────
all_assets = load_assets_from_modules([bronze, silver, validation, gold, postgres])

# ─── Resources ────────────────────────────────────────────────────────────────
resources = {
    "postgres": PostgresResource(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "ad_analytics"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    ),
}

# ─── Definitions ──────────────────────────────────────────────────────────────
defs = Definitions(
    assets=all_assets,
    resources=resources,
    jobs=[full_pipeline_job],
    schedules=[daily_schedule],
)
