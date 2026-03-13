"""Job Dagster que orquestra o pipeline completo de Ads Analytics.

Define o job full_pipeline_job agrupando todos os assets em sequência:
Bronze → Silver → Validação → Gold → PostgreSQL.
"""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    description=(
        "Pipeline completo: ingestão Bronze → transformação Silver → validação GX → agregação Gold → carga PostgreSQL."
    ),
    selection=AssetSelection.groups(
        "ingestion",
        "transformation",
        "quality",
        "aggregation",
        "serving",
    ),
)

# Roda todo dia às 06:00 (horário UTC)
daily_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",  # min hora dia mês dia-semana
    name="daily_pipeline_schedule",
)
