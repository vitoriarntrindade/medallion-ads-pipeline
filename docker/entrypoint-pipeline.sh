#!/bin/bash
# Entrypoint do serviço de pipeline completo.
#
# Aguarda o PostgreSQL ficar pronto, sobe as APIs mock em background,
# executa o pipeline completo uma vez e encerra.
#
# Variáveis de ambiente esperadas:
#   POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
#   GOOGLE_ADS_API_URL, META_ADS_API_URL, TIKTOK_ADS_API_URL

set -euo pipefail

echo "========================================"
echo "  Ad Analytics Pipeline"
echo "========================================"

# ─── Aguarda PostgreSQL ───────────────────────────────────────────────────────
echo "[1/4] Aguardando PostgreSQL em ${POSTGRES_HOST}:${POSTGRES_PORT}..."
until python -c "
import sqlalchemy as sa
engine = sa.create_engine('postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}')
with engine.connect() as c: c.execute(sa.text('SELECT 1'))
print('PostgreSQL ok')
" 2>/dev/null; do
    echo "  PostgreSQL não disponível ainda. Aguardando 3s..."
    sleep 3
done

# ─── Ingestão Bronze ─────────────────────────────────────────────────────────
echo "[2/4] Executando ingestão Bronze..."
python -m ingestion.run_ingestion

# ─── Transformação Bronze → Silver ───────────────────────────────────────────
echo "[3/4] Transformando Bronze → Silver..."
python -m pipeline.bronze_to_silver.run_transformation

# ─── Validação Silver ─────────────────────────────────────────────────────────
echo "      Validando qualidade Silver..."
python -m pipeline.validation.run_validation || echo "AVISO: Validação com falhas — continuando."

# ─── Agregação Silver → Gold ──────────────────────────────────────────────────
echo "      Agregando Silver → Gold..."
python -m pipeline.silver_to_gold.run_gold

# ─── Carga Gold → PostgreSQL ──────────────────────────────────────────────────
echo "[4/4] Carregando Gold → PostgreSQL..."
python -m pipeline.gold_to_postgres.run_loader

echo "========================================"
echo "  Pipeline concluído com sucesso."
echo "========================================"
