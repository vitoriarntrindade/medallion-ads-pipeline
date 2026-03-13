# 📊 Ad Analytics Data Platform

> Pipeline de dados end-to-end para consolidação e análise de campanhas de mídia paga,  construído simulando as 
> mesmas práticas usadas em plataformas de dados de escala empresarial.

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
  <img src="https://img.shields.io/badge/Polars-1.39-CD792C?style=for-the-badge&logo=polars&logoColor=white"/>
  <img src="https://img.shields.io/badge/Dagster-1.12-4F46E5?style=for-the-badge&logo=dagster&logoColor=white"/>
  <img src="https://img.shields.io/badge/PostgreSQL-16-336791?style=for-the-badge&logo=postgresql&logoColor=white"/>
  <img src="https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
  <img src="https://img.shields.io/badge/Great_Expectations-1.15-FF6B6B?style=for-the-badge"/>
  <img src="https://img.shields.io/badge/Testes-262_passing-22C55E?style=for-the-badge&logo=pytest&logoColor=white"/>
  <img src="https://img.shields.io/badge/Cobertura-100%25-22C55E?style=for-the-badge"/>
</p>

---

## 📋 Sumário

- [O Problema](#-o-problema)
- [Visão Geral da Arquitetura](#-visão-geral-da-arquitetura)
- [Stack Tecnológico](#-stack-tecnológico)
- [Design do Pipeline](#-design-do-pipeline)
- [Funcionalidades](#-funcionalidades)
- [Fluxo de Dados](#-fluxo-de-dados)
- [Estrutura do Projeto](#-estrutura-do-projeto)
- [Como Rodar](#-como-rodar)
- [Comandos Make](#-comandos-make)
- [Testes](#-testes)
- [Decisões de Engenharia](#-decisões-de-engenharia)
- [Escalabilidade](#-escalabilidade)
- [Melhorias Futuras](#-melhorias-futuras)

---

## 🎯 O Problema

Empresas que anunciam em múltiplas plataformas de mídia paga (Google Ads, Meta Ads, TikTok Ads) enfrentam um problema estrutural: **cada plataforma entrega dados em schemas completamente diferentes**, com nomenclaturas inconsistentes, granularidades distintas e sem garantia de qualidade.

O resultado prático:
- Analistas passam horas consolidando planilhas manualmente
- Métricas de ROAS, CPC e CPM calculadas de formas diferentes por plataforma
- Impossibilidade de comparar performance entre canais com confiança
- Dados inconsistentes chegando a dashboards sem nenhuma validação

Este projeto resolve esse problema construindo um **pipeline de dados confiável e automatizado** que:

1. **Coleta** dados brutos de 3 plataformas com schemas intencionalmente diferentes
2. **Normaliza** para um schema unificado com validação de qualidade
3. **Agrega** em tabelas analíticas prontas para consumo
4. **Persiste** no PostgreSQL com upsert idempotente
5. **Orquestra** tudo via Dagster com rastreabilidade completa

---

## 🏗️ Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FONTES DE DADOS                             │
│                                                                     │
│   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐             │
│   │  Google Ads │   │   Meta Ads  │   │  TikTok Ads │             │
│   │  (FastAPI)  │   │  (FastAPI)  │   │  (FastAPI)  │             │
│   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘             │
└──────────┼─────────────────┼─────────────────┼────────────────────┘
           │                 │                 │
           └─────────────────┴─────────────────┘
                             │  HTTP + httpx
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CAMADA BRONZE 🥉                              │
│                                                                     │
│  Raw JSON → Parquet particionado por fonte e timestamp              │
│  Schema original preservado — dados imutáveis                       │
│  storage/bronze/{source}/{source}_{timestamp}.parquet               │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  Polars transformations
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       CAMADA SILVER 🥈                              │
│                                                                     │
│  Schema unificado (SilverRow) com Pydantic contracts                │
│  Normalização de nomes, tipos e moeda (BRL)                         │
│  storage/silver/{source}/{source}_{timestamp}.parquet               │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │               GREAT EXPECTATIONS VALIDATION                   │  │
│  │  ✓ Tipos de dados        ✓ Ranges de métricas                │  │
│  │  ✓ Valores não-nulos     ✓ Consistência entre colunas        │  │
│  │  ✓ Cardinalidade         ✓ Invariantes de negócio            │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  Polars aggregations
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        CAMADA GOLD 🥇                               │
│                                                                     │
│  Três tabelas analíticas prontas para consumo:                      │
│                                                                     │
│  ┌─────────────────┐ ┌──────────────────┐ ┌──────────────────┐    │
│  │  daily_summary  │ │campaign_summary  │ │source_comparison │    │
│  │  (data + fonte) │ │(fonte + campanha) │ │  (por plataforma)│    │
│  └────────┬────────┘ └────────┬─────────┘ └────────┬─────────┘    │
└───────────┼────────────────────┼────────────────────┼──────────────┘
            └────────────────────┴────────────────────┘
                                 │  SQLAlchemy + psycopg2
                                 │  INSERT ... ON CONFLICT DO UPDATE
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         POSTGRESQL 16                               │
│                                                                     │
│   gold_daily_summary │ gold_campaign_summary │ gold_source_comparison│
│   Upsert idempotente — re-execuções são seguras                     │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      ORQUESTRAÇÃO — DAGSTER                         │
│                                                                     │
│  bronze_data → silver_data → silver_validation → gold_data          │
│                                                      → postgres_load │
│                                                                     │
│  Assets com metadata visível na UI · Rastreabilidade por execução   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Stack Tecnológico

### Processamento de Dados
| Tecnologia | Versão | Papel |
|---|---|---|
| **Python** | 3.12 | Linguagem principal |
| **Polars** | 1.39 | Transformações e agregações (lazy evaluation) |
| **Pandas** | 2.2 | Interoperabilidade com SQLAlchemy |
| **PyArrow** | 23.0 | Serialização Parquet |

### Qualidade de Dados
| Tecnologia | Versão | Papel |
|---|---|---|
| **Great Expectations** | 1.15 | Suite de expectativas com 16+ validações |
| **Pydantic** | 2.12 | Contratos de schema (SilverRow, PostgresSettings) |

### Banco de Dados
| Tecnologia | Versão | Papel |
|---|---|---|
| **PostgreSQL** | 16 | Persistência da camada Gold |
| **SQLAlchemy** | 2.0 | Core (sem ORM) — controle total do SQL |
| **psycopg2** | 2.9 | Driver PostgreSQL |

### Orquestração e Observabilidade
| Tecnologia | Versão | Papel |
|---|---|---|
| **Dagster** | 1.12 | Asset-based orchestration com UI |
| **FastAPI** | 0.135 | APIs mock + health check endpoint |
| **Loguru** | 0.7 | Logging estruturado com rotação |

### Infraestrutura e Testes
| Tecnologia | Versão | Papel |
|---|---|---|
| **Docker** | 28.4 | Containerização |
| **Docker Compose** | v2 | Orquestração local multi-serviço |
| **pytest** | 9.0 | Framework de testes |
| **Testcontainers** | 4.14 | PostgreSQL real em testes de integração |

---

## 🔬 Design do Pipeline

### Por que Medallion Architecture (Bronze / Silver / Gold)?

A arquitetura em camadas não é apenas uma convenção — é uma decisão de engenharia com implicações práticas:

**Bronze — Dados Brutos Imutáveis**
- Preserva o dado exatamente como foi recebido da API
- Permite reprocessamento completo se a lógica de transformação mudar
- Auditabilidade: sempre é possível saber o que veio da fonte
- Parquet com timestamp no nome → particionamento natural por ingestão

**Silver — Schema Unificado com Contratos**
- `SilverRow` é um modelo Pydantic que define o contrato de dados
- Cada fonte tem seu próprio transformer que normaliza para `SilverRow`
- Great Expectations valida 16+ regras antes de avançar para Gold
- Falhas de validação bloqueiam o pipeline — dados ruins não chegam ao usuário

**Gold — Tabelas Analíticas Prontas**
- Três visões específicas para diferentes perguntas de negócio
- Métricas calculadas uma única vez, com proteção contra divisão por zero
- Upsert idempotente: re-executar o pipeline não duplica dados

### Validação com Great Expectations

As expectativas cobrem quatro categorias:

```
Estrutura       → colunas obrigatórias, tipos de dados
Completude      → valores não-nulos em campos críticos
Ranges          → impressions ≥ 0, cost > 0, CTR entre 0% e 100%
Consistência    → clicks ≤ impressions, conversions ≤ clicks
```

### Upsert Idempotente no PostgreSQL

```sql
-- Executar duas vezes com os mesmos dados = mesmo resultado
INSERT INTO gold_daily_summary (date, source, impressions, ...)
VALUES (...)
ON CONFLICT (date, source)
DO UPDATE SET impressions = EXCLUDED.impressions, ...
```

Isso garante que o pipeline pode ser re-executado a qualquer momento sem efeitos colaterais — fundamental para recuperação de falhas e backfill de dados.

---

## ✨ Funcionalidades

| Feature | Descrição |
|---|---|
| 🔄 **Multi-source ingestion** | Coleta de 3 plataformas com schemas diferentes via HTTP |
| 🏗️ **Medallion Architecture** | Bronze → Silver → Gold com responsabilidades claras |
| ✅ **Data Quality Gate** | Great Expectations bloqueia dados inválidos antes da Gold |
| 🔁 **Idempotência total** | Re-execuções seguras em todas as camadas |
| 📊 **3 visões analíticas** | Daily, Campaign e Source como tabelas Gold prontas |
| 🎯 **Asset observability** | Metadata de cada execução visível na UI do Dagster |
| 🏥 **Health check HTTP** | `/health/detail` verifica Bronze/Silver/Gold/Postgres |
| 🐳 **Stack completo em Docker** | Um `docker compose up` sobe tudo |
| 🧪 **262 testes** | Unit, integração e Testcontainers (PostgreSQL real) |
| 📋 **Make CLI** | 30+ atalhos para todas as operações |

---

## 🌊 Fluxo de Dados

Veja como um dado percorre todo o sistema, do momento da extração até estar disponível para análise:

```
1. EXTRAÇÃO
   ├── GoogleAdsExtractor.run(start_date, end_date)
   ├── HTTP GET /google-ads/campaigns → JSON com schema proprietário
   └── bronze_writer.py → storage/bronze/google_ads/google_ads_20260312_143022.parquet

2. TRANSFORMAÇÃO
   ├── find_latest_bronze_file("google_ads") → seleciona arquivo mais recente
   ├── GoogleAdsTransformer.run(bronze_file)
   │   ├── pl.read_parquet(bronze_file)           # lê dado bruto
   │   ├── pl.col("spend").alias("cost_brl")      # normaliza nomenclatura
   │   ├── pl.col("date_start").cast(pl.Date)     # corrige tipos
   │   └── SilverRow.model_validate(row)          # valida contrato Pydantic
   └── silver_writer.py → storage/silver/google_ads/google_ads_20260312_143025.parquet

3. VALIDAÇÃO
   ├── Great Expectations carrega o Parquet Silver
   ├── Executa 16+ expectativas (ranges, tipos, consistência)
   ├── PASS → pipeline continua
   └── FAIL → ValueError lançado, Dagster marca execução como falha

4. AGREGAÇÃO
   ├── load_silver_files("storage/silver") → carrega todos os sources em um DataFrame
   ├── build_daily_summary(df)      → GROUP BY date, source + métricas calculadas
   ├── build_campaign_summary(df)   → GROUP BY source, campaign_name
   ├── build_source_comparison(df)  → GROUP BY source (visão macro)
   └── write_gold() → storage/gold/{table}/{table}_20260312_143030.parquet

5. CARGA
   ├── find_latest_gold_file("daily_summary")
   ├── pl.read_parquet() → pandas → list[dict]
   ├── pg_insert(table).values(records)
   │       .on_conflict_do_update(index_elements=["date", "source"], ...)
   └── PostgreSQL: gold_daily_summary atualizado

6. OBSERVABILIDADE
   ├── context.add_output_metadata({"total_rows": 1250, "duration_seconds": 2.4})
   ├── Dagster UI: metadata visível por execução
   └── GET /health/detail → {"status": "healthy", "bronze": {...}, "postgres": {...}}
```

---

## 📁 Estrutura do Projeto

```
ad_analytics_pipeline/
│
├── 📡 sources/                    # APIs mock das plataformas de Ads
│   ├── main.py                    # FastAPI app com /health
│   ├── google_ads.py              # Schema proprietário Google
│   ├── meta_ads.py                # Schema proprietário Meta
│   └── tiktok_ads.py              # Schema proprietário TikTok
│
├── 🔽 ingestion/                  # Extração e persistência Bronze
│   ├── extractors/
│   │   ├── base.py                # BaseExtractor com retry e logging
│   │   ├── google_ads.py
│   │   ├── meta_ads.py
│   │   └── tiktok_ads.py
│   ├── bronze_writer.py           # Serialização Parquet com timestamp
│   ├── http_client.py             # Cliente httpx com timeout e headers
│   ├── settings.py                # Configuração via pydantic-settings
│   └── run_ingestion.py           # Entrypoint CLI
│
├── 🔄 pipeline/                   # Transformações Bronze → Silver → Gold
│   ├── bronze_to_silver/
│   │   ├── schema.py              # SilverRow — contrato Pydantic
│   │   ├── base_transformer.py    # Lógica comum entre transformers
│   │   ├── google_ads.py          # Normalização Google → Silver
│   │   ├── meta_ads.py            # Normalização Meta → Silver
│   │   ├── tiktok_ads.py          # Normalização TikTok → Silver
│   │   ├── silver_writer.py       # Persistência Silver em Parquet
│   │   └── run_transformation.py  # Entrypoint CLI
│   │
│   ├── validation/
│   │   ├── silver_suite.py        # 16+ Great Expectations
│   │   ├── validator.py           # ValidationReport + execução GX
│   │   └── run_validation.py      # Entrypoint CLI
│   │
│   ├── silver_to_gold/
│   │   ├── aggregations.py        # 3 funções de agregação Polars
│   │   ├── gold_writer.py         # Persistência Gold em Parquet
│   │   └── run_gold.py            # Entrypoint CLI
│   │
│   └── gold_to_postgres/
│       ├── schema.py              # SQLAlchemy Core table definitions
│       ├── loader.py              # upsert_table com ON CONFLICT
│       ├── settings.py            # PostgresSettings via pydantic-settings
│       └── run_loader.py          # Entrypoint CLI
│
├── 🎼 orchestration/              # Dagster assets e resources
│   ├── assets/
│   │   ├── bronze.py              # Asset: ingestão Bronze
│   │   ├── silver.py              # Asset: transformação Silver
│   │   ├── validation.py          # Asset: validação GX (bloqueia em falha)
│   │   ├── gold.py                # Asset: agregação Gold
│   │   └── postgres.py            # Asset: carga PostgreSQL
│   ├── resources/
│   │   └── postgres.py            # PostgresResource injetável
│   ├── jobs.py                    # full_pipeline_job
│   └── definitions.py             # Definitions — entrypoint dagster dev
│
├── 📡 observability/              # Métricas e health check
│   ├── metrics.py                 # Timer + funções de metadata Dagster
│   └── health_check.py            # FastAPI /health e /health/detail
│
├── 🧪 tests/                      # 262 testes
│   ├── sources/                   # Testes das APIs mock
│   ├── ingestion/                 # Testes de extração e Bronze
│   ├── pipeline/                  # Testes de transformação, GX, Gold, Postgres
│   ├── orchestration/             # Testes unitários dos assets Dagster
│   └── observability/             # Testes de métricas e health check
│
├── 🐳 docker/
│   ├── Dockerfile                 # Multi-stage: builder + runtime
│   └── entrypoint-pipeline.sh     # Aguarda Postgres, roda pipeline completo
│
├── docker-compose.yml             # 6 serviços: postgres, apis, pipeline, dagster, health
├── Makefile                       # 30+ atalhos de desenvolvimento
├── pyproject.toml                 # Dependências e configuração de ferramentas
└── .env.example                   # Template de variáveis de ambiente
```

---

## 🚀 Como Rodar

### Pré-requisitos

- Python 3.12+
- Docker e Docker Compose v2
- Make

### Opção 1 — Stack completo com Docker (recomendado)

```bash
# Clone o repositório
git clone https://github.com/vitoriarntrindade/medallion-ads-pipeline.git
cd medallion-ads-pipelineg

# Sobe todos os serviços
docker compose up --build
```

Serviços disponíveis após o startup:

| Serviço | URL | Descrição |
|---|---|---|
| APIs mock | http://localhost:8000/docs | Swagger das 3 plataformas |
| Dagster UI | http://localhost:3000 | Visualização e execução do pipeline |
| Health Check | http://localhost:8080/health/detail | Status de todos os componentes |
| PostgreSQL | localhost:5432 | Banco de dados Gold |

### Opção 2 — Desenvolvimento local com Make

```bash
# 1. Configura o ambiente virtual
make setup

# 2. Copia e edita as variáveis de ambiente
cp .env.example .env

# 3. Sobe o PostgreSQL
make db

# 4. Sobe as APIs mock em background
make apis

# 5. Executa o pipeline completo
make run
```

### Opção 3 — Etapas individuais

```bash
make ingest      # Bronze: extrai dados das APIs
make transform   # Silver: normaliza e unifica schemas
make validate    # Validação: Great Expectations
make gold        # Gold: agrega em tabelas analíticas
make load        # PostgreSQL: upsert das tabelas Gold
```

---

## 🔧 Comandos Make

```bash
# ── Ambiente ───────────────────────────────────────────
make setup           # Cria venv e instala dependências
make install         # Reinstala dependências

# ── Qualidade de código ────────────────────────────────
make lint            # Verifica estilo com ruff
make format          # Formata código com ruff
make typecheck       # Verifica tipos com mypy

# ── Testes ─────────────────────────────────────────────
make test            # Todos os 262 testes
make test-fast       # Sem Testcontainers (mais rápido)
make test-pipeline   # Só testes do pipeline
make test-orchestration  # Só testes Dagster

# ── Pipeline manual ────────────────────────────────────
make run             # Pipeline completo end-to-end
make ingest          # Apenas ingestão Bronze
make transform       # Apenas transformação Silver
make validate        # Apenas validação GX
make gold            # Apenas agregação Gold
make load            # Apenas carga PostgreSQL

# ── Infraestrutura ─────────────────────────────────────
make up              # Docker Compose completo
make down            # Para todos os containers
make db              # Só o PostgreSQL
make db-shell        # psql interativo
make logs            # Logs em tempo real

# ── Dagster ────────────────────────────────────────────
make dagster         # UI em http://localhost:3000

# ── Limpeza ────────────────────────────────────────────
make clean           # Remove cache e artefatos
make clean-storage   # Limpa Bronze/Silver/Gold/logs
```

---

## 🧪 Testes

O projeto tem **262 testes** organizados em 4 categorias:

| Categoria | Quantidade | Tecnologia | O que testa |
|---|---|---|---|
| **Unitários** | ~180 | pytest + mocks | Transformers, aggregations, schemas, assets |
| **Integração Bronze→Silver** | ~48 | pytest | Transformação completa por fonte |
| **Integração Gold→Postgres** | 17 | Testcontainers | PostgreSQL 16 real em container |
| **Observabilidade** | 6 | httpx TestClient | Health check endpoints |

### Testcontainers — PostgreSQL real nos testes

Em vez de mockar o banco ou usar SQLite, os testes de integração sobem um **container PostgreSQL 16 real**:

```python
@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg  # container compartilhado por toda a sessão
```

Isso garante paridade total com o ambiente de produção — sem surpresas de comportamento SQL.

```bash
# Rodar todos os testes
make test

# Pular Testcontainers (sem Docker disponível)
make test-fast
```

---

## 🧠 Decisões de Engenharia

### Polars em vez de Pandas

**Problema:** Pandas tem overhead de memória significativo e performance limitada para transformações em colunas.

**Decisão:** Polars com lazy evaluation para todas as transformações. O plano de execução é otimizado antes de rodar, com predicate pushdown automático.

**Impacto:** 5-10x mais rápido em benchmarks de transformação; uso de memória até 3x menor em datasets grandes.

### SQLAlchemy Core em vez de ORM

**Problema:** ORMs abstraem demais o SQL, dificultando upserts com `ON CONFLICT DO UPDATE`.

**Decisão:** SQLAlchemy Core — controle total do SQL gerado, sem mágica de ORM, com type safety do Python.

**Impacto:** Upsert idempotente implementado diretamente com `pg_insert().on_conflict_do_update()` — comportamento previsível e testável.

### pydantic-settings em vez de `os.getenv`

**Problema:** `os.getenv` retorna sempre `str | None`, sem validação de tipo, sem defaults documentados, sem suporte a `.env`.

**Decisão:** `pydantic-settings` com campos tipados e `@computed_field` para o DSN.

**Impacto:** `POSTGRES_PORT=abc` causa `ValidationError` no startup — falha rápida e clara antes de qualquer I/O.

### Dagster Assets em vez de Ops/Graphs

**Problema:** Ops tradicionais não têm rastreabilidade de materialização — você sabe que o job rodou, mas não *o que* foi produzido.

**Decisão:** Software-defined Assets com `add_output_metadata()` — cada execução registra linhas processadas, paths e duração.

**Impacto:** Time de dados consegue ver no histórico do Dagster exatamente o que cada execução produziu, sem precisar de logs externos.

### Testcontainers em vez de SQLite para testes

**Problema:** SQLite não suporta `ON CONFLICT DO UPDATE` com a mesma semântica do PostgreSQL. Testes passariam, produção quebraria.

**Decisão:** Testcontainers sobe um PostgreSQL 16-alpine real, compartilhado por toda a sessão de testes para minimizar overhead.

**Impacto:** Zero diferença entre comportamento nos testes e em produção.

### Arquitetura de validação como gate

**Problema:** Dados inválidos chegando à Gold e ao PostgreSQL sem nenhuma barreira.

**Decisão:** O asset `silver_validation` lança `ValueError` se qualquer fonte reprovar — o Dagster interrompe o pipeline e não executa `gold_data` nem `postgres_load`.

**Impacto:** Dados ruins nunca chegam às camadas downstream. A causa raiz é identificada na camada correta.

---

## 📈 Escalabilidade

O projeto foi desenhado para crescer sem refatoração estrutural:

**Adicionar uma nova fonte de dados**
```
1. Criar sources/nova_fonte.py        (FastAPI endpoint)
2. Criar ingestion/extractors/nova_fonte.py  (herdar BaseExtractor)
3. Criar pipeline/bronze_to_silver/nova_fonte.py  (herdar BaseTransformer)
```
As camadas Silver, Gold, PostgreSQL e Dagster não precisam mudar — o schema unificado `SilverRow` absorve a nova fonte.

**Volume de dados maior**
- Polars lazy evaluation já particiona o processamento automaticamente
- Parquet suporta particionamento por data nativamente
- PostgreSQL com índices compostos nas chaves naturais

**Histórico e backfill**
- Bronze preserva todos os dados brutos — reprocessamento completo possível a qualquer momento
- Upsert idempotente garante que reprocessar não duplica dados

**Streaming no futuro**
- A separação Bronze/Silver/Gold facilita a migração de batch para micro-batch
- Dagster suporta sensors para trigger por evento sem mudar os assets

---

## 🔭 Melhorias Futuras

| Melhoria | Impacto | Complexidade |
|---|---|---|
| **Dashboard Metabase/Streamlit** | Visualização das Gold tables | Baixa |
| **Particionamento por data no Parquet** | Performance em queries históricas | Média |
| **Sensor Dagster por arquivo novo** | Pipeline event-driven em vez de scheduled | Média |
| **Alertas de qualidade via Slack** | Notificação proativa de falhas GX | Média |
| **Streaming com Kafka** | Ingestão em near real-time | Alta |
| **dbt para transformações Silver→Gold** | SQL versionado e testável | Alta |
| **Great Expectations Data Docs** | Portal de qualidade publicado | Baixa |
| **CI/CD com GitHub Actions** | Testes automáticos em PR | Baixa |


---

<p align="center">
  Construído com atenção a cada detalhe de engenharia de dados.
</p>
