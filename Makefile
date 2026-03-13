# ─── Ad Analytics Pipeline — Makefile ────────────────────────────────────────
# Atalhos para os comandos mais usados no desenvolvimento e operação.
#
# Uso:
#   make help          lista todos os comandos
#   make setup         prepara o ambiente pela primeira vez
#   make run           executa o pipeline completo localmente
#   make up            sobe o stack Docker completo

SHELL := /bin/bash
PYTHON := .venve1/bin/python
PIP    := .venve1/bin/pip
PYTEST := .venve1/bin/pytest

.DEFAULT_GOAL := help

# ─── Ajuda ────────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Mostra esta mensagem de ajuda
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ─── Ambiente ─────────────────────────────────────────────────────────────────

.PHONY: setup
setup: ## Cria o venv e instala todas as dependências
	python3.12 -m venv .venve1
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@echo "✓ Ambiente pronto. Ative com: source .venve1/bin/activate"

.PHONY: install
install: ## Reinstala dependências sem recriar o venv
	$(PIP) install -e ".[dev]"

# ─── Qualidade de código ──────────────────────────────────────────────────────

.PHONY: lint
lint: ## Verifica estilo e erros com ruff
	.venve1/bin/ruff check .

.PHONY: format
format: ## Formata o código com ruff
	.venve1/bin/ruff format .

.PHONY: typecheck
typecheck: ## Verifica tipos com mypy
	.venve1/bin/mypy pipeline/ ingestion/ orchestration/ observability/

# ─── Testes ───────────────────────────────────────────────────────────────────

.PHONY: test
test: ## Roda todos os testes
	$(PYTEST) tests/ -q --tb=short

.PHONY: test-fast
test-fast: ## Roda testes exceto os de integração com Docker (Testcontainers)
	$(PYTEST) tests/ -q --tb=short --ignore=tests/pipeline/test_gold_to_postgres.py

.PHONY: test-verbose
test-verbose: ## Roda todos os testes com output detalhado
	$(PYTEST) tests/ -v --tb=short

.PHONY: test-pipeline
test-pipeline: ## Roda apenas os testes do pipeline
	$(PYTEST) tests/pipeline/ -v --tb=short

.PHONY: test-orchestration
test-orchestration: ## Roda apenas os testes de orquestração Dagster
	$(PYTEST) tests/orchestration/ -v --tb=short

.PHONY: test-observability
test-observability: ## Roda apenas os testes de observabilidade
	$(PYTEST) tests/observability/ -v --tb=short

# ─── APIs Mock ────────────────────────────────────────────────────────────────

.PHONY: apis
apis: ## Sobe as APIs mock em background (porta 8000)
	$(PYTHON) -m uvicorn sources.main:app --port 8000 --reload &
	@echo "✓ APIs mock rodando em http://localhost:8000"
	@echo "  Docs: http://localhost:8000/docs"

.PHONY: apis-stop
apis-stop: ## Para as APIs mock
	@pkill -f "uvicorn sources.main:app" && echo "✓ APIs mock encerradas" || echo "APIs mock não estavam rodando"

# ─── Pipeline manual ──────────────────────────────────────────────────────────

.PHONY: ingest
ingest: ## Executa a ingestão Bronze (últimos 7 dias)
	$(PYTHON) -m ingestion.run_ingestion

.PHONY: transform
transform: ## Executa a transformação Bronze → Silver
	$(PYTHON) -m pipeline.bronze_to_silver.run_transformation

.PHONY: validate
validate: ## Executa a validação Silver com Great Expectations
	$(PYTHON) -m pipeline.validation.run_validation

.PHONY: gold
gold: ## Executa a agregação Silver → Gold
	$(PYTHON) -m pipeline.silver_to_gold.run_gold

.PHONY: load
load: ## Carrega as tabelas Gold no PostgreSQL
	$(PYTHON) -m pipeline.gold_to_postgres.run_loader

.PHONY: run
run: ingest transform validate gold load ## Executa o pipeline completo localmente (end-to-end)
	@echo "✓ Pipeline completo finalizado."

# ─── Dagster ──────────────────────────────────────────────────────────────────

.PHONY: dagster
dagster: ## Sobe a UI do Dagster em http://localhost:3000
	.venve1/bin/dagster dev -f orchestration/definitions.py

# ─── Health Check ─────────────────────────────────────────────────────────────

.PHONY: health
health: ## Sobe o servidor de health check (porta 8080)
	$(PYTHON) -m uvicorn observability.health_check:app --port 8080 --reload &
	@echo "✓ Health check em http://localhost:8080/health/detail"

.PHONY: health-check
health-check: ## Chama o endpoint /health/detail e exibe o resultado
	@curl -s http://localhost:8080/health/detail | $(PYTHON) -m json.tool

# ─── Docker ───────────────────────────────────────────────────────────────────

.PHONY: up
up: ## Sobe o stack completo com Docker Compose
	docker compose up --build

.PHONY: up-detached
up-detached: ## Sobe o stack em background
	docker compose up --build -d
	@echo "✓ Stack rodando. Acesse:"
	@echo "  APIs mock:      http://localhost:8000/docs"
	@echo "  Dagster UI:     http://localhost:3000"
	@echo "  Health check:   http://localhost:8080/health/detail"

.PHONY: down
down: ## Para e remove todos os containers
	docker compose down

.PHONY: down-volumes
down-volumes: ## Para containers e remove volumes (apaga dados)
	docker compose down -v
	@echo "⚠ Volumes removidos — dados apagados."

.PHONY: db
db: ## Sobe apenas o PostgreSQL
	docker compose up postgres -d
	@echo "✓ PostgreSQL disponível em localhost:5432"

.PHONY: db-shell
db-shell: ## Abre o psql no container do PostgreSQL
	docker compose exec postgres psql -U postgres -d ad_analytics

.PHONY: logs
logs: ## Exibe logs de todos os serviços em tempo real
	docker compose logs -f

.PHONY: build
build: ## Constrói a imagem Docker sem subir os serviços
	docker build -f docker/Dockerfile -t ad-analytics-pipeline .

# ─── Limpeza ──────────────────────────────────────────────────────────────────

.PHONY: clean
clean: ## Remove artefatos temporários (cache, pyc, etc.)
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	@echo "✓ Artefatos removidos."

.PHONY: clean-storage
clean-storage: ## Remove todos os dados de storage (Bronze, Silver, Gold, logs)
	rm -rf storage/bronze storage/silver storage/gold storage/logs
	mkdir -p storage/bronze storage/silver storage/gold storage/logs
	@echo "⚠ Storage limpo."
