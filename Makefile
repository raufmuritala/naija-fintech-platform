# =============================================================================
# NaijaBank Data Platform — Makefile
# Quick commands for common dev tasks
# =============================================================================

.PHONY: help up down restart logs seed gcp-setup airflow-ui pgadmin lint

# Load .env
ifneq (,$(wildcard .env))
    include .env
    export
endif

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Docker ───────────────────────────────────────────────────────────────────
up:  ## Start all services (Airflow, Postgres, pgAdmin)
	docker compose up -d
	@echo ""
	@echo "  Airflow UI : http://localhost:8080  (admin / admin)"
	@echo "  pgAdmin    : http://localhost:5050  (admin@naijabank.dev / admin)"
	@echo "  Source DB  : localhost:5433  (naija / naija123)"

down:  ## Stop all services
	docker compose down

restart:  ## Restart all services
	docker compose down && docker compose up -d

logs:  ## Tail Airflow scheduler logs
	docker compose logs -f airflow-scheduler

logs-web:  ## Tail Airflow webserver logs
	docker compose logs -f airflow-webserver

# ─── Data generation ──────────────────────────────────────────────────────────
seed:  ## Generate and load seed data into postgres-source
	@echo "Installing dependencies locally..."
	pip install faker psycopg2-binary python-dotenv -q
	SOURCE_DB_CONN_LOCAL="postgresql://naija:naija123@localhost:5433/naijabank" \
		python scripts/generate_seed_data.py

seed-small:  ## Seed with small dataset (50 users, 30 days) for quick testing
	SEED_USERS=50 SEED_DAYS=30 \
	SOURCE_DB_CONN_LOCAL="postgresql://naija:naija123@localhost:5433/naijabank" \
		python scripts/generate_seed_data.py

# ─── GCP ──────────────────────────────────────────────────────────────────────
gcp-setup:  ## Bootstrap GCP resources (run once)
	bash scripts/setup_gcp.sh

# ─── Quick access ─────────────────────────────────────────────────────────────
airflow-ui:  ## Open Airflow UI in browser
	open http://localhost:8080 || xdg-open http://localhost:8080

pgadmin:  ## Open pgAdmin in browser
	open http://localhost:5050 || xdg-open http://localhost:5050

psql:  ## Connect to source DB via psql
	docker exec -it $$(docker compose ps -q postgres-source) \
		psql -U naija -d naijabank

# ─── Code quality ─────────────────────────────────────────────────────────────
lint:  ## Run ruff linter
	ruff check airflow/dags/ scripts/ dbt/

format:  ## Auto-format with black
	black airflow/dags/ scripts/

test:  ## Run pytest
	pytest tests/ -v

# ─── dbt ──────────────────────────────────────────────────────────────────────
dbt-run:  ## Run dbt models
	cd dbt && dbt run

dbt-test:  ## Run dbt tests
	cd dbt && dbt test

dbt-docs:  ## Generate and serve dbt docs
	cd dbt && dbt docs generate && dbt docs serve --port 8081
