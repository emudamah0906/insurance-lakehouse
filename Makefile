.PHONY: up down down-volumes logs status \
        setup-venv test lint format \
        seed dag-trigger dbt-run dbt-test \
        clean ci-check

# ── Docker lifecycle ──────────────────────────────────────────────────────────

up:
	docker compose --env-file .env up -d
	@echo ""
	@echo "  Services starting (allow ~60s for Airflow init)"
	@echo "  MinIO Console : http://localhost:9001   user=minioadmin    pass=minioadmin123"
	@echo "  Airflow UI    : http://localhost:8080   user=admin         pass=admin"
	@echo "  Metabase      : http://localhost:3000   user=memudapuram@gmail.com"
	@echo ""

down:
	docker compose down

down-volumes:
	@echo "WARNING: this deletes ALL MinIO data, Postgres data, and logs."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	docker compose down -v

logs:
	docker compose logs -f --tail=100

status:
	docker compose ps

# ── Local Python dev ──────────────────────────────────────────────────────────

setup-venv:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements-dev.txt
	@echo "Activate with: source .venv/bin/activate"

test:
	.venv/bin/pytest tests/ -v

lint:
	.venv/bin/ruff check .
	.venv/bin/black --check .

format:
	.venv/bin/ruff check --fix .
	.venv/bin/black .

# ── CI gate (mirrors GitHub Actions locally) ─────────────────────────────────

ci-check: lint test
	@echo "--- dbt parse ---"
	cd dbt && ../.venv/bin/dbt parse --profiles-dir . --target dev
	@echo "--- terraform validate ---"
	cd terraform && terraform validate
	@echo "All CI checks passed."

# ── Data pipeline shortcuts ───────────────────────────────────────────────────

seed:
	.venv/bin/python data_generator/generate.py

dag-trigger:
	@echo "Usage: make dag-trigger DAG=02_bronze_silver"
	docker exec airflow-scheduler airflow dags trigger $(DAG)

dbt-run:
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt && ../.venv/bin/dbt run --profiles-dir . --target dev

dbt-test:
	@export $$(grep -v '^#' .env | xargs) && \
	cd dbt && ../.venv/bin/dbt test --profiles-dir . --target dev

# ── Housekeeping ──────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name "*.pyc" -delete 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null; true
	rm -rf dbt/target dbt/dbt_packages
