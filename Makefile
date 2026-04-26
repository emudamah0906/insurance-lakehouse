.PHONY: up down down-volumes test seed dag-trigger lint format clean setup-venv logs status

# ── Docker lifecycle ──────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo ""
	@echo "  Services starting (allow ~60s for Airflow init to complete)"
	@echo "  MinIO Console : http://localhost:9001   user=minioadmin  pass=minioadmin123"
	@echo "  Airflow UI    : http://localhost:8080   user=admin       pass=admin"
	@echo ""

down:
	docker compose down

down-volumes:
	@echo "WARNING: this deletes all MinIO data, Postgres data, and Airflow logs."
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

# ── Data pipeline shortcuts ───────────────────────────────────────────────────

seed:
	.venv/bin/python data_generator/generate.py

dag-trigger:
	@echo "Usage: make dag-trigger DAG=01_generate_and_land"
	docker exec airflow-webserver airflow dags trigger $(DAG)

# ── Housekeeping ──────────────────────────────────────────────────────────────

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -type f -name "*.pyc" -delete 2>/dev/null; true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null; true
