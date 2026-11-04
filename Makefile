# Mini-Lakehouse developer & ops entrypoints.
# `make up` starts the full core stack + demo producer (design doc success criteria).

COMPOSE := docker compose -f deploy/compose/docker-compose.yml
PYTEST  := python -m pytest

.DEFAULT_GOAL := help
.PHONY: help up up-extras down logs ps install test test-unit test-chaos \
        test-integration lint typecheck fmt bench demo clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

up:  ## Start the core stack (broker, storage, catalog, services) + demo producer
	$(COMPOSE) --profile core up -d --build
	@echo "Platform API: http://localhost:8000/docs  |  MinIO: http://localhost:9001"

up-extras:  ## Start core + extras (Trino, Prometheus, Grafana, Redpanda Console)
	$(COMPOSE) --profile core --profile extras up -d --build

down:  ## Stop the stack and remove volumes
	$(COMPOSE) --profile core --profile extras down -v

logs:  ## Tail service logs
	$(COMPOSE) logs -f --tail=100

ps:  ## Show running services
	$(COMPOSE) ps

install:  ## Install the package with dev + adapter + api extras
	pip install -e ".[dev,adapters,api]"

test:  ## Run unit + chaos tests with coverage (domain/ports gate at 80%)
	$(PYTEST) -m "unit or chaos" --cov=mini_lakehouse.domain --cov=mini_lakehouse.ports

test-unit:  ## Run only the fast unit tests
	$(PYTEST) -m unit

test-chaos:  ## Run the exactly-once chaos tests
	$(PYTEST) -m chaos

test-integration:  ## Run Testcontainers integration tests (needs Docker)
	$(PYTEST) -m integration

lint:  ## Ruff lint
	ruff check src tests

fmt:  ## Ruff format
	ruff format src tests

typecheck:  ## mypy (strict on domain + ports)
	mypy

bench:  ## Run the load benchmark (requires the stack to be up)
	python -m mini_lakehouse.gateway  # ML_EVENTS_PER_SEC controls rate

demo:  ## Open the demo notebook
	jupyter notebook notebooks/demo.ipynb

clean:  ## Remove caches
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
