.PHONY: run test lint format typecheck check clean diagrams \
	dev dev-up dev-down dev-reset dev-migrate

run:
	uv run uvicorn resonance.app:create_app --factory --reload

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src/

check: lint typecheck test

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache dist

diagrams: ## Render dot diagrams to SVG
	@find docs/diagrams -name '*.dot' -exec sh -c 'dot -Tsvg "$$1" -o "$${1%.dot}.svg"' _ {} \;
	@echo "Rendered $$(find docs/diagrams -name '*.svg' | wc -l | tr -d ' ') diagrams"

# Local development environment
dev-up:
	docker-compose up -d
	@echo "Waiting for PostgreSQL..."
	@until docker-compose exec postgres pg_isready -U resonance > /dev/null 2>&1; do sleep 1; done
	@echo "PostgreSQL and Redis are ready."

dev-down:
	docker-compose down

dev-reset:
	docker-compose down -v
	@echo "All data volumes removed."

dev-migrate:
	uv run alembic upgrade head

dev: dev-up dev-migrate run
