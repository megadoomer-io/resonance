.PHONY: run test lint format typecheck check clean

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
