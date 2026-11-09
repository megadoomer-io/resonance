.PHONY: run test lint format typecheck check clean diagrams \
	dev dev-up dev-down dev-reset dev-migrate dev-app dev-stop-app dev-env dev-status

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

# Local development environment. With ENV=<name>, targets act on a named
# environment instead (own compose project, ports, volume and app worktrees);
# see docs/dev-environments.md.
DEV_ENV := scripts/dev-env.sh

# ENV, REF, TO, NAME and PORT count only when given on the make command line:
# ENV and PORT are common shell variables, and an exported one must not turn
# `make dev-reset` into something else.
cli_var = $(if $(filter command line,$(origin $(1))),$(strip $($(1))))
ifeq ($(origin ENV),command line)
ifeq ($(strip $(ENV)),)
$(error ENV is empty; pass ENV=<name> or leave it out for the default stack)
endif
endif
D_ENV := $(call cli_var,ENV)
D_REF := $(call cli_var,REF)
D_TO := $(or $(call cli_var,TO),head)
D_NAME := $(call cli_var,NAME)
D_PORT := $(call cli_var,PORT)

dev-up:
ifneq ($(D_ENV),)
	@$(DEV_ENV) up $(D_ENV)
else
	docker-compose up -d
	@echo "Waiting for PostgreSQL..."
	@until docker-compose exec postgres pg_isready -U resonance > /dev/null 2>&1; do sleep 1; done
	@echo "PostgreSQL and Redis are ready."
endif

dev-down:
ifneq ($(D_ENV),)
	@$(DEV_ENV) down $(D_ENV)
else
	docker-compose down
endif

dev-reset:
ifneq ($(D_ENV),)
	@$(DEV_ENV) down $(D_ENV)
else
	docker-compose down -v
	@echo "All data volumes removed."
endif

dev-migrate:
ifneq ($(D_ENV),)
	@$(DEV_ENV) migrate $(D_ENV) "$(D_REF)" "$(D_TO)"
else
	uv run alembic upgrade $(D_TO)
endif

dev-app: _require-env
	@$(DEV_ENV) app $(D_ENV) "$(D_REF)" "$(or $(D_NAME),app)" "$(D_PORT)"

dev-stop-app: _require-env
	@$(DEV_ENV) stop-app $(D_ENV) "$(D_NAME)"

dev-env: _require-env
	@$(DEV_ENV) env $(D_ENV) "$(D_NAME)"

dev-status:
	@$(DEV_ENV) status

_require-env:
ifeq ($(D_ENV),)
	$(error ENV=<name> is required)
endif

dev: dev-up dev-migrate run
