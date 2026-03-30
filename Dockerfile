FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app

# Install dependencies first (cache-friendly layer ordering)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Copy source, alembic config, and install the project
COPY src/ src/
COPY alembic.ini ./
COPY alembic/ alembic/
RUN uv sync --frozen --no-dev

FROM python:3.14-slim-bookworm

WORKDIR /app

# Copy the virtual environment, source, and alembic config from builder
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/src /app/src
COPY --from=builder /app/alembic.ini /app/alembic.ini
COPY --from=builder /app/alembic /app/alembic
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
USER nobody

CMD ["uvicorn", "resonance.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
