FROM python:3.14-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.29 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /build

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src

RUN uv sync --frozen --no-dev --no-editable

FROM python:3.14-slim AS runtime

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --system --gid 10001 cogito \
    && useradd --system --uid 10001 --gid cogito --create-home cogito

WORKDIR /app

COPY --from=builder --chown=cogito:cogito /opt/venv /opt/venv

USER cogito

EXPOSE 8000

CMD ["uvicorn", "cogito_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
