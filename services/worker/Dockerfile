ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/crmagz/cogito
ARG CLAUDE_CODE_VERSION=2.1.215

FROM node:22.14.0-bookworm-slim AS claude-code

ARG CLAUDE_CODE_VERSION

RUN npm install --global --no-audit --no-fund "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && npm cache clean --force

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

ARG VERSION
ARG REVISION
ARG SOURCE
ARG CLAUDE_CODE_VERSION

LABEL org.opencontainers.image.title="Cogito Worker" \
      org.opencontainers.image.description="Cogito Temporal workflow worker" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}"

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install --no-install-recommends --yes ca-certificates git \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 cogito \
    && useradd --system --uid 10001 --gid cogito --create-home cogito

WORKDIR /app

COPY --from=builder --chown=cogito:cogito /opt/venv /opt/venv
COPY --from=claude-code /usr/local/bin/node /usr/local/bin/node
COPY --from=claude-code /usr/local/bin/claude /usr/local/bin/claude
COPY --from=claude-code /usr/local/lib/node_modules /usr/local/lib/node_modules

USER cogito

CMD ["python", "-m", "cogito_worker.worker"]
