FROM python:3.12-slim AS builder

ENV VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

RUN python -m venv "$VIRTUAL_ENV"

COPY pyproject.toml ./
COPY src ./src

RUN pip install --no-cache-dir --no-compile .

FROM python:3.12-slim AS runtime

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

USER cogito

CMD ["python", "-m", "cogito_worker.worker"]
