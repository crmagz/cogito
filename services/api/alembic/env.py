"""Alembic environment for Cogito supervisor schema migrations."""

from __future__ import annotations

import os
from logging.config import fileConfig
from urllib.parse import quote

from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _database_url() -> str:
    host = os.environ.get("COGITO_SUPERVISOR_DATABASE_HOST", "cogito-postgresql")
    port = os.environ.get("COGITO_SUPERVISOR_DATABASE_PORT", "5432")
    name = os.environ.get("COGITO_SUPERVISOR_DATABASE_NAME", "cogito")
    user = quote(os.environ.get("COGITO_SUPERVISOR_DATABASE_USER", "postgres"), safe="")
    password = quote(os.environ.get("COGITO_SUPERVISOR_DATABASE_PASSWORD", "cogito"), safe="")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


def run_migrations_offline() -> None:
    """Run migrations without connecting to the database."""

    context.configure(url=_database_url(), literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against the configured supervisor database."""

    from sqlalchemy import create_engine

    connectable = create_engine(_database_url(), pool_pre_ping=True)
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
