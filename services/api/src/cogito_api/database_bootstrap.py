"""Create the non-destructive supervisor database when a local cluster lacks it."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.errors import InvalidCatalogName

from .config import Settings, load_settings

_DATABASE_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def ensure_supervisor_database(settings: Settings) -> None:
    """Create the configured database only when it does not already exist."""

    if not _DATABASE_NAME.fullmatch(settings.supervisor_database_name):
        raise ValueError("COGITO_SUPERVISOR_DATABASE_NAME must be a valid PostgreSQL identifier")
    try:
        with psycopg.connect(settings.supervisor_database_sync_url):
            return
    except InvalidCatalogName:
        pass

    target = urlsplit(settings.supervisor_database_sync_url)
    admin_url = urlunsplit((target.scheme, target.netloc, "/postgres", target.query, target.fragment))
    with psycopg.connect(admin_url, autocommit=True) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(settings.supervisor_database_name))
            )


def main() -> None:
    """Run the Helm migration hook's database bootstrap step."""

    ensure_supervisor_database(load_settings())


if __name__ == "__main__":
    main()
