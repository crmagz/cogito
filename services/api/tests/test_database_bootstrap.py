from __future__ import annotations

import pytest

from cogito_api.database_bootstrap import ensure_supervisor_database

from .conftest import make_settings


class _Cursor:
    def __init__(self, database_exists: bool) -> None:
        self.executed = False
        self.database_exists = database_exists

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, *_: object) -> None:
        self.executed = True

    def fetchone(self) -> tuple[int] | None:
        return (1,) if self.database_exists else None


class _Connection:
    def __init__(self, database_exists: bool) -> None:
        self.cursor_instance = _Cursor(database_exists)

    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def cursor(self) -> _Cursor:
        return self.cursor_instance


def test_existing_supervisor_database_is_not_created(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def connect(url: str, **_: object) -> _Connection:
        calls.append(url)
        return _Connection(database_exists=True)

    monkeypatch.setattr("cogito_api.database_bootstrap.psycopg.connect", connect)

    ensure_supervisor_database(make_settings())

    assert len(calls) == 1
    assert calls[0].endswith("/postgres")


def test_missing_supervisor_database_is_created_from_postgres_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    admin_connection = _Connection(database_exists=False)

    def connect(url: str, **_: object) -> _Connection:
        calls.append(url)
        return admin_connection

    monkeypatch.setattr("cogito_api.database_bootstrap.psycopg.connect", connect)

    ensure_supervisor_database(make_settings())

    assert len(calls) == 1
    assert calls[0].endswith("/postgres")
    assert admin_connection.cursor_instance.executed is True


def test_invalid_database_name_is_rejected_before_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    connect = lambda *_args, **_kwargs: pytest.fail("must not connect")
    monkeypatch.setattr("cogito_api.database_bootstrap.psycopg.connect", connect)

    with pytest.raises(ValueError, match="valid PostgreSQL identifier"):
        ensure_supervisor_database(make_settings(supervisor_database_name="cogito; DROP DATABASE postgres"))
