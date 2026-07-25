from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

from cogito_worker.run_state import PostgresRunStateReporter


@dataclass
class _Result:
    previous_status: str

    def mappings(self) -> _Result:
        return self

    def one_or_none(self) -> dict[str, str]:
        return {"status": self.previous_status}


class _Connection:
    def __init__(self, previous_status: str = "QUEUED") -> None:
        self._previous_status = previous_status
        self.calls: list[tuple[str, dict]] = []

    async def execute(self, statement, parameters: dict) -> _Result:
        self.calls.append((str(statement), parameters))
        return _Result(self._previous_status)


class _Engine:
    def __init__(self, connection: _Connection) -> None:
        self._connection = connection

    @asynccontextmanager
    async def begin(self):
        yield self._connection


async def test_status_report_accepts_a_missing_failure_detail_without_an_untyped_sql_parameter() -> (
    None
):
    connection = _Connection()
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "claimed", None, None)

    update_statement, update_parameters = connection.calls[1]
    assert "COALESCE(CAST(:error_summary AS text), error_summary)" in update_statement
    assert update_parameters["error_summary"] is None


async def test_successful_backup_stop_is_a_terminal_timed_out_lifecycle() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "stopped_with_backup", None, {"ceiling": "cost"})

    _, update_parameters = connection.calls[1]
    _, event_parameters = connection.calls[2]
    assert update_parameters["status"] == "TIMED_OUT"
    assert update_parameters["terminal"] is True
    assert event_parameters["to_status"] == "TIMED_OUT"


async def test_review_escalation_is_a_terminal_successful_lifecycle() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "escalated", None, {"review": {"status": "escalated"}})

    _, update_parameters = connection.calls[1]
    assert update_parameters["status"] == "SUCCEEDED"
    assert update_parameters["terminal"] is True
