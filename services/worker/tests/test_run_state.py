from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass

from cogito_worker.run_state import PostgresRunStateReporter


@dataclass
class _Result:
    previous_status: str
    implementation_revision: int = 1

    def mappings(self) -> _Result:
        return self

    def one_or_none(self) -> dict[str, str | int]:
        return {"status": self.previous_status, "implementation_revision": self.implementation_revision}


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
    coordination_statement, coordination_parameters = connection.calls[3]
    _, event_parameters = connection.calls[5]
    assert update_parameters["status"] == "TIMED_OUT"
    assert update_parameters["terminal"] is True
    assert "INSERT INTO coordination_events" in coordination_statement
    assert '"lifecycle_status":"TIMED_OUT"' in coordination_parameters["payload"]
    assert event_parameters["to_status"] == "TIMED_OUT"


async def test_review_escalation_is_a_terminal_successful_lifecycle() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "escalated", None, {"review": {"status": "escalated"}})

    _, update_parameters = connection.calls[1]
    assert update_parameters["status"] == "SUCCEEDED"
    assert update_parameters["terminal"] is True


async def test_failed_workflow_closes_the_supervisor_projection() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "failed", "execution unavailable", None)

    supervisor_statement, supervisor_parameters = connection.calls[2]
    assert "SET status = 'planning_failed'" in supervisor_statement
    assert supervisor_parameters["run_id"] == "run-1"


async def test_running_run_can_enter_implementation_approval_and_register_artifact() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report(
        "run-1",
        "awaiting_implementation_approval",
        None,
        {"implementation_artifact": {"ref": "s3://plans/implementation.json", "sha256": "a" * 64}},
    )

    _, agent_update = connection.calls[1]
    artifact_statement, artifact_update = connection.calls[2]
    assert agent_update["status"] == "WAITING_FOR_APPROVAL"
    assert "SET status = 'awaiting_implementation_approval'" in artifact_statement
    assert artifact_update["sha256"] == "a" * 64
    gate_statement, gate_parameters = connection.calls[4]
    lifecycle_statement, lifecycle_parameters = connection.calls[6]
    assert "implementation_approval_requested" in gate_statement
    assert '"sha256":"' + "a" * 64 + '"' in gate_parameters["payload"]
    assert "run_status_changed" in lifecycle_statement
    assert '"lifecycle_status":"WAITING_FOR_APPROVAL"' in lifecycle_parameters["payload"]


async def test_coordination_event_dedupe_keys_are_deterministic() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report(
        "run-1",
        "awaiting_implementation_approval",
        None,
        {"implementation_artifact": {"ref": "s3://plans/implementation.json", "sha256": "a" * 64}},
    )

    gate_statement, gate_parameters = connection.calls[4]
    lifecycle_statement, lifecycle_parameters = connection.calls[6]
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in gate_statement
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in lifecycle_statement
    assert gate_parameters["dedupe_key"] != gate_parameters["event_id"]
    assert lifecycle_parameters["dedupe_key"] != lifecycle_parameters["event_id"]
