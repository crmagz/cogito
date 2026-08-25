from __future__ import annotations

import json
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
    assert "SET status = 'implementation_failed'" in supervisor_statement
    assert supervisor_parameters["run_id"] == "run-1"


async def test_approved_run_persists_a_failure_reason_when_execution_setup_fails() -> None:
    connection = _Connection(previous_status="WAITING_FOR_APPROVAL")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "failed", "MCP repository scope does not match its pinned release", None)

    _, agent_update = connection.calls[1]
    assert agent_update["status"] == "FAILED"
    assert agent_update["error_summary"] == "MCP repository scope does not match its pinned release"


async def test_duplicate_failed_status_hydrates_an_empty_failure_reason_without_a_second_lifecycle_event() -> None:
    connection = _Connection(previous_status="FAILED")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.report("run-1", "failed", "phase one failed verification", None)

    hydration_statement, hydration_parameters = connection.calls[1]
    assert "SET error_summary = COALESCE(error_summary" in hydration_statement
    assert hydration_parameters == {"run_id": "run-1", "error_summary": "phase one failed verification"}
    assert len(connection.calls) == 2


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


async def test_stage_invocation_event_is_idempotent_and_correlation_only() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.record_stage_invocation("run-1", "implement-api", "developer", 2, True)

    statement, parameters = connection.calls[0]
    assert "INSERT INTO coordination_events" in statement
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in statement
    assert parameters["dedupe_key"] != parameters["event_id"]
    assert '"event_type":"stage_invocation_started"' in parameters["payload"]
    assert '"attempt":2' in parameters["payload"]
    assert json.loads(parameters["payload"])["activity"] == {
        "kind": "agent",
        "actor_label": "Developer",
        "log_evidence_available": True,
    }
    assert "notification_outbox" not in statement


async def test_mcp_invocation_event_persists_only_the_safe_aggregate() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.record_mcp_invocation_evidence(
        "run-1",
        {
            "status": "observed",
            "events": [
                {
                    "server_id": "readonly",
                    "server_version": "1.0.0",
                    "server_manifest_sha256": "b" * 64,
                    "tool_name": "catalog_read",
                    "input_schema_sha256": "c" * 64,
                    "outcome": "success",
                    "invocation_count": 2,
                    "request_body": "must not be persisted",
                }
            ],
        },
    )

    statement, parameters = connection.calls[0]
    assert "INSERT INTO coordination_events" in statement
    assert "ON CONFLICT (dedupe_key) DO NOTHING" in statement
    assert '"event_type":"mcp_invocation_observed"' in parameters["payload"]
    assert '"invocation_count":2' in parameters["payload"]
    assert json.loads(parameters["payload"])["activity"] == {
        "kind": "mcp",
        "actor_label": "readonly / catalog_read",
        "log_evidence_available": False,
    }
    assert "request_body" not in parameters["payload"]


async def test_mcp_invocation_events_keep_each_observed_tool_as_the_actor() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.record_mcp_invocation_evidence(
        "run-1",
        {
            "status": "observed",
            "events": [
                {
                    "server_id": "catalog",
                    "server_version": "1.0.0",
                    "server_manifest_sha256": "b" * 64,
                    "tool_name": "catalog_read",
                    "input_schema_sha256": "c" * 64,
                    "outcome": "success",
                    "invocation_count": 1,
                },
                {
                    "server_id": "github",
                    "server_version": "1.0.0",
                    "server_manifest_sha256": "d" * 64,
                    "tool_name": "pull_request_read",
                    "input_schema_sha256": "e" * 64,
                    "outcome": "success",
                    "invocation_count": 1,
                },
            ],
        },
    )

    assert [json.loads(parameters["payload"])["activity"]["actor_label"] for _, parameters in connection.calls] == [
        "catalog / catalog_read",
        "github / pull_request_read",
    ]


async def test_execution_workspace_lifecycle_hides_the_job_name() -> None:
    connection = _Connection(previous_status="RUNNING")
    reporter = object.__new__(PostgresRunStateReporter)
    reporter._engine = _Engine(connection)  # type: ignore[assignment]

    await reporter.record_execution_workspace_lifecycle("run-1", "cogito-execution-sensitive-name", "provisioned")

    statement, parameters = connection.calls[0]
    assert "INSERT INTO coordination_events" in statement
    assert '"event_type":"execution_workspace_lifecycle"' in parameters["payload"]
    assert "cogito-execution-sensitive-name" not in parameters["payload"]
