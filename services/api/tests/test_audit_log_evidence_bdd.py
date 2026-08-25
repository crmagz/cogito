"""Operator-visible audit-log evidence behavior expressed in Gherkin."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, scenarios, then, when

from cogito_api.audit_logs import AuditLogLine, AuditLogPage
from cogito_api.main import create_app

from .conftest import make_settings
from .fakes import FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .test_approvals import _awaiting_plan

scenarios("features/audit_log_evidence.feature")


class RecordedAuditLogReader:
    """A deterministic bounded reader that proves the API authorizes the event first."""

    def __init__(self) -> None:
        self.invocations: list[str] = []
        self.occurred_at: list[str | None] = []

    async def read_invocation(
        self, invocation_id: str, cursor: str | None = None, occurred_at: str | None = None
    ) -> AuditLogPage:
        assert cursor is None
        self.invocations.append(invocation_id)
        self.occurred_at.append(occurred_at)
        return AuditLogPage(
            availability="available",
            lines=[
                AuditLogLine(
                    timestamp="2026-08-23T00:00:00+00:00",
                    stream="cogito-executions/execution-pod/execution",
                    message=f"{invocation_id} stdout token=[REDACTED]",
                )
            ],
        )


@pytest.fixture
def audit_context() -> dict[str, object]:
    """Share the persisted event IDs and HTTP responses between Gherkin steps."""

    return {}


@pytest.fixture
def audit_log_reader() -> RecordedAuditLogReader:
    return RecordedAuditLogReader()


@pytest.fixture
def audit_client(
    store: InMemoryPlanStore,
    starter: FakeRunStarter,
    supervisor_store: InMemorySupervisorStore,
    planner,
    audit_log_reader: RecordedAuditLogReader,
) -> TestClient:
    app = create_app(
        store=store,
        settings=make_settings(audit_logs_enabled=True, audit_logs_loki_endpoint="http://loki.test"),
        starter=starter,
        supervisor_store=supervisor_store,
        planner=planner,
        audit_log_reader=audit_log_reader,
    )
    return TestClient(app, headers={"Authorization": "Bearer operator-test-token"})


@given("a planning run has a logged stage invocation")
def planning_run_has_logged_stage_invocation(
    audit_client: TestClient,
    audit_context: dict[str, object],
    supervisor_store: InMemorySupervisorStore,
    valid_plan: dict,
) -> None:
    run_id, _ = _awaiting_plan(audit_client, valid_plan)
    invocation_id = "a" * 64
    supervisor_store._append_coordination_event(
        run_id,
        "stage_invocation_started",
        invocation={
            "invocation_id": invocation_id,
            "source": "worker_phase",
            "stage_id": "phase-1",
            "role": "developer",
            "attempt": 1,
            "trace_context_available": True,
        },
    )
    supervisor_store._append_coordination_event(
        run_id,
        "mcp_invocation_observed",
        mcp_invocation={
            "invocation_id": "b" * 64,
            "server_id": "readonly",
            "server_version": "1.0.0",
            "server_manifest_sha256": "c" * 64,
            "tool_name": "catalog_read",
            "input_schema_sha256": "d" * 64,
            "outcome": "success",
            "invocation_count": 1,
        },
    )
    stage_event_id = next(
        event.event_id for event in supervisor_store.coordination_events.values()
        if event.run_id == run_id and event.event_type == "stage_invocation_started"
    )
    non_log_event_id = next(
        event.event_id for event in supervisor_store.coordination_events.values()
        if event.run_id == run_id and event.event_type == "mcp_invocation_observed"
    )
    audit_context.update(
        run_id=run_id,
        invocation_id=invocation_id,
        stage_event_id=stage_event_id,
        non_log_event_id=non_log_event_id,
    )


@when("the authorized operator opens that audit event's output")
def authorized_operator_opens_audit_output(audit_client: TestClient, audit_context: dict[str, object]) -> None:
    audit_context["response"] = audit_client.get(
        f"/api/v1/workbench/runs/{audit_context['run_id']}/timeline/{audit_context['stage_event_id']}/logs"
    )
    audit_context["non_log_response"] = audit_client.get(
        f"/api/v1/workbench/runs/{audit_context['run_id']}/timeline/{audit_context['non_log_event_id']}/logs"
    )


@then("the operator receives only the bounded redacted output for that invocation")
def operator_receives_bounded_redacted_output(
    audit_context: dict[str, object], audit_log_reader: RecordedAuditLogReader
) -> None:
    response = audit_context["response"]
    assert response.status_code == 200
    assert response.json() == {
        "availability": "available",
        "lines": [{
            "timestamp": "2026-08-23T00:00:00+00:00",
            "stream": "cogito-executions/execution-pod/execution",
            "message": f"{audit_context['invocation_id']} stdout token=[REDACTED]",
        }],
        "next_cursor": None,
    }
    assert audit_log_reader.invocations == [audit_context["invocation_id"]]
    assert len(audit_log_reader.occurred_at) == 1
    assert isinstance(audit_log_reader.occurred_at[0], str)


@then("another audit event does not expose a raw output stream")
def unrelated_event_has_no_raw_output(audit_context: dict[str, object]) -> None:
    response = audit_context["non_log_response"]
    assert response.status_code == 200
    assert response.json() == {"availability": "not_available", "lines": [], "next_cursor": None}
