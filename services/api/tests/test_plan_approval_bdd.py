"""Operator-visible plan-approval behavior expressed in Gherkin."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, scenarios, then, when

from .fakes import FakeRunStarter, InMemorySupervisorStore
from .test_planning_runs import _planning_request

scenarios("features/plan_approval.feature")


@pytest.fixture
def approval_context() -> dict[str, object]:
    """Share one planning run and HTTP response between scenario steps."""

    return {}


@given("a generated plan is awaiting approval")
def generated_plan_is_awaiting_approval(
    approval_context: dict[str, object], client: TestClient, valid_plan: dict
) -> None:
    """Create a source artifact and its normalized plan through the public API."""

    submission = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submission.json()["run_id"]
    planning = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    approval_context["run_id"] = run_id
    approval_context["digest"] = planning.json()["plan_artifact"]["sha256"]


@when("the authorized operator approves the current plan")
def approve_current_plan(approval_context: dict[str, object], client: TestClient) -> None:
    """Submit the exact digest with the local test operator credential."""

    approval_context["response"] = client.post(
        f"/api/v1/runs/{approval_context['run_id']}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": approval_context["digest"]},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "bdd-approval"},
    )


@when("the authorized operator rejects the current plan with a reason")
def reject_current_plan(approval_context: dict[str, object], client: TestClient) -> None:
    """Submit a rejection with durable reviewer context."""

    approval_context["response"] = client.post(
        f"/api/v1/runs/{approval_context['run_id']}/approvals/plan",
        json={"decision": "reject", "artifact_sha256": approval_context["digest"], "comment": "Needs a narrower scope."},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "bdd-rejection"},
    )


@when("the authorized operator approves a stale plan digest")
def approve_stale_plan(approval_context: dict[str, object], client: TestClient) -> None:
    """Attempt to approve an artifact that is not the current generated plan."""

    approval_context["response"] = client.post(
        f"/api/v1/runs/{approval_context['run_id']}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": "0" * 64},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "bdd-stale"},
    )


@then("the approval is delivered exactly once to the workflow")
def approval_is_delivered_once(
    approval_context: dict[str, object], starter: FakeRunStarter
) -> None:
    """The operator response and durable delivery use one immutable decision ID."""

    response = approval_context["response"]
    assert response.status_code == 202
    assert response.json()["delivered"] is True
    assert len(starter.plan_approvals) == 1


@then("the run enters implementing state")
def run_enters_implementing_state(
    approval_context: dict[str, object], supervisor_store: InMemorySupervisorStore
) -> None:
    """Approval advances the authoritative Supervisor state projection."""

    assert supervisor_store.planning_runs[approval_context["run_id"]].status.value == "implementing"


@then("the run enters rejected state")
def run_enters_rejected_state(
    approval_context: dict[str, object], supervisor_store: InMemorySupervisorStore
) -> None:
    """A rejection remains terminal and cannot provision a workspace."""

    assert supervisor_store.planning_runs[approval_context["run_id"]].status.value == "rejected"


@then("the approval request is rejected as a conflict")
def approval_request_is_conflict(approval_context: dict[str, object], starter: FakeRunStarter) -> None:
    """The stale digest fails before a Temporal update is delivered."""

    response = approval_context["response"]
    assert response.status_code == 409
    assert starter.plan_approvals == []
