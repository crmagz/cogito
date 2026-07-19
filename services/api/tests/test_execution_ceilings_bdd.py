"""Product-facing ceiling validation expressed in Gherkin."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pytest_bdd import given, scenarios, then, when

from .test_planning_runs import _planning_request

scenarios("features/execution_ceilings.feature")


@pytest.fixture
def ceiling_context() -> dict[str, object]:
    """Share the request and response through one BDD scenario."""

    return {}


@given("a valid planning request")
def valid_planning_request(ceiling_context: dict[str, object], valid_plan: dict) -> None:
    """Start from an otherwise accepted planning payload."""

    ceiling_context["payload"] = _planning_request(valid_plan)


@when("the reserve consumes every allowed agent turn")
def reserve_consumes_every_turn(ceiling_context: dict[str, object], client: TestClient) -> None:
    """Submit an invalid hard-turn budget through the public API."""

    payload = ceiling_context["payload"]
    assert isinstance(payload, dict)
    payload["constraints"]["max_turns_per_phase"] = payload["constraints"]["backup_reserve_turns"]
    ceiling_context["response"] = client.post("/api/v1/planning-runs", json=payload)


@then("the planning request is rejected before it is persisted")
def rejected_before_persist(ceiling_context: dict[str, object]) -> None:
    """A reserve must not erase all productive work capacity."""

    response = ceiling_context["response"]
    assert response.status_code == 422
