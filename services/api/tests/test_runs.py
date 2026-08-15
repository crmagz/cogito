"""Tests for the non-executing legacy inventory endpoint."""

from fastapi.testclient import TestClient


def test_direct_plan_submission_cannot_start_execution(client: TestClient, valid_plan: dict, starter) -> None:
    response = client.post("/api/v1/runs", json={"plan": valid_plan})

    assert response.status_code == 202
    assert response.json()["status"] == "planning_required"
    assert starter.started_runs == []
