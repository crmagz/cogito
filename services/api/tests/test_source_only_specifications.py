"""Regression coverage for source-only specification drafting."""

from fastapi.testclient import TestClient

from .fakes import InMemorySupervisorStore


def test_product_manager_creates_a_persisted_source_only_draft_without_a_planning_run(
    client: TestClient, supervisor_store: InMemorySupervisorStore
) -> None:
    """A draft must have no planning, agent, or acceptance lifecycle to advance."""

    response = client.post(
        "/api/v1/projects/default/source-only-specifications",
        json={"initial_specification": "Expose source-only drafts without starting planning."},
    )

    assert response.status_code == 201
    draft = response.json()
    specification_id = draft["specification_id"]
    assert draft["status"] == "source_recorded"
    assert "/source-only-specifications/" in draft["source_artifact"]["ref"]
    assert draft["product_specification_artifact"] is None
    assert specification_id in supervisor_store.source_only_specifications
    assert specification_id not in supervisor_store.planning_runs
    assert specification_id not in supervisor_store.agent_runs

    assert (
        client.post(
            f"/api/v1/planning-runs/{specification_id}/accept-product-specification",
            json={
                "revision": 1,
                "artifact_sha256": "a" * 64,
            },
            headers={"Idempotency-Key": "source-only-must-not-be-accepted"},
        ).status_code
        == 404
    )


def test_product_manager_can_retrieve_only_source_only_drafts_for_its_project(
    client: TestClient,
) -> None:
    """The persisted draft has a read path independent of Workbench run lists."""

    created = client.post(
        "/api/v1/projects/default/source-only-specifications",
        json={"initial_specification": "Draft a product requirement from source only."},
    )
    assert created.status_code == 201
    draft = created.json()

    listed = client.get("/api/v1/projects/default/source-only-specifications")
    fetched = client.get(f"/api/v1/projects/default/source-only-specifications/{draft['specification_id']}")

    assert listed.status_code == 200
    assert [item["specification_id"] for item in listed.json()] == [draft["specification_id"]]
    assert fetched.status_code == 200
    assert fetched.json() == draft
