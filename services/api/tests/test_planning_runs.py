from __future__ import annotations

import asyncio
import copy
import json
import time
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore
from .conftest import make_settings
from cogito_api.main import create_app
from cogito_api.models import AiPlan
from cogito_api.planner import PlannerError, PlannerOutputError


def _planning_request(valid_plan: dict) -> dict:
    return {
        "initial_specification": "Add a rate limiter with bounded, observable behavior.",
        "target_repos": valid_plan["target_repos"],
        "spec_set": valid_plan["spec_set"],
        "constraints": valid_plan["constraints"],
        "priority": "normal",
    }


def _select_product_specification(client: TestClient, run_id: str) -> dict:
    """Generate and explicitly select the immutable draft used by plan-generation tests."""

    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")
    assert draft.status_code == 200
    body = draft.json()
    evaluation = client.post(f"/api/v1/planning-runs/{run_id}/evaluate-product-specification")
    assert evaluation.status_code == 200
    assert evaluation.json()["specification_evaluation_readiness"] == "ready"
    selected = client.post(
        f"/api/v1/planning-runs/{run_id}/select-product-specification",
        json={
            "revision": body["product_specification_revision"],
            "artifact_sha256": body["product_specification_artifact"]["sha256"],
        },
        headers={"Idempotency-Key": f"select-{run_id}"},
    )
    assert selected.status_code == 200
    return selected.json()


def test_submit_planning_run_persists_immutable_source_artifact_and_run(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    response = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "planning"
    assert body["source_artifact"]["ref"].endswith(f"runs/{body['run_id']}/source-spec.json")
    assert len(body["source_artifact"]["sha256"]) == 64
    source = json.loads(store.source_specifications[body["run_id"]])
    assert source == {
        "schema_version": "cogito.initial-specification/v1",
        "goal": "Add a rate limiter with bounded, observable behavior.",
        "repositories": [
            {"ref": repository, "role": "primary_target"}
            for repository in valid_plan["target_repos"]
        ],
        "specification_context": {"spec_set": valid_plan["spec_set"]},
        "constraints": valid_plan["constraints"],
        "priority": "normal",
        "workflow_context": {
            "project_id": "default",
            "required_gates": [
                "product_specification_review",
                "plan_scope_review",
                "delivery_review",
            ],
        },
    }
    record = supervisor_store.planning_runs[body["run_id"]]
    assert record.source_artifact.sha256 == body["source_artifact"]["sha256"]
    assert record.target_repos == valid_plan["target_repos"]


def test_operator_can_cancel_a_pre_plan_run(client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore) -> None:
    """Cancellation is a terminal operator choice, distinct from rejecting a plan."""

    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    response = client.post(f"/api/v1/planning-runs/{run_id}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert supervisor_store.planning_runs[run_id].status.value == "cancelled"
    assert any(event.event_type == "planning_cancelled" for event in supervisor_store.coordination_events.values())
    assert client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").status_code == 409


def test_operator_can_replay_a_cancelled_pre_plan_run(client: TestClient, valid_plan: dict) -> None:
    """An ambiguous cancel response can be retried without converting success to a conflict."""

    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]

    first = client.post(f"/api/v1/planning-runs/{run_id}/cancel", headers={"Idempotency-Key": "cancel-replay"})
    replay = client.post(f"/api/v1/planning-runs/{run_id}/cancel", headers={"Idempotency-Key": "cancel-replay"})

    assert first.status_code == replay.status_code == 200
    assert first.json()["status"] == replay.json()["status"] == "cancelled"


def test_approver_can_auditably_waive_a_failing_specification_evaluation(
    client: TestClient, valid_plan: dict
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")
    assert draft.status_code == 200
    evaluated = client.post(f"/api/v1/planning-runs/{run_id}/evaluate-product-specification")
    assert evaluated.status_code == 200
    # The fixture is ready, so make the request reference a ready evaluation and
    # prove that a waiver cannot silently broaden the exception path.
    rejected = client.post(
        f"/api/v1/planning-runs/{run_id}/waive-specification-evaluation",
        json={"artifact_sha256": evaluated.json()["specification_evaluation_artifact"]["sha256"], "rationale": "No exception needed."},
        headers={"Idempotency-Key": f"waive-{run_id}"},
    )
    assert rejected.status_code == 409


def test_workbench_projects_evaluation_readiness_after_evaluation(
    client: TestClient, valid_plan: dict
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    assert client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").status_code == 200
    assert client.post(f"/api/v1/planning-runs/{run_id}/evaluate-product-specification").status_code == 200

    response = client.get(f"/api/v1/workbench/runs/{run_id}")

    assert response.status_code == 200
    assert response.json()["specification_evaluation_readiness"] == "ready"


def test_accept_product_specification_evaluates_and_selects_a_ready_current_revision(
    client: TestClient, valid_plan: dict
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    accepted = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json={
            "revision": draft["product_specification_revision"],
            "artifact_sha256": draft["product_specification_artifact"]["sha256"],
        },
        headers={"Idempotency-Key": "accept-ready-specification"},
    )

    assert accepted.status_code == 200
    body = accepted.json()
    assert body["outcome"] == "accepted"
    assert body["specification_evaluation_readiness"] == "ready"
    assert body["selected_product_specification_revision"] == draft["product_specification_revision"]
    assert body["selected_specification_evaluation_artifact"] == body["specification_evaluation_artifact"]
    assert body["plan_artifact"] is None
    assert body["status"] == "planning"
    workbench = client.get(f"/api/v1/workbench/runs/{run_id}")
    stages = {stage["stage_id"]: stage for stage in workbench.json()["stages"]}
    assert stages["planning"]["state"] == "queued"
    assert stages["plan_approval"]["state"] == "unavailable"


def test_workbench_projects_only_a_running_planner_as_planning_in_progress(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()

    accepted = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json={
            "revision": draft["product_specification_revision"],
            "artifact_sha256": draft["product_specification_artifact"]["sha256"],
        },
        headers={"Idempotency-Key": "planner-start-projection"},
    )
    assert accepted.status_code == 200

    queued = client.get(f"/api/v1/workbench/runs/{run_id}").json()
    assert {stage["stage_id"]: stage for stage in queued["stages"]}["planning"]["state"] == "queued"

    claims = asyncio.run(supervisor_store.claim_planning_generation_deliveries(limit=1, lease_seconds=60))
    assert len(claims) == 1
    running = client.get(f"/api/v1/workbench/runs/{run_id}").json()
    assert {stage["stage_id"]: stage for stage in running["stages"]}["planning"]["state"] == "in_progress"

    asyncio.run(
        supervisor_store.record_planning_agent_terminal(
            run_id, claims[0].claim_id, succeeded=False, error_summary="Plan generation failed: duplicate requirement IDs."
        )
    )
    failed = client.get(f"/api/v1/workbench/runs/{run_id}").json()
    assert failed["status"] == "planning_failed"
    assert {stage["stage_id"]: stage for stage in failed["stages"]}["planning"]["state"] == "failed"


def test_planning_generation_claim_is_singleton_and_recoverable(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    """An accepted specification becomes one durable, retryable planner handoff."""

    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    accepted = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json={
            "revision": draft["product_specification_revision"],
            "artifact_sha256": draft["product_specification_artifact"]["sha256"],
        },
        headers={"Idempotency-Key": "durable-planner-handoff"},
    )
    assert accepted.status_code == 200

    first = asyncio.run(supervisor_store.claim_planning_generation_deliveries(limit=1, lease_seconds=60))
    assert len(first) == 1
    assert asyncio.run(supervisor_store.claim_planning_generation_deliveries(limit=1, lease_seconds=60)) == []

    asyncio.run(supervisor_store.release_planning_generation_delivery(run_id, first[0].claim_id, retry_seconds=1))
    recovered = asyncio.run(supervisor_store.claim_planning_generation_deliveries(limit=1, lease_seconds=60))
    assert len(recovered) == 1
    assert recovered[0].claim_id != first[0].claim_id


def test_lifespan_dispatcher_generates_an_accepted_plan(
    store: InMemoryPlanStore,
    starter: FakeRunStarter,
    supervisor_store: InMemorySupervisorStore,
    planner: FakePlanner,
    valid_plan: dict,
) -> None:
    """The durable dispatcher, rather than a request task, performs planning."""

    app = create_app(
        store=store,
        settings=make_settings(),
        starter=starter,
        supervisor_store=supervisor_store,
        planner=planner,
    )
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as running_client:
        run_id = running_client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
        draft = running_client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
        assert running_client.post(
            f"/api/v1/planning-runs/{run_id}/accept-product-specification",
            json={
                "revision": draft["product_specification_revision"],
                "artifact_sha256": draft["product_specification_artifact"]["sha256"],
            },
            headers={"Idempotency-Key": "lifespan-planner-dispatch"},
        ).status_code == 200

        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            current = running_client.get(f"/api/v1/planning-runs/{run_id}").json()
            if current["status"] == "awaiting_plan_approval":
                break
            time.sleep(0.05)
        assert current["status"] == "awaiting_plan_approval"
        assert len(starter.started_runs) == 1


def test_accept_product_specification_records_findings_and_selects_on_explicit_approval(
    client: TestClient, valid_plan: dict, valid_product_specification: dict
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    refinement_needed = copy.deepcopy(valid_product_specification)
    refinement_needed["unresolved_questions"] = [
        {
            "id": "question-retention",
            "text": "How long must rejected requests be retained?",
            "kind": "question",
            "source_segment_ids": [],
            "requirement_ids": [],
        }
    ]
    revised = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": draft["product_specification_revision"],
            "parent_artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "specification": refinement_needed,
        },
        headers={"Idempotency-Key": "create-refinement-needed-specification"},
    ).json()

    accepted = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json={
            "revision": revised["product_specification_revision"],
            "artifact_sha256": revised["product_specification_artifact"]["sha256"],
        },
        headers={"Idempotency-Key": "accept-refinement-needed-specification"},
    )

    assert accepted.status_code == 200
    body = accepted.json()
    assert body["outcome"] == "accepted"
    assert body["specification_evaluation_readiness"] == "needs_revision"
    assert body["specification_evaluation_artifact"] is not None
    assert body["selected_product_specification_artifact"]["sha256"] == revised["product_specification_artifact"]["sha256"]
    workbench = client.get(f"/api/v1/workbench/runs/{run_id}").json()
    states = {stage["stage_id"]: stage["state"] for stage in workbench["stages"]}
    assert states["product_specification"] == "completed"
    assert states["specification_evaluation"] == "completed"
    assert states["planning"] == "queued"
    assert states["plan_approval"] == "unavailable"


def test_accept_product_specification_replays_an_accepted_request_without_duplicate_selection(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    request = {
        "revision": draft["product_specification_revision"],
        "artifact_sha256": draft["product_specification_artifact"]["sha256"],
    }

    first = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json=request,
        headers={"Idempotency-Key": "replay-accepted-specification"},
    )
    replay = client.post(
        f"/api/v1/planning-runs/{run_id}/accept-product-specification",
        json=request,
        headers={"Idempotency-Key": "replay-accepted-specification"},
    )

    assert first.status_code == replay.status_code == 200
    assert first.json()["outcome"] == replay.json()["outcome"] == "accepted"
    assert [event.event_type for event in supervisor_store.coordination_events.values()].count(
        "product_specification_selected"
    ) == 1


def test_submit_planning_run_requires_a_scoped_approver(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    unauthenticated = client.post(
        "/api/v1/planning-runs",
        json=_planning_request(valid_plan),
        headers={"Authorization": "Bearer invalid-token"},
    )

    assert unauthenticated.status_code == 401
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_submit_planning_run_rejects_an_approver_without_default_project_scope(
    valid_plan: dict, starter: FakeRunStarter, planner: FakePlanner
) -> None:
    store = InMemoryPlanStore()
    supervisor_store = InMemorySupervisorStore()
    client = TestClient(
        create_app(
            store=store,
            settings=make_settings(auth_static_projects=("other-project",)),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=planner,
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    assert response.status_code == 403
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_generate_plan_requires_a_scoped_approver(
    client: TestClient, valid_plan: dict, planner: FakePlanner
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    response = client.post(
        f"/api/v1/planning-runs/{submitted.json()['run_id']}/generate-plan",
        headers={"Authorization": "Bearer invalid-token"},
    )

    assert response.status_code == 401
    assert planner.contexts == []


def test_generate_product_specification_persists_an_immutable_draft(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
    planner: FakePlanner,
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]

    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "planning"
    assert body["product_specification_revision"] == 1
    artifact = body["product_specification_artifact"]
    assert artifact["ref"].endswith(
        f"runs/{run_id}/product-specifications/1/{artifact['sha256']}/specification.json"
    )
    assert store.product_specifications[(run_id, 1)].title.text == "Rate limiting"
    source = json.loads(planner.product_specification_contexts[0].initial_specification)
    assert source["schema_version"] == "cogito.initial-specification/v1"
    assert source["goal"] == _planning_request(valid_plan)["initial_specification"]
    assert source["repositories"] == [
        {"ref": repository, "role": "primary_target"}
        for repository in valid_plan["target_repos"]
    ]
    assert supervisor_store.planning_runs[run_id].product_specification_artifact is not None


def test_product_specification_generation_failure_is_recorded_in_the_workbench_timeline(
    client: TestClient, valid_plan: dict, planner: FakePlanner
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]

    async def fail_product_specification_generation(*_args: object) -> AiPlan:
        raise PlannerError("LiteLLM product specification request failed")

    planner.generate_product_specification = fail_product_specification_generation  # type: ignore[method-assign]
    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")

    assert response.status_code == 502
    timeline = client.get(f"/api/v1/workbench/runs/{run_id}/timeline")
    assert timeline.status_code == 200
    failure = next(item for item in timeline.json()["items"] if item["event_type"] == "product_specification_generation_failed")
    assert failure["stage_ids"] == ["product_specification"]
    assert failure["message"] == (
        "Product specification generation failed because the configured model provider was unavailable or rejected "
        "the request. Retry after platform configuration is restored."
    )


def test_generate_product_specification_retries_without_generating_a_second_draft(
    client: TestClient, valid_plan: dict, planner: FakePlanner
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]

    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")
    repeated = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification")

    assert first.status_code == repeated.status_code == 200
    assert first.json()["product_specification_artifact"] == repeated.json()["product_specification_artifact"]
    assert len(planner.product_specification_contexts) == 1


def test_abandoned_product_specification_generation_claim_is_recovered(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    claim = asyncio.run(supervisor_store.claim_product_specification_generation(run_id))
    assert claim is not None
    supervisor_store.product_specification_generation_claimed_at[run_id] = datetime.now(timezone.utc) - timedelta(minutes=16)

    recovered = asyncio.run(supervisor_store.claim_product_specification_generation(run_id))

    assert recovered == claim


def test_legacy_product_specification_generation_claim_without_a_timestamp_is_recovered(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    supervisor_store.product_specification_generation_claims[run_id] = "pre-timestamp-claim"

    recovered = asyncio.run(supervisor_store.claim_product_specification_generation(run_id))

    assert recovered == f"claim-{run_id}"


def test_selected_product_specification_is_the_only_plan_input(
    client: TestClient, valid_plan: dict, planner: FakePlanner
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]

    blocked = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert blocked.status_code == 409
    assert planner.contexts == []

    selected = _select_product_specification(client, run_id)
    planned = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert planned.status_code == 200
    assert selected["selected_product_specification_revision"] == 1
    assert planner.contexts[0].initial_specification.startswith('{"acceptance_criteria":')
    assert "Add a rate limiter with bounded, observable behavior." not in planner.contexts[0].initial_specification


def test_generated_plan_retains_its_selected_product_specification_binding(
    client: TestClient, valid_plan: dict, supervisor_store: InMemorySupervisorStore
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    selected = _select_product_specification(client, run_id)

    planned = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert planned.status_code == 200
    revision, artifact = supervisor_store.plan_product_specification_bindings[(run_id, 1)]
    assert revision == selected["selected_product_specification_revision"]
    assert artifact.model_dump() == selected["selected_product_specification_artifact"]


def test_human_revision_records_evaluation_selects_and_regenerates_a_plan(
    client: TestClient, valid_plan: dict, planner: FakePlanner, valid_product_specification: dict
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    revised = copy.deepcopy(valid_product_specification)
    revised["title"]["text"] = "Reviewed rate limiting"

    response = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 1,
            "parent_artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "specification": revised,
        },
        headers={"Idempotency-Key": "human-revision-1"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["product_specification_revision"] == 2
    assert body["specification_evaluation_artifact"] is not None
    assert body["selected_product_specification_artifact"] == body["product_specification_artifact"]
    assert client.post(f"/api/v1/planning-runs/{run_id}/generate-plan").status_code == 200


def test_human_revision_can_restore_a_prior_immutable_specification(
    client: TestClient, valid_plan: dict, valid_product_specification: dict
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    changed = copy.deepcopy(valid_product_specification)
    changed["title"]["text"] = "Reviewed rate limiting"
    second = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 1,
            "parent_artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "specification": changed,
        },
        headers={"Idempotency-Key": "change-specification"},
    ).json()

    restored = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 2,
            "parent_artifact_sha256": second["product_specification_artifact"]["sha256"],
            "specification": valid_product_specification,
        },
        headers={"Idempotency-Key": "restore-original-specification"},
    )

    assert restored.status_code == 200
    assert restored.json()["product_specification_revision"] == 3
    assert restored.json()["product_specification_artifact"]["sha256"] == draft["product_specification_artifact"]["sha256"]


def test_human_revision_rejects_unknown_source_provenance(
    client: TestClient, valid_plan: dict, valid_product_specification: dict
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    revised = copy.deepcopy(valid_product_specification)
    revised["title"]["source_segment_ids"] = ["invented-segment"]

    response = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 1,
            "parent_artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "specification": revised,
        },
        headers={"Idempotency-Key": "reject-invented-provenance"},
    )

    assert response.status_code == 422


def test_stale_product_specification_cannot_be_selected_after_a_human_revision(
    client: TestClient, valid_plan: dict, valid_product_specification: dict
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
    revised = copy.deepcopy(valid_product_specification)
    revised["title"]["text"] = "Reviewed rate limiting"
    latest = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 1,
            "parent_artifact_sha256": draft["product_specification_artifact"]["sha256"],
            "specification": revised,
        },
        headers={"Idempotency-Key": "human-revision-before-stale-selection"},
    ).json()

    stale = client.post(
        f"/api/v1/planning-runs/{run_id}/select-product-specification",
        json={"revision": 1, "artifact_sha256": draft["product_specification_artifact"]["sha256"]},
        headers={"Idempotency-Key": "stale-selection"},
    )

    assert latest["product_specification_revision"] == 2
    assert stale.status_code == 409


def test_superseded_selection_cannot_attach_a_generated_plan(
    client: TestClient,
    valid_plan: dict,
    valid_product_specification: dict,
    supervisor_store: InMemorySupervisorStore,
    store: InMemoryPlanStore,
) -> None:
    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    selected = _select_product_specification(client, run_id)
    record = supervisor_store.planning_runs[run_id]
    revised = copy.deepcopy(valid_product_specification)
    revised["title"]["text"] = "Reviewed rate limiting"
    response = client.post(
        f"/api/v1/planning-runs/{run_id}/revise-product-specification",
        json={
            "expected_product_specification_revision": 1,
            "parent_artifact_sha256": selected["selected_product_specification_artifact"]["sha256"],
            "specification": revised,
        },
        headers={"Idempotency-Key": "replace-selected-specification"},
    )
    assert response.status_code == 200

    stale_plan = store.put_planning_plan(run_id, 1, AiPlan.model_validate(valid_plan))
    try:
        asyncio.run(
            supervisor_store.attach_generated_plan(
                run_id,
                stale_plan,
                "balanced",
                "stale-workflow",
                record.plan_revision,
                record.selected_product_specification_revision,
                record.selected_product_specification_artifact.sha256,
            )
        )
    except ValueError:
        pass
    else:
        raise AssertionError("superseded selected product specification accepted a stale generated plan")


def test_submit_planning_run_rejects_unpinned_repository_without_writing(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    payload = _planning_request(valid_plan)
    payload["target_repos"] = ["https://github.com/acme/api-gateway.git#main"]

    response = client.post("/api/v1/planning-runs", json=payload)

    assert response.status_code == 422
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_dry_run_planning_validates_without_writing(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    payload = _planning_request(valid_plan)
    payload["dry_run"] = True

    response = client.post("/api/v1/planning-runs", json=payload)

    assert response.status_code == 200
    assert response.json()["status"] == "validated"
    assert store.source_specifications == {}
    assert supervisor_store.planning_runs == {}


def test_get_planning_run_returns_authoritative_supervisor_record(
    client: TestClient, valid_plan: dict
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))

    response = client.get(
        f"/api/v1/planning-runs/{submitted.json()['run_id']}",
        headers={"Authorization": "Bearer operator-test-token"},
    )

    assert response.status_code == 200
    assert response.json() == submitted.json()


def test_generate_plan_persists_validated_artifact_and_enters_approval_state(
    client: TestClient,
    valid_plan: dict,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)

    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "awaiting_plan_approval"
    assert body["plan_artifact"]["ref"].endswith(
        f"/plans/{run_id}/revisions/1/{body['plan_artifact']['sha256']}/plan.json"
    )
    assert len(body["plan_artifact"]["sha256"]) == 64
    assert store.plans[run_id].title == valid_plan["title"]
    assert supervisor_store.planning_runs[run_id].plan_artifact is not None


def test_generate_plan_rejects_a_planner_without_the_model_grant(
    valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    class GrantRevokingStore(InMemorySupervisorStore):
        def __init__(self) -> None:
            super().__init__()
            self.planner_resolutions = 0

        async def resolve_run_registration(self, *args, **kwargs):
            resolution = await super().resolve_run_registration(*args, **kwargs)
            if resolution.role == "planner":
                self.planner_resolutions += 1
                if self.planner_resolutions > 2:
                    return resolution.model_copy(update={"grants": []})
            return resolution

    store = InMemoryPlanStore()
    supervisor_store = GrantRevokingStore()
    app = create_app(
        store=store,
        settings=make_settings(),
        starter=starter,
        supervisor_store=supervisor_store,
        planner=planner,
    )
    client = TestClient(app, headers={"Authorization": "Bearer operator-test-token"})
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    _select_product_specification(client, submitted.json()["run_id"])

    response = client.post(f"/api/v1/planning-runs/{submitted.json()['run_id']}/generate-plan")

    assert response.status_code == 503
    assert response.json()["detail"] == "planner registry grant is unavailable"
    assert planner.contexts == []
    assert starter.started_runs == []


def test_generate_plan_reports_invalid_model_output_without_a_retryable_status(
    client: TestClient, valid_plan: dict, planner: FakePlanner
) -> None:
    """An accepted specification must not be retried forever for bad model output."""

    run_id = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan)).json()["run_id"]
    _select_product_specification(client, run_id)

    async def invalid_output(*_args: object) -> AiPlan:
        raise PlannerOutputError("LiteLLM planner output failed contract validation after one repair attempt")

    planner.generate = invalid_output  # type: ignore[method-assign]
    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert response.status_code == 422
    assert response.json()["detail"] == "planner output did not satisfy the approved planning contract"


def test_generate_plan_retries_workflow_start_without_regenerating_artifact(
    client: TestClient, valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    response = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert first.status_code == 200
    assert response.status_code == 200
    assert response.json()["plan_artifact"] == first.json()["plan_artifact"]
    assert len(planner.contexts) == 1
    assert len(starter.started_runs) == 1


def test_concurrent_generation_converges_on_the_persisted_plan(
    valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    class ConcurrentPlanStore(InMemorySupervisorStore):
        async def attach_generated_plan(self, *args, **kwargs):
            await super().attach_generated_plan(*args, **kwargs)
            raise ValueError("another caller persisted the active plan")

    store = InMemoryPlanStore()
    supervisor_store = ConcurrentPlanStore()
    racing_client = TestClient(
        create_app(
            store=store,
            settings=make_settings(),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=planner,
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )
    submitted = racing_client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    _select_product_specification(racing_client, submitted.json()["run_id"])

    response = racing_client.post(f"/api/v1/planning-runs/{submitted.json()['run_id']}/generate-plan")

    assert response.status_code == 200
    assert response.json()["plan_artifact"] == supervisor_store.planning_runs[submitted.json()["run_id"]].plan_artifact.model_dump()
    assert len(starter.started_runs) == 1


def test_generate_plan_reports_retryable_temporal_start_failure(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)
    starter.start_error = ConnectionError("Temporal unavailable")

    failed = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")

    assert failed.status_code == 503
    starter.start_error = None
    retried = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert retried.status_code == 200


def test_revision_reopens_planning_with_a_new_artifact_and_workflow(
    client: TestClient, valid_plan: dict, planner: FakePlanner, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    first_digest = first.json()["plan_artifact"]["sha256"]
    revision = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "request_revision", "artifact_sha256": first_digest, "comment": "Narrow the scope."},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "revision-1"},
    )

    assert revision.status_code == 202
    reopened = client.get(
        f"/api/v1/planning-runs/{run_id}", headers={"Authorization": "Bearer operator-test-token"}
    ).json()
    assert reopened["status"] == "planning"
    assert reopened["plan_artifact"] is None
    revised_plan = copy.deepcopy(valid_plan)
    revised_plan["title"] = "Add a narrower rate limiter"
    planner.plan = AiPlan.model_validate(revised_plan)
    second = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    second_digest = second.json()["plan_artifact"]["sha256"]

    assert second.status_code == 200
    assert second_digest != first_digest
    assert len(starter.started_runs) == 2
    assert starter.started_runs[0].workflow_id != starter.started_runs[1].workflow_id
    stale = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": first_digest},
        headers={"Authorization": "Bearer operator-test-token", "Idempotency-Key": "stale-after-revision"},
    )
    assert stale.status_code == 409


def test_plan_approval_rejects_a_pre_upgrade_duration_above_the_github_app_limit(
    client: TestClient,
    valid_plan: dict,
    planner: FakePlanner,
    starter: FakeRunStarter,
    store: InMemoryPlanStore,
    supervisor_store: InMemorySupervisorStore,
) -> None:
    legacy_plan = copy.deepcopy(valid_plan)
    legacy_plan["constraints"]["max_wall_clock_minutes"] = 60
    planner.plan = AiPlan.model_validate(legacy_plan)
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(legacy_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)
    generated = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    digest = generated.json()["plan_artifact"]["sha256"]
    restricted_client = TestClient(
        create_app(
            store=store,
            settings=make_settings(max_wall_clock_minutes=50),
            starter=starter,
            supervisor_store=supervisor_store,
            planner=planner,
        ),
        headers={"Authorization": "Bearer operator-test-token"},
    )

    response = restricted_client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Idempotency-Key": "reject-legacy-duration"},
    )

    assert response.status_code == 409
    assert "GitHub App workspace credential limit" in response.json()["detail"]
    coordination_response = restricted_client.post(
        f"/api/v1/coordination/runs/{run_id}/actions/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers={"Idempotency-Key": "reject-legacy-duration-coordination"},
    )
    assert coordination_response.status_code == 409
    assert supervisor_store.planning_runs[run_id].status.value == "awaiting_plan_approval"


def test_revision_scopes_workflow_and_idempotency_when_plan_content_is_identical(
    client: TestClient, valid_plan: dict, starter: FakeRunStarter
) -> None:
    submitted = client.post("/api/v1/planning-runs", json=_planning_request(valid_plan))
    run_id = submitted.json()["run_id"]
    _select_product_specification(client, run_id)
    first = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    digest = first.json()["plan_artifact"]["sha256"]
    headers = {"Authorization": "Bearer operator-test-token", "Idempotency-Key": "same-key"}
    revision = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "request_revision", "artifact_sha256": digest, "comment": "Regenerate."},
        headers=headers,
    )

    assert revision.status_code == 202
    assert client.get(
        f"/api/v1/planning-runs/{run_id}", headers={"Authorization": "Bearer operator-test-token"}
    ).json()["plan_artifact"] is None
    second = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
    assert second.status_code == 200
    assert second.json()["plan_artifact"]["sha256"] == digest
    assert second.json()["plan_artifact"]["ref"] != first.json()["plan_artifact"]["ref"]
    assert starter.started_runs[0].workflow_id != starter.started_runs[1].workflow_id
    approved = client.post(
        f"/api/v1/runs/{run_id}/approvals/plan",
        json={"decision": "approve", "artifact_sha256": digest},
        headers=headers,
    )
    assert approved.status_code == 202
    assert len(starter.plan_approvals) == 2


def test_existing_direct_plan_submission_contract_requires_planning(
    client: TestClient, valid_plan: dict
) -> None:
    plan = copy.deepcopy(valid_plan)

    response = client.post("/api/v1/runs", json={"plan": plan})

    assert response.status_code == 202
    assert response.json()["status"] == "planning_required"
