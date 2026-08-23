from __future__ import annotations

import asyncio
import json

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError

from cogito_api.main import create_app
from cogito_api.models import (
    AiPlan,
    ModelTier,
    PlanPhase,
    PlanConstraints,
    ProductSpecification,
    WorkflowConfigurationState,
    WorkflowGateDecisionRequest,
    WorkflowPhaseDefinition,
    WorkflowTemplate,
)
from cogito_api.specification_evaluation import validate_requirement_assignments
from cogito_api.workflow_control import InMemoryWorkflowConfigurationStore, default_policy, default_template

from .conftest import make_settings
from .fakes import FakePlanner, FakeRunStarter, InMemoryPlanStore, InMemorySupervisorStore


def _intake() -> dict:
    return {
        "objective": "Give API operators a bounded rate limit.",
        "actors": ["API operator"],
        "desired_outcomes": ["Requests are safely rate limited."],
        "scope_in": ["API gateway rate limiting"],
        "scope_out": ["Authentication changes"],
        "acceptance_expectations": ["Over-limit requests have a clear response."],
        "constraints": ["Keep metrics observable."],
        "unknowns": [],
    }


def _binding() -> dict:
    return {
        "project_id": "default",
        "template_ref": "software_delivery@1.0.0",
        "target_repos": ["https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"],
        "spec_set": "typescript-backend@v2.1#sha256=" + "a" * 64,
        "constraints": {
            "max_wall_clock_minutes": 50,
            "max_cost_usd": 50,
            "max_review_rounds": 3,
            "max_turns_per_phase": 500,
            "backup_reserve_turns": 25,
        },
    }


def _app(*, roles: tuple[str, ...], valid_plan: dict, valid_product_specification: dict):
    store = InMemoryPlanStore()
    app = create_app(
        store=store,
        settings=make_settings(auth_static_roles=roles),
        starter=FakeRunStarter(),
        supervisor_store=InMemorySupervisorStore(),
        planner=FakePlanner(AiPlan.model_validate(valid_plan), ProductSpecification.model_validate(valid_product_specification)),
        workflow_configuration_store=InMemoryWorkflowConfigurationStore(),
    )
    app.state.test_plan_store = store
    return app


def test_template_requires_default_policy_and_all_human_gates() -> None:
    with pytest.raises(ValidationError, match="default_policy_ref"):
        WorkflowTemplate.model_validate(
            {
                "id": "invalid",
                "version": "1.0.0",
                "default_policy_ref": "",
                "phases": [
                    {"id": "phase", "kind": "phase", "agent_role": "planner", "permitted_tiers": ["balanced"]}
                ],
                "required_gates": [],
            }
        )


def test_requirement_relationships_allow_support_reuse_but_one_owner(valid_product_specification: dict) -> None:
    from cogito_api.models import ProductSpecification

    specification = ProductSpecification.model_validate(valid_product_specification)
    phases = [
        PlanPhase.model_validate(
            {
                "id": "build", "name": "Build", "description": "Build", "tasks": ["build"],
                "acceptance_criteria": ["done"], "verification": ["test"],
                "requirement_assignments": [
                    {"requirement_id": "functional-1", "relationship": "owns"},
                    {"requirement_id": "functional-2", "relationship": "supports"},
                ],
            }
        ),
        PlanPhase.model_validate(
            {
                "id": "verify", "name": "Verify", "description": "Verify", "tasks": ["verify"],
                "acceptance_criteria": ["verified"], "verification": ["test"],
                "requirement_assignments": [
                    {"requirement_id": "functional-1", "relationship": "verifies", "acceptance_criterion_ids": ["acceptance-1"]},
                    {"requirement_id": "functional-2", "relationship": "owns"},
                ],
            }
        ),
    ]
    validate_requirement_assignments(specification, phases)


def test_product_manager_can_submit_only_structured_intake_after_platform_binding(
    valid_plan: dict, valid_product_specification: dict
) -> None:
    app = _app(
        roles=("cogito-product-manager", "cogito-policy-editor", "cogito-policy-publisher", "cogito-viewer"),
        valid_plan=valid_plan,
        valid_product_specification=valid_product_specification,
    )
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as client:
        binding = client.put("/api/v1/project-workflow-bindings/default", json=_binding())
        assert binding.status_code == 200
        response = client.post("/api/v1/projects/default/workflow-runs", json={"specification": _intake()})
        assert response.status_code == 202
        body = response.json()
        assert body["status"] == "planning"
        assert body["source_artifact"]["ref"].endswith("/source-spec.json")
        source = json.loads(app.state.test_plan_store.source_specifications[body["run_id"]])
        assert source["schema_version"] == "cogito.initial-specification/v1"
        assert source["goal"] == _intake()["objective"]
        assert source["repositories"] == [
            {"ref": _binding()["target_repos"][0], "role": "primary_target"}
        ]
        assert source["workflow_context"] == {
            "project_id": "default",
            "template_ref": "software_delivery@1.0.0",
            "policy_ref": "platform_standard@1.0.0",
            "required_gates": [
                "product_specification_review",
                "plan_scope_review",
                "delivery_review",
            ],
        }
        assert source["product_manager_intake"] == _intake() | {
            "schema_version": 1,
            "repository_candidates": [],
            "discovery_preference": "supplied_first",
        }


def test_product_manager_cannot_modify_project_binding(valid_plan: dict, valid_product_specification: dict) -> None:
    app = _app(
        roles=("cogito-product-manager",), valid_plan=valid_plan, valid_product_specification=valid_product_specification
    )
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as client:
        response = client.put("/api/v1/project-workflow-bindings/default", json=_binding())
        assert response.status_code == 403


def test_configuration_versions_have_an_audited_draft_to_publish_lifecycle() -> None:
    store = InMemoryWorkflowConfigurationStore()
    constraints = PlanConstraints()
    policy = default_policy("default", constraints).model_copy(update={"id": "platform_custom", "version": "1.0.0"})
    template = default_template().model_copy(
        update={"id": "delivery_custom", "version": "1.0.0", "default_policy_ref": "platform_custom@1.0.0"}
    )

    asyncio.run(store.create_policy_draft(policy, actor="policy-editor"))
    asyncio.run(store.transition_policy("platform_custom@1.0.0", WorkflowConfigurationState.VALIDATED, actor="policy-editor"))
    asyncio.run(store.transition_policy("platform_custom@1.0.0", WorkflowConfigurationState.PUBLISHED, actor="policy-publisher"))
    asyncio.run(store.create_template_draft(template, actor="policy-editor"))
    asyncio.run(store.transition_template("delivery_custom@1.0.0", WorkflowConfigurationState.VALIDATED, actor="policy-editor"))
    asyncio.run(store.transition_template("delivery_custom@1.0.0", WorkflowConfigurationState.PUBLISHED, actor="policy-publisher"))

    assert asyncio.run(store.get_policy("platform_custom@1.0.0")) == policy
    assert asyncio.run(store.get_template("delivery_custom@1.0.0")) == template
    assert [event[3].value for event in store.lifecycle_events] == ["validated", "published", "validated", "published"]


def test_platform_can_publish_a_policy_only_after_validation(
    valid_plan: dict, valid_product_specification: dict
) -> None:
    app = _app(
        roles=("cogito-policy-editor", "cogito-policy-publisher"),
        valid_plan=valid_plan,
        valid_product_specification=valid_product_specification,
    )
    policy = default_policy("default", PlanConstraints()).model_copy(update={"id": "staged_policy", "version": "1.0.0"})
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as client:
        assert client.post("/api/v1/workflow-policies/drafts", json=policy.model_dump(mode="json")).status_code == 201
        blocked = client.post("/api/v1/workflow-policies/staged_policy@1.0.0/publish")
        assert blocked.status_code == 409
        assert client.post("/api/v1/workflow-policies/staged_policy@1.0.0/validate").json()["state"] == "validated"
        assert client.post("/api/v1/workflow-policies/staged_policy@1.0.0/publish").json()["state"] == "published"


def test_schema_gate_decision_requires_a_rationale_for_non_approvals() -> None:
    with pytest.raises(ValidationError, match="comment is required"):
        WorkflowGateDecisionRequest(decision="request_revision", artifact_sha256="a" * 64)

    request = WorkflowGateDecisionRequest(
        decision="approve", artifact_sha256="a" * 64, artifact_revision=1
    )
    assert request.artifact_revision == 1


def test_resolved_plan_gate_routes_to_the_durable_plan_approval_adapter(
    valid_plan: dict, valid_product_specification: dict
) -> None:
    app = _app(
        roles=(
            "cogito-product-manager", "cogito-policy-editor", "cogito-policy-publisher", "cogito-viewer",
            "cogito-workflow-approver", "cogito-approver",
        ),
        valid_plan=valid_plan,
        valid_product_specification=valid_product_specification,
    )
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as client:
        template = default_template().model_copy(
            update={
                "id": "single_operator_delivery",
                "version": "1.0.0",
                "required_gates": [
                    gate.model_copy(update={"separation_of_duties": False}) if gate.id == "plan_scope_review" else gate
                    for gate in default_template().required_gates
                ],
            }
        )
        assert client.post("/api/v1/workflow-templates", json=template.model_dump(mode="json")).status_code == 201
        binding = _binding() | {"template_ref": "single_operator_delivery@1.0.0"}
        assert client.put("/api/v1/project-workflow-bindings/default", json=binding).status_code == 200
        run_id = client.post("/api/v1/projects/default/workflow-runs", json={"specification": _intake()}).json()["run_id"]
        draft = client.post(f"/api/v1/planning-runs/{run_id}/generate-product-specification").json()
        accepted = client.post(
            f"/api/v1/planning-runs/{run_id}/gates/product_specification_review",
            json={
                "decision": "approve",
                "artifact_revision": draft["product_specification_revision"],
                "artifact_sha256": draft["product_specification_artifact"]["sha256"],
            },
            headers={"Idempotency-Key": "accept-product-gate"},
        )
        assert accepted.status_code == 200
        planned = client.post(f"/api/v1/planning-runs/{run_id}/generate-plan")
        assert planned.status_code == 200, planned.text
        decision = client.post(
            f"/api/v1/planning-runs/{run_id}/gates/plan_scope_review",
            json={"decision": "approve", "artifact_sha256": planned.json()["plan_artifact"]["sha256"]},
            headers={"Idempotency-Key": "schema-gate-plan-approval"},
        )
        assert decision.status_code == 202
        assert decision.json()["gate_id"] == "plan_scope_review"
