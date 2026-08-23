from __future__ import annotations

from fastapi.testclient import TestClient
import pytest
from pydantic import ValidationError

from cogito_api.main import create_app
from cogito_api.models import (
    AiPlan,
    ModelTier,
    PlanPhase,
    WorkflowPhaseDefinition,
    WorkflowTemplate,
)
from cogito_api.specification_evaluation import validate_requirement_assignments
from cogito_api.workflow_control import InMemoryWorkflowConfigurationStore

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
    return create_app(
        store=InMemoryPlanStore(),
        settings=make_settings(auth_static_roles=roles),
        starter=FakeRunStarter(),
        supervisor_store=InMemorySupervisorStore(),
        planner=FakePlanner(AiPlan.model_validate(valid_plan), valid_product_specification),
        workflow_configuration_store=InMemoryWorkflowConfigurationStore(),
    )


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


def test_product_manager_cannot_modify_project_binding(valid_plan: dict, valid_product_specification: dict) -> None:
    app = _app(
        roles=("cogito-product-manager",), valid_plan=valid_plan, valid_product_specification=valid_product_specification
    )
    with TestClient(app, headers={"Authorization": "Bearer operator-test-token"}) as client:
        response = client.put("/api/v1/project-workflow-bindings/default", json=_binding())
        assert response.status_code == 403
