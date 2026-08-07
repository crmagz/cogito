from __future__ import annotations

import json

import httpx
import pytest

from cogito_api.models import AgentGatewayResolution, AiPlan
from cogito_api.planner import LiteLLMPlanner, PlannerError, PlanningContext

from .conftest import make_settings


def planner_gateway(**overrides: object) -> AgentGatewayResolution:
    values = {
        "policy_revision": "agent_gateway_initial",
        "project_id": "default",
        "role": "planner",
        "registration_id": "planner",
        "registration_version": "1.0.0",
        "manifest_sha256": "a" * 64,
        "model_alias": "balanced",
        "max_budget_usd": 5.0,
        "toolset": "planning-readonly",
    }
    values.update(overrides)
    return AgentGatewayResolution(**values)


async def test_litellm_planner_requests_json_with_dedicated_bearer_key(valid_plan: dict) -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps(valid_plan)}}]},
        )

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert plan == AiPlan.model_validate(valid_plan)
    assert captured["authorization"] == "Bearer planner-test-key"
    assert captured["body"]["model"] == "balanced"  # type: ignore[index]
    assert captured["body"]["response_format"] == {"type": "json_object"}  # type: ignore[index]
    assert '"title"' in captured["body"]["messages"][0]["content"]  # type: ignore[index]


async def test_litellm_planner_rejects_model_output_that_changes_target_repositories(valid_plan: dict) -> None:
    changed = dict(valid_plan)
    changed["target_repos"] = ["https://github.com/acme/other.git#0123456789abcdef0123456789abcdef01234567"]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(changed)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(PlannerError, match="target_repos"):
        await planner.generate(
            PlanningContext(
                initial_specification="Add a rate limiter.",
                target_repos=valid_plan["target_repos"],
                spec_set=valid_plan["spec_set"],
                constraints=AiPlan.model_validate(valid_plan).constraints,
            ),
            planner_gateway(),
        )


async def test_litellm_planner_accepts_a_single_fenced_json_object(valid_plan: dict) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": f"```json\n{json.dumps(valid_plan)}\n```"}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert plan.title == valid_plan["title"]


async def test_litellm_planner_rejects_a_route_that_exceeds_its_configured_role_key(valid_plan: dict) -> None:
    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(lambda _: httpx.Response(500)))

    with pytest.raises(PlannerError, match="gateway route"):
        await planner.generate(
            PlanningContext(
                initial_specification="Add a rate limiter.",
                target_repos=valid_plan["target_repos"],
                spec_set=valid_plan["spec_set"],
                constraints=AiPlan.model_validate(valid_plan).constraints,
            ),
            planner_gateway(model_alias="complex"),
        )


def test_ai_plan_rejects_undeclared_output_fields(valid_plan: dict) -> None:
    invalid = {**valid_plan, "untrusted_execution_mode": "bypass"}

    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        AiPlan.model_validate(invalid)
