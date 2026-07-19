from __future__ import annotations

import json

import httpx
import pytest

from cogito_api.models import AiPlan
from cogito_api.planner import LiteLLMPlanner, PlannerError, PlanningContext

from .conftest import make_settings


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
        )
    )

    assert plan == AiPlan.model_validate(valid_plan)
    assert captured["authorization"] == "Bearer planner-test-key"
    assert captured["body"]["model"] == "balanced"  # type: ignore[index]
    assert captured["body"]["response_format"] == {"type": "json_object"}  # type: ignore[index]


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
            )
        )
