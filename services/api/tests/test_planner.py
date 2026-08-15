from __future__ import annotations

import json

import httpx
import pytest

from cogito_api.models import AgentGatewayResolution, AiPlan, ProductSpecification
from cogito_api.planner import LiteLLMPlanner, PlannerError, PlanningContext, ProductSpecificationContext

from .conftest import make_settings


def planner_gateway(**overrides: object) -> AgentGatewayResolution:
    values = {
        "policy_revision": "agent_gateway_initial",
        "project_id": "default",
        "role": "planner",
        "registration_id": "planner",
        "registration_version": "1.1.0",
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


def valid_product_specification() -> dict:
    """Return a source-grounded product specification fixture for planner contract tests."""

    def source(statement_id: str, text: str, requirement_ids: list[str] | None = None) -> dict:
        return {"id": statement_id, "text": text, "kind": "source", "source_segment_ids": ["source-1"], "requirement_ids": requirement_ids or []}

    return {
        "schema_version": 2,
        "title": source("title", "Rate limiting"),
        "problem_statement": source("problem", "The API needs bounded request rates."),
        "desired_outcomes": [source("outcome-1", "Protect API endpoints from abuse.")],
        "actors": [source("actor-1", "API consumers")],
        "in_scope": [source("scope-in-1", "Rate limiting on API endpoints")],
        "out_of_scope": [source("scope-out-1", "Changing authentication")],
        "functional_requirements": [source("functional-1", "Enforce a bounded request rate.")],
        "non_functional_requirements": [],
        "acceptance_criteria": [source("acceptance-1", "Requests beyond the limit are rejected.", ["functional-1"])],
        "assumptions": [
            {"id": "assumption-1", "text": "A default threshold is acceptable.", "kind": "assumption", "source_segment_ids": []}
        ],
        "risks": [source("risk-1", "A low threshold can reject valid traffic.")],
        "unresolved_questions": [
            {"id": "question-1", "text": "What threshold should apply?", "kind": "question", "source_segment_ids": []}
        ],
        "personas": [source("persona-1", "API consumer")],
        "user_journeys": [source("journey-1", "Consumer receives an explicit rate-limit response")],
        "constraints": [source("constraint-1", "The rate limiter remains observable")],
        "dependencies": [source("dependency-1", "The API gateway middleware pipeline")],
    }


async def test_litellm_planner_generates_a_source_grounded_product_specification() -> None:
    captured: dict[str, object] = {}
    fixture = valid_product_specification()

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(fixture)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    specification = await planner.generate_product_specification(
        ProductSpecificationContext(initial_specification="Add a rate limiter."),
        planner_gateway(),
    )

    assert specification == ProductSpecification.model_validate(fixture)
    assert captured["authorization"] == "Bearer planner-test-key"
    assert captured["body"]["model"] == "balanced"  # type: ignore[index]
    assert captured["body"]["response_format"] == {"type": "json_object"}  # type: ignore[index]
    payload = json.loads(captured["body"]["messages"][1]["content"])  # type: ignore[index]
    assert payload == {"source_segments": [{"id": "source-1", "content": "Add a rate limiter."}]}
    assert "no tools" in captured["body"]["messages"][0]["content"]  # type: ignore[index]


async def test_litellm_planner_rejects_product_specification_with_unknown_source_segment() -> None:
    fixture = valid_product_specification()
    fixture["title"]["source_segment_ids"] = ["unknown-source"]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(fixture)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(PlannerError, match="unknown source segments: title"):
        await planner.generate_product_specification(
            ProductSpecificationContext(initial_specification="Add a rate limiter."),
            planner_gateway(),
        )


def test_product_specification_rejects_a_question_as_a_requirement() -> None:
    fixture = valid_product_specification()
    fixture["functional_requirements"][0]["kind"] = "question"
    fixture["functional_requirements"][0]["source_segment_ids"] = []

    with pytest.raises(ValueError, match="must be source-grounded"):
        ProductSpecification.model_validate(fixture)


def test_product_specification_is_bounded_to_readable_workbench_evidence() -> None:
    fixture = valid_product_specification()
    fixture["desired_outcomes"] = [
        {
            "id": f"outcome-{index}",
            "text": "x" * 10_000,
            "kind": "source",
            "source_segment_ids": ["source-1"],
        }
        for index in range(10)
    ]

    with pytest.raises(ValueError, match="96 KiB evidence limit"):
        ProductSpecification.model_validate(fixture)
