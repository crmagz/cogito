"""Tool-free product-specification refinement behavior expressed in Gherkin."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from pytest_bdd import given, scenarios, then, when

from cogito_api.models import AgentGatewayResolution
from cogito_api.planner import LiteLLMPlanner, ProductSpecificationContext

from .conftest import make_settings

scenarios("features/product_specification.feature")


@pytest.fixture
def refinement_context() -> dict[str, object]:
    """Share the gateway request and structured response through one scenario."""

    return {}


@given("a source-grounded product specification response")
def source_grounded_product_specification_response(refinement_context: dict[str, object]) -> None:
    """Provide the model response returned through the LiteLLM-compatible boundary."""

    def source(statement_id: str, text: str) -> dict[str, object]:
        return {"id": statement_id, "text": text, "kind": "source", "source_segment_ids": ["source-1"]}

    refinement_context["response"] = {
        "title": source("title", "Rate limiting"),
        "problem_statement": source("problem", "The API needs bounded request rates."),
        "desired_outcomes": [source("outcome-1", "Protect API endpoints from abuse.")],
        "actors": [source("actor-1", "API consumers")],
        "in_scope": [source("scope-in-1", "Rate limiting on API endpoints")],
        "out_of_scope": [source("scope-out-1", "Changing authentication")],
        "functional_requirements": [source("functional-1", "Enforce a bounded request rate.")],
        "non_functional_requirements": [],
        "acceptance_criteria": [source("acceptance-1", "Requests beyond the limit are rejected.")],
        "assumptions": [
            {"id": "assumption-1", "text": "A default threshold is acceptable.", "kind": "assumption", "source_segment_ids": []}
        ],
        "risks": [source("risk-1", "A low threshold can reject valid traffic.")],
        "unresolved_questions": [
            {"id": "question-1", "text": "What threshold should apply?", "kind": "question", "source_segment_ids": []}
        ],
    }


@when("the planner refines the product intake")
def planner_refines_product_intake(refinement_context: dict[str, object]) -> None:
    """Exercise the model-gateway request and strict response contract together."""

    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps(refinement_context["response"])}}]},
        )

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    refinement_context["specification"] = asyncio.run(
        planner.generate_product_specification(
            ProductSpecificationContext(initial_specification="Add a rate limiter."),
            AgentGatewayResolution(
                policy_revision="agent_gateway_initial",
                project_id="default",
                role="planner",
                registration_id="planner",
                registration_version="1.1.0",
                manifest_sha256="a" * 64,
                model_alias="balanced",
                max_budget_usd=5.0,
                toolset="planning-readonly",
            ),
        )
    )
    refinement_context["request"] = captured


@then("the structured draft preserves source provenance and unresolved questions")
def structured_draft_preserves_provenance(refinement_context: dict[str, object]) -> None:
    """Assert that uncertainty remains visibly separate from sourced requirements."""

    specification = refinement_context["specification"]
    assert specification.functional_requirements[0].source_segment_ids == ["source-1"]
    assert specification.assumptions[0].kind.value == "assumption"
    assert specification.unresolved_questions[0].kind.value == "question"


@then("the planner request has no tool or repository authority")
def planner_request_has_no_tool_or_repository_authority(refinement_context: dict[str, object]) -> None:
    """Verify the planner receives only the intake and its model-gateway bearer credential."""

    request = refinement_context["request"]
    payload = request["payload"]
    assert request["authorization"] == "Bearer planner-test-key"
    assert payload["model"] == "balanced"
    assert "tool" not in payload
    assert "target_repositories" not in payload
    assert "cannot access repositories" in payload["messages"][0]["content"]
