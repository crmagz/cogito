from __future__ import annotations

import copy
import json

import httpx
import pytest

from cogito_api.models import AgentGatewayResolution, AiPlan, ProductSpecification
from cogito_api.main import _discovery_agent_prompt, _planning_agent_prompt
from cogito_api.planner import (
    LiteLLMPlanner,
    OperatorRefinement,
    PlannerError,
    PlanningContext,
    ProductSpecificationContext,
    assemble_agent_plan_draft,
)

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


def planner_draft(plan: dict) -> dict:
    """Return only the variable fields a planner is allowed to control."""

    return {key: plan[key] for key in ("title", "summary", "phases", "review_profile") if key in plan}


def assert_trusted_plan(plan: AiPlan, expected: dict) -> None:
    """Assert that the platform restored its trusted envelope and ownership facts."""

    expected_plan = AiPlan.model_validate(expected)
    assert plan.model_dump(exclude={"phases"}) == expected_plan.model_dump(exclude={"phases"})
    assert [phase.model_dump(exclude={"requirement_assignments"}) for phase in plan.phases] == [
        phase.model_dump(exclude={"requirement_assignments"}) for phase in expected_plan.phases
    ]
    assert all(
        assignment.relationship.value == "owns"
        for phase in plan.phases
        for assignment in phase.requirement_assignments
    )


def test_planning_agent_prompt_requires_project_scoped_uv_verification(valid_plan: dict) -> None:
    """The planner must produce commands the execution harness can reproduce."""

    plan = AiPlan.model_validate(valid_plan)
    prompt = _planning_agent_prompt(
        "Create a uv-managed Python project.",
        plan.target_repos,
        plan.spec_set,
        plan.constraints,
        ["requirement-1"],
        None,
        None,
    )

    assert "uv run pytest" in prompt
    assert "uv run python -c" in prompt
    assert "Never use bare `python`" in prompt
    assert "Never create a phase for any of those activities" in prompt
    assert "Every returned phase must own at least one supplied requirement ID" in prompt


def test_discovery_agent_prompt_bounds_repository_inspection() -> None:
    """Discovery must reserve its agent budget for a useful structured handoff."""

    prompt = _discovery_agent_prompt("Inspect the existing package.", ["https://github.com/acme/example.git#abc"])

    assert "four-command research budget" in prompt
    assert "Do not install dependencies" in prompt
    assert "stop using tools and return the required JSON" in prompt


def test_agent_plan_draft_receives_only_the_trusted_server_envelope(valid_plan: dict) -> None:
    """A specialist handoff cannot choose repository pins or execution limits."""

    expected = AiPlan.model_validate(valid_plan)
    result = assemble_agent_plan_draft(
        json.dumps(planner_draft(valid_plan)),
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=expected.target_repos,
            spec_set=expected.spec_set,
            constraints=expected.constraints,
        ),
    )

    assert_trusted_plan(result, valid_plan)


def test_agent_plan_draft_converts_repeated_ownership_to_verification(valid_plan: dict) -> None:
    """A final verification phase may cite earlier work without owning it again."""

    candidate = planner_draft(valid_plan)
    candidate["phases"][1]["requirement_ids"].append("acceptance-1")
    expected = AiPlan.model_validate(valid_plan)

    result = assemble_agent_plan_draft(
        json.dumps(candidate),
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=expected.target_repos,
            spec_set=expected.spec_set,
            constraints=expected.constraints,
        ),
    )

    phase = result.phases[1]
    assert phase.requirement_ids == ["acceptance-2"]
    assert "acceptance-1" in phase.verification_references
    assert any(
        assignment.requirement_id == "acceptance-1" and assignment.relationship.value == "verifies"
        for assignment in phase.requirement_assignments
    )


async def test_litellm_planner_requests_json_with_dedicated_bearer_key(valid_plan: dict) -> None:
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["authorization"] = request.headers["authorization"]
        captured["body"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps(planner_draft(valid_plan))}}]},
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

    assert_trusted_plan(plan, valid_plan)
    assert captured["authorization"] == "Bearer planner-test-key"
    assert captured["body"]["model"] == "balanced"  # type: ignore[index]
    assert captured["body"]["response_format"] == {"type": "json_object"}  # type: ignore[index]
    assert '"title"' in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "product specification has already been accepted" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "repository-relative paths" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "Minimize the number of phases" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "pytest as a development dependency synchronized by default" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    planner_input = json.loads(captured["body"]["messages"][1]["content"])  # type: ignore[index]
    assert "target_repos" not in planner_input
    assert "constraints" not in planner_input


async def test_litellm_planner_includes_operator_feedback_without_promoting_it_to_policy(valid_plan: dict) -> None:
    """A reviewer comment is task input only; the trusted envelope remains API-owned."""

    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        draft = copy.deepcopy(planner_draft(valid_plan)) | {
            "operator_feedback_id": "decision-1",
            "operator_feedback_response": "Adds the requested quality tooling to implementation and verification.",
        }
        draft["phases"][0]["tasks"] = ["Add Pydantic, mypy, and Ruff."]
        draft["phases"][0]["verification"] = ["uv run ruff check src/"]
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(draft)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
            operator_refinement=OperatorRefinement(
                refinement_id="decision-1",
                source_gate="implementation",
                comment="Add Pydantic, mypy, and Ruff before delivery.",
            ),
            base_plan=AiPlan.model_validate(valid_plan),
        ),
        planner_gateway(),
    )

    planner_input = json.loads(captured["body"]["messages"][1]["content"])  # type: ignore[index]
    assert planner_input["operator_refinement"]["source_gate"] == "implementation"
    assert planner_input["operator_refinement"]["comment"] == "Add Pydantic, mypy, and Ruff before delivery."
    assert planner_input["base_plan"]["title"] == valid_plan["title"]
    assert "cannot change repositories" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "do not create a separate phase solely" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "preserves every existing task" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert plan.operator_feedback_id == "decision-1"
    assert valid_plan["phases"][0]["tasks"][0] in plan.phases[0].tasks
    assert valid_plan["phases"][0]["verification"][0] in plan.phases[0].verification


async def test_litellm_planner_honors_an_explicit_operator_replacement(valid_plan: dict) -> None:
    """Only an exact removal directive may omit a base-plan verification command."""

    removed = valid_plan["phases"][0]["verification"][0]

    async def handler(_: httpx.Request) -> httpx.Response:
        draft = copy.deepcopy(planner_draft(valid_plan)) | {
            "operator_feedback_id": "decision-2",
            "operator_feedback_response": "Replaces the failing verification command.",
            "superseded_base_verification": [removed],
        }
        draft["phases"][0]["verification"] = ["npm run lint"]
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(draft)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Replace the failing verification.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
            operator_refinement=OperatorRefinement(
                refinement_id="decision-2",
                source_gate="implementation",
                comment="Replace npm run typecheck with npm run lint.",
            ),
            base_plan=AiPlan.model_validate(valid_plan),
        ),
        planner_gateway(),
    )

    assert removed not in plan.phases[0].verification
    assert "npm run lint" in plan.phases[0].verification


async def test_litellm_planner_retries_an_invalid_requirement_partition(valid_plan: dict) -> None:
    duplicate = json.loads(json.dumps(valid_plan))
    duplicate["phases"][1]["requirement_ids"] = ["acceptance-1"]
    requests: list[dict[str, object]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        candidate = planner_draft(duplicate) if len(requests) == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
            requirement_ids=("acceptance-1", "acceptance-2"),
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert len(requests) == 2
    assert requests[0]["messages"][1]["content"].find("required_requirement_ids") >= 0  # type: ignore[index]
    assert "prior candidate was rejected" in requests[1]["messages"][2]["content"]  # type: ignore[index]


@pytest.mark.parametrize("invalid_phase_ids", [["acceptance-1", "acceptance-1"], []])
async def test_litellm_planner_retries_requirement_partition_errors_rejected_by_the_schema(
    valid_plan: dict, invalid_phase_ids: list[str]
) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["requirement_ids"] = invalid_phase_ids
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Add a rate limiter.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
            requirement_ids=("acceptance-1", "acceptance-2"),
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


async def test_litellm_planner_stops_after_three_requirement_partition_attempts(valid_plan: dict) -> None:
    duplicate = json.loads(json.dumps(valid_plan))
    duplicate["phases"][1]["requirement_ids"] = ["acceptance-1"]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(planner_draft(duplicate))}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(PlannerError, match="failed contract validation"):
        await planner.generate(
            PlanningContext(
                initial_specification="Add a rate limiter.",
                target_repos=valid_plan["target_repos"],
                spec_set=valid_plan["spec_set"],
                constraints=AiPlan.model_validate(valid_plan).constraints,
                requirement_ids=("acceptance-1", "acceptance-2"),
            ),
            planner_gateway(),
        )

    assert requests == 3


async def test_litellm_planner_rejects_model_output_that_attempts_to_set_target_repositories(valid_plan: dict) -> None:
    changed = dict(valid_plan)
    changed["target_repos"] = ["https://github.com/acme/other.git#0123456789abcdef0123456789abcdef01234567"]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(changed)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(PlannerError, match="invalid plan draft JSON"):
        await planner.generate(
            PlanningContext(
                initial_specification="Add a rate limiter.",
                target_repos=valid_plan["target_repos"],
                spec_set=valid_plan["spec_set"],
                constraints=AiPlan.model_validate(valid_plan).constraints,
            ),
            planner_gateway(),
        )


async def test_litellm_planner_repairs_ephemeral_workspace_verification_paths(valid_plan: dict) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["verification"] = ["test -f /tmp/specification.md"]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

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

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


async def test_litellm_planner_repairs_optional_uv_configuration_marker_verification(valid_plan: dict) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["verification"] = ["grep -q '\\[tool.uv\\]' pyproject.toml"]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Scaffold a Python project with uv.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


async def test_litellm_planner_repairs_volatile_uv_sync_output_verification(valid_plan: dict) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["verification"] = ["uv sync --frozen 2>&1 | grep -q 'Resolved'"]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))

    plan = await planner.generate(
        PlanningContext(
            initial_specification="Scaffold a Python project with uv.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


async def test_litellm_planner_repairs_natural_language_verification_descriptions(valid_plan: dict) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["verification"] = [
        "uv sync --frozen completes without errors",
        "uv run pytest runs successfully and reports test results",
    ]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Scaffold a Python project with uv.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


@pytest.mark.parametrize("command", ["mypy src/", "ruff check src/", "pytest", "python -m pytest tests/"])
async def test_litellm_planner_repairs_quality_tool_verification_outside_uv(
    valid_plan: dict, command: str
) -> None:
    invalid = json.loads(json.dumps(valid_plan))
    invalid["phases"][0]["verification"] = [command]
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = planner_draft(invalid) if requests == 1 else planner_draft(valid_plan)
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    plan = await planner.generate(
        PlanningContext(
            initial_specification="Scaffold a Python project with uv.",
            target_repos=valid_plan["target_repos"],
            spec_set=valid_plan["spec_set"],
            constraints=AiPlan.model_validate(valid_plan).constraints,
        ),
        planner_gateway(),
    )

    assert_trusted_plan(plan, valid_plan)
    assert requests == 2


async def test_litellm_planner_accepts_a_single_fenced_json_object(valid_plan: dict) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": f"```json\n{json.dumps(planner_draft(valid_plan))}\n```"}}]})

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

    def source(statement_id: str, text: str) -> dict:
        return {"kind": "source", "id": statement_id, "text": text, "source_segment_ids": ["source-1"]}

    return {
        "schema_version": 3,
        "title": source("title", "Rate limiting"),
        "user_story": source("user-story", "As an API operator, I need bounded request rates so the service remains available."),
        "outcome": source("outcome", "Protect API endpoints from abuse."),
        "acceptance_criteria": [source("acceptance-1", "Requests beyond the limit are rejected.")],
        "technical_context": [source("technical-context", "Rate limiting is applied in the API gateway middleware pipeline.")],
        "assumptions": [
            {"id": "assumption-1", "text": "A default threshold is acceptable.", "kind": "assumption", "source_segment_ids": []}
        ],
        "unresolved_questions": [
            {"id": "question-1", "text": "What threshold should apply?", "kind": "question", "source_segment_ids": []}
        ],
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
    assert "Title, user_story, outcome" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "Only assumptions may use kind=assumption" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert "Default assumptions and unresolved_questions to empty arrays" in captured["body"]["messages"][0]["content"]  # type: ignore[index]
    assert '"acceptance_criteria"' in captured["body"]["messages"][0]["content"]  # type: ignore[index]


async def test_litellm_planner_repairs_an_incomplete_work_specification() -> None:
    incomplete = valid_product_specification()
    incomplete["schema_version"] = 2
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        candidate = incomplete if requests == 1 else valid_product_specification()
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(candidate)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    specification = await planner.generate_product_specification(
        ProductSpecificationContext(initial_specification="Add a rate limiter."),
        planner_gateway(),
    )

    assert specification == ProductSpecification.model_validate(valid_product_specification())
    assert requests == 2


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


async def test_litellm_planner_repairs_omitted_citation_for_a_single_source_segment() -> None:
    fixture = valid_product_specification()
    fixture["title"]["source_segment_ids"] = []

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(fixture)}}]})

    planner = LiteLLMPlanner(make_settings(), transport=httpx.MockTransport(handler))
    specification = await planner.generate_product_specification(
        ProductSpecificationContext(initial_specification="Add a rate limiter."),
        planner_gateway(),
    )

    assert specification.title.source_segment_ids == ["source-1"]


def test_product_specification_rejects_a_question_as_a_requirement() -> None:
    fixture = valid_product_specification()
    fixture["acceptance_criteria"][0]["kind"] = "question"
    fixture["acceptance_criteria"][0]["source_segment_ids"] = []

    with pytest.raises(ValueError, match="must be source-grounded"):
        ProductSpecification.model_validate(fixture)


def test_product_specification_is_bounded_to_readable_workbench_evidence() -> None:
    fixture = valid_product_specification()
    fixture["technical_context"] = [
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
