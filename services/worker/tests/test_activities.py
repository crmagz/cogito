from __future__ import annotations

import pytest
from temporalio.testing import ActivityEnvironment

from cogito_worker.activities import WorkerActivities
from cogito_worker.execution import ExecutionJobSettings, ExecutionWorkspaceService
from cogito_worker.models import (
    ExecutionRequest,
    ExecutionWorkspace,
    McpToolGrant,
    PhaseExecutionRequest,
    PlanPhase,
    ValidationRequest,
)
from cogito_worker.run_state import stage_invocation_id
from cogito_worker.run_state import RunStateReporter

from .fakes import (
    InMemoryExecutionJobClient,
    InMemoryHarness,
    InMemoryExecutionWorkspaces,
    InMemoryRunStore,
)


@pytest.fixture
def store() -> InMemoryRunStore:
    return InMemoryRunStore()


@pytest.fixture
def activities(store: InMemoryRunStore) -> WorkerActivities:
    return WorkerActivities(store, InMemoryExecutionWorkspaces(), InMemoryHarness())


@pytest.fixture
def env() -> ActivityEnvironment:
    return ActivityEnvironment()


async def test_load_plan_returns_plan_from_store(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.plans["s3://plans/plans/run-1/plan.json"] = {"title": "Test plan"}

    result = await env.run(activities.load_plan, "s3://plans/plans/run-1/plan.json")

    assert result == {"title": "Test plan"}


async def test_load_resolved_workflow_returns_immutable_artifact_from_store(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    workflow_ref = "s3://plan-snapshots/runs/run-1/resolved-workflow.json"
    resolution = {"run_id": "run-1", "gates": [], "phases": []}
    store.plans[workflow_ref] = resolution

    result = await env.run(activities.load_resolved_workflow, workflow_ref)

    assert result == resolution


async def test_report_status_creates_status_when_none_exists(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    await env.run(activities.report_status, "run-1", "claimed")

    assert store.statuses["run-1"]["status"] == "claimed"
    assert store.statuses["run-1"]["run_id"] == "run-1"
    assert "updated_at" in store.statuses["run-1"]


async def test_report_status_preserves_existing_fields(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
):
    store.statuses["run-1"] = {"run_id": "run-1", "status": "queued", "plan_ref": "s3://plans/plans/run-1/plan.json"}

    await env.run(activities.report_status, "run-1", "completed")

    assert store.statuses["run-1"]["status"] == "completed"
    assert store.statuses["run-1"]["plan_ref"] == "s3://plans/plans/run-1/plan.json"


async def test_report_status_forwards_only_bounded_transition_metadata(env: ActivityEnvironment, store: InMemoryRunStore):
    class RecordingRunStateReporter:
        calls: list[tuple[str, str, str | None, dict | None]] = []

        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            self.calls.append((run_id, status, failure_detail, metadata))

    reporter: RunStateReporter = RecordingRunStateReporter()
    activities = WorkerActivities(store, InMemoryExecutionWorkspaces(), InMemoryHarness(), run_state=reporter)

    await env.run(activities.report_status, "run-1", "completed", None, {"phase_result": {"summary": "secret"}})

    assert reporter.calls == [("run-1", "completed", None, {"phase_result": {"summary": "secret"}})]


async def test_run_phase_records_safe_stage_invocation_before_harness_execution(
    env: ActivityEnvironment, store: InMemoryRunStore
) -> None:
    class RecordingRunStateReporter:
        calls: list[tuple[str, str, str, int, bool]] = []

        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            del run_id, status, failure_detail, metadata

        async def record_stage_invocation(
            self, run_id: str, stage_id: str, role: str, attempt: int, trace_context_available: bool
        ) -> None:
            self.calls.append((run_id, stage_id, role, attempt, trace_context_available))

    reporter: RunStateReporter = RecordingRunStateReporter()
    harness = InMemoryHarness()
    activities = WorkerActivities(store, InMemoryExecutionWorkspaces(), harness, run_state=reporter)
    request = PhaseExecutionRequest(
        phase=PlanPhase(
            id="implement-api",
            name="Implement API",
            description="Safe test phase",
            tasks=["implement"],
            acceptance_criteria=["passes"],
            verification=["pytest"],
        ),
        workspace=ExecutionWorkspace(run_id="run-1", job_name="cogito-execution-run-1", workspace_root="/workspace"),
        max_turns=3,
        timeout_seconds=60,
        traceparent="00-" + "a" * 32 + "-" + "b" * 16 + "-01",
    )

    await env.run(activities.run_phase, request)

    assert reporter.calls == [("run-1", "implement-api", "developer", 1, True)]
    assert len(harness.requests) == 1
    assert harness.requests[0].workspace.audit_invocation_id == stage_invocation_id(
        "run-1", "implement-api", "developer", 1
    )
    assert request.workspace.audit_invocation_id == ""


async def test_run_phase_continues_when_stage_invocation_evidence_is_unavailable(
    env: ActivityEnvironment, store: InMemoryRunStore
) -> None:
    class UnavailableRunStateReporter:
        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            del run_id, status, failure_detail, metadata

        async def record_stage_invocation(
            self, run_id: str, stage_id: str, role: str, attempt: int, trace_context_available: bool
        ) -> None:
            del run_id, stage_id, role, attempt, trace_context_available
            raise RuntimeError("database unavailable")

    harness = InMemoryHarness()
    activities = WorkerActivities(store, InMemoryExecutionWorkspaces(), harness, run_state=UnavailableRunStateReporter())
    request = PhaseExecutionRequest(
        phase=PlanPhase(
            id="implement-api",
            name="Implement API",
            description="Safe test phase",
            tasks=["implement"],
            acceptance_criteria=["passes"],
            verification=["pytest"],
        ),
        workspace=ExecutionWorkspace(run_id="run-1", job_name="cogito-execution-run-1", workspace_root="/workspace"),
        max_turns=3,
        timeout_seconds=60,
    )

    result = await env.run(activities.run_phase, request)

    assert result.succeeded is True
    assert len(harness.requests) == 1
    assert harness.requests[0].workspace.audit_invocation_id == stage_invocation_id(
        "run-1", "implement-api", "developer", 1
    )


async def test_validator_accepts_converged_evidence_with_passing_verification(
    env: ActivityEnvironment, activities: WorkerActivities
) -> None:
    result = await env.run(
        activities.validate_implementation,
        ValidationRequest(
            run_id="run-1",
            phase_results=[{"succeeded": True, "verification": [{"passed": True}]}],
            review={"status": "converged"},
        ),
    )

    assert result.status == "passed"
    assert result.checked_phases == 1


@pytest.mark.parametrize("verification", [[], [{"passed": False}]])
async def test_validator_rejects_missing_or_failed_verification(
    verification: list[dict],
    env: ActivityEnvironment, activities: WorkerActivities
) -> None:
    result = await env.run(
        activities.validate_implementation,
        ValidationRequest(
            run_id="run-1",
            phase_results=[{"succeeded": True, "verification": verification}],
            review={"status": "converged"},
        ),
    )

    assert result.status == "failed"
    assert result.reason == "verification_not_passed"


async def test_freeze_implementation_artifact_adds_only_bounded_mcp_evidence(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
) -> None:
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        mcp_grants=[
            McpToolGrant(
                server_id="cogito_readonly_mcp",
                server_version="1.0.1",
                server_manifest_sha256="b" * 64,
                tool_name="catalog_read",
                input_schema_sha256="c" * 64,
            )
        ],
    )

    artifact = await env.run(activities.freeze_implementation_artifact, "run-1", {"review": {}}, workspace)

    assert store.implementation_artifacts[artifact.sha256] == {
        "review": {},
        "mcp_invocations": {
            "version": 1,
            "status": "observed",
            "events": [],
            "selected_grants": [
                {
                    "role": "developer",
                    "server_id": "cogito_readonly_mcp",
                    "server_version": "1.0.1",
                    "server_manifest_sha256": "b" * 64,
                    "tool_name": "catalog_read",
                    "input_schema_sha256": "c" * 64,
                }
            ],
        },
    }


async def test_freeze_implementation_artifact_reports_aggregate_mcp_evidence_without_raw_content(
    env: ActivityEnvironment, store: InMemoryRunStore
) -> None:
    class ObservedWorkspaces(InMemoryExecutionWorkspaces):
        async def collect_mcp_invocations(self, workspace: ExecutionWorkspace) -> dict[str, object] | None:
            del workspace
            return {
                "version": 1,
                "status": "observed",
                "events": [
                    {
                        "server_id": "readonly",
                        "server_version": "1.0.0",
                        "server_manifest_sha256": "b" * 64,
                        "tool_name": "catalog_read",
                        "input_schema_sha256": "c" * 64,
                        "outcome": "success",
                        "invocation_count": 2,
                        "request_body": "never persisted in audit evidence",
                    }
                ],
            }

    class RecordingRunStateReporter:
        calls: list[tuple[str, dict[str, object]]] = []

        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            del run_id, status, failure_detail, metadata

        async def record_stage_invocation(
            self, run_id: str, stage_id: str, role: str, attempt: int, trace_context_available: bool
        ) -> None:
            del run_id, stage_id, role, attempt, trace_context_available

        async def record_mcp_invocation_evidence(self, run_id: str, evidence: dict[str, object]) -> None:
            self.calls.append((run_id, evidence))

    reporter: RunStateReporter = RecordingRunStateReporter()
    activities = WorkerActivities(store, ObservedWorkspaces(), InMemoryHarness(), run_state=reporter)
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        mcp_grants=[
            McpToolGrant(
                server_id="readonly",
                server_version="1.0.0",
                server_manifest_sha256="b" * 64,
                tool_name="catalog_read",
                input_schema_sha256="c" * 64,
            )
        ],
    )

    await env.run(activities.freeze_implementation_artifact, "run-1", {"review": {}}, workspace)

    assert reporter.calls[0][0] == "run-1"
    assert reporter.calls[0][1]["events"] == [
        {
            "server_id": "readonly",
            "server_version": "1.0.0",
            "server_manifest_sha256": "b" * 64,
            "tool_name": "catalog_read",
            "input_schema_sha256": "c" * 64,
            "outcome": "success",
            "invocation_count": 2,
        }
    ]
    assert "request_body" not in store.implementation_artifacts[next(iter(store.implementation_artifacts))]["mcp_invocations"][
        "events"
    ][0]


async def test_freeze_implementation_artifact_records_an_explicit_empty_mcp_selection(
    env: ActivityEnvironment, activities: WorkerActivities, store: InMemoryRunStore
) -> None:
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        mcp_selection_explicit=True,
    )

    artifact = await env.run(activities.freeze_implementation_artifact, "run-1", {"review": {}}, workspace)

    assert store.implementation_artifacts[artifact.sha256]["mcp_invocations"] == {
        "version": 1,
        "status": "not_applicable",
        "events": [],
        "selected_grants": [],
    }


async def test_execution_workspace_activities_manage_only_the_current_run(
    env: ActivityEnvironment, store: InMemoryRunStore
):
    class RecordingRunStateReporter:
        workspace_calls: list[tuple[str, str, str]] = []

        async def report(self, run_id: str, status: str, failure_detail: str | None, metadata: dict | None) -> None:
            del run_id, status, failure_detail, metadata

        async def record_stage_invocation(
            self, run_id: str, stage_id: str, role: str, attempt: int, trace_context_available: bool
        ) -> None:
            del run_id, stage_id, role, attempt, trace_context_available

        async def record_mcp_invocation_evidence(self, run_id: str, evidence: dict[str, object]) -> None:
            del run_id, evidence

        async def record_execution_workspace_lifecycle(self, run_id: str, job_name: str, lifecycle: str) -> None:
            self.workspace_calls.append((run_id, job_name, lifecycle))

    reporter: RunStateReporter = RecordingRunStateReporter()
    jobs = InMemoryExecutionJobClient()
    activities = WorkerActivities(
        store,
        ExecutionWorkspaceService(
            ExecutionJobSettings(
                namespace="cogito",
                image="cogito-worker:local",
                image_pull_policy="IfNotPresent",
                workspace_root="/workspace",
                idle_seconds=3600,
                startup_timeout_seconds=30,
                cleanup_timeout_seconds=90,
                active_deadline_seconds=3900,
                ttl_seconds_after_finished=300,
                termination_grace_period_seconds=10,
                workspace_size_limit="2Gi",
                resources={"limits": {"memory": "1Gi"}},
                allowed_git_hosts=("github.com",),
                minio_endpoint="cogito-minio:9000",
                minio_secure=False,
                specs_bucket="specs",
                specs_prefix="specs",
                specs_max_archive_bytes=1024 * 1024,
                specs_max_extracted_bytes=2 * 1024 * 1024,
                object_store_secret="cogito-minio",
                object_store_access_key_secret_key="rootUser",
                object_store_secret_key_secret_key="rootPassword",
                litellm_endpoint="http://cogito-litellm:4000",
                litellm_model="complex",
                litellm_key_secret="cogito-developer-key",
                litellm_key_secret_key="api-key",
                git_author_name="Cogito Agent",
                git_author_email="cogito@local.invalid",
                command_output_limit_bytes=262144,
            ),
            jobs,
        ),
        InMemoryHarness(),
        run_state=reporter,
    )

    workspace = await env.run(
        activities.provision_execution_workspace,
        ExecutionRequest(run_id="run-1", spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64, target_repos=[]),
    )
    await env.run(activities.cleanup_execution_workspace, workspace)

    assert [job_name for job_name, _ in jobs.created] == [workspace.job_name]
    assert jobs.awaited == [(workspace.job_name, 30)]
    assert jobs.deleted == [workspace.job_name]
    assert reporter.workspace_calls == [
        ("run-1", workspace.job_name, "provisioned"),
        ("run-1", workspace.job_name, "cleanup_started"),
    ]
