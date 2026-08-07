from __future__ import annotations

import json
import math
from dataclasses import replace

import pytest

from cogito_worker.execution import (
    ExecutionJobSettings,
    ExecutionWorkspaceService,
    KubernetesExecutionJobClient,
    _append_bounded_output,
    _bounded_output,
    build_execution_job,
    execution_job_name,
    _sanitize_diagnostics,
)
import cogito_worker.budgets as budgets
from cogito_worker.budgets import (
    KubernetesLiteLLMRunKeyManager,
    RunBudget,
    _expected_mcp_tools,
    _mcp_invocation_evidence,
    _run_key_payload,
    _validate_budget,
    run_key_secret_name,
)
from cogito_worker.config import McpGatewayServer
from cogito_worker.models import (
    AgentGatewayResolution,
    ExecutionRequest,
    ExecutionWorkspace,
    McpServerConfiguration,
    McpToolGrant,
    RegistrationReference,
    ToolGrant,
)

from .fakes import InMemoryExecutionJobClient


def execution_settings() -> ExecutionJobSettings:
    return ExecutionJobSettings(
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
        git_credentials_secret="cogito-developer-git",
        git_credentials_secret_key="token",
        git_author_name="Cogito Agent",
        git_author_email="cogito@local.invalid",
        command_output_limit_bytes=262144,
    )


def test_execution_job_template_uses_an_isolated_emptydir_workspace() -> None:
    """Execution jobs use a private workspace and cannot access Kubernetes APIs."""

    job_name = execution_job_name("run-1")
    job = build_execution_job(
        request=ExecutionRequest(
            run_id="run-1",
            spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            target_repos=["https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"],
        ),
        job_name=job_name,
        settings=execution_settings(),
    )

    pod_spec = job["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    init_container = pod_spec["initContainers"][0]

    assert job["metadata"]["name"] == job_name
    assert job["metadata"]["labels"]["cogito.dev/run-hash"]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"]["runAsNonRoot"] is True
    assert pod_spec["volumes"] == [{"name": "workspace", "emptyDir": {"sizeLimit": "2Gi"}}]
    assert pod_spec["activeDeadlineSeconds"] == 3900
    assert pod_spec["ttlSecondsAfterFinished"] == 300
    assert container["volumeMounts"] == [{"name": "workspace", "mountPath": "/workspace"}]
    assert container["command"] == ["python", "-m", "cogito_worker.execution_pod"]
    assert init_container["command"] == ["python", "-m", "cogito_worker.execution_prepare"]
    assert all(env["name"] not in {"MINIO_ACCESS_KEY", "MINIO_SECRET_KEY"} for env in container["env"])
    secret_env = {env["name"]: env["valueFrom"] for env in init_container["env"] if "valueFrom" in env}
    assert secret_env["MINIO_ACCESS_KEY"]["secretKeyRef"]["name"] == "cogito-minio"
    assert secret_env["COGITO_GIT_HTTPS_TOKEN"]["secretKeyRef"]["name"] == "cogito-developer-git"
    execution_secret_env = {env["name"]: env["valueFrom"] for env in container["env"] if "valueFrom" in env}
    assert execution_secret_env["ANTHROPIC_AUTH_TOKEN"]["secretKeyRef"]["name"] == "cogito-developer-key"
    assert execution_secret_env["COGITO_GIT_HTTPS_TOKEN"]["secretKeyRef"]["name"] == "cogito-developer-git"
    assert (
        next(env["value"] for env in container["env"] if env["name"] == "ANTHROPIC_BASE_URL")
        == "http://cogito-litellm:4000"
    )
    assert next(env["value"] for env in container["env"] if env["name"] == "ANTHROPIC_MODEL") == "complex"
    assert (
        next(
            env["value"]
            for env in container["env"]
            if env["name"] == "CLAUDE_CODE_DISABLE_LEGACY_MODEL_REMAP"
        )
        == "1"
    )
    assert json.loads(next(env["value"] for env in init_container["env"] if env["name"] == "COGITO_TARGET_REPOS")) == [
        "https://github.com/acme/api-gateway.git#0123456789abcdef0123456789abcdef01234567"
    ]


def test_execution_job_uses_the_approved_budget_without_exceeding_operator_ceiling() -> None:
    settings = execution_settings()
    job = build_execution_job(
        request=ExecutionRequest(
            run_id="run-1",
            spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            target_repos=[],
            execution_timeout_seconds=120,
        ),
        job_name=execution_job_name("run-1"),
        settings=settings,
    )

    pod_spec = job["spec"]["template"]["spec"]
    container = pod_spec["containers"][0]
    assert pod_spec["activeDeadlineSeconds"] == 120
    assert (
        next(env["value"] for env in container["env"] if env["name"] == "COGITO_EXECUTION_IDLE_SECONDS")
        == "120"
    )

    with pytest.raises(ValueError, match="operator-configured Job deadline"):
        build_execution_job(
            request=ExecutionRequest(
                run_id="run-1",
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                target_repos=[],
                execution_timeout_seconds=3901,
            ),
            job_name=execution_job_name("run-1"),
            settings=settings,
        )


def test_run_key_payload_scopes_mcp_access_to_explicit_gateway_tools() -> None:
    gateway_server_id = "a" * 32
    budget = RunBudget(
        run_id="run-1",
        max_cost_usd=2.5,
        model="complex",
        expires_in_seconds=120,
        mcp_tool_permissions={gateway_server_id: ("catalog_read",)},
    )

    _validate_budget(budget)
    payload = _run_key_payload("test-token", budget)

    assert payload["object_permission"] == {
        "mcp_servers": [gateway_server_id],
        "mcp_tool_permissions": {gateway_server_id: ["catalog_read"]},
    }
    with pytest.raises(ValueError, match="gateway server IDs"):
        _validate_budget(
            RunBudget("run-1", 2.5, "complex", 120, {"not-a-gateway-id": ("catalog_read",)})
        )


@pytest.mark.parametrize("max_cost_usd", [math.inf, math.nan])
def test_run_key_rejects_non_finite_budget(max_cost_usd: float) -> None:
    with pytest.raises(ValueError, match="positive cost"):
        _validate_budget(RunBudget("run-1", max_cost_usd, "complex", 120))


def test_run_key_payload_records_the_pinned_agent_identity_without_a_credential() -> None:
    budget = RunBudget(
        run_id="run-1",
        max_cost_usd=2.5,
        model="balanced",
        expires_in_seconds=120,
        agent_registration_id="developer",
        agent_manifest_sha256="a" * 64,
    )

    _validate_budget(budget)
    payload = _run_key_payload("test-token", budget)

    assert payload["models"] == ["balanced"]
    assert payload["metadata"] == {
        "cogito_run_hash": budgets._run_hash("run-1"),
        "cogito_agent_registration": "developer",
        "cogito_agent_manifest_sha256": "a" * 64,
    }


async def test_execution_workspace_uses_the_pinned_gateway_model_and_bounded_budget() -> None:
    class RecordingRunKeys:
        def __init__(self) -> None:
            self.budgets: list[RunBudget] = []

        async def provision(self, budget: RunBudget) -> str:
            self.budgets.append(budget)
            return "cogito-run-key-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            return None

    run_keys = RecordingRunKeys()
    jobs = InMemoryExecutionJobClient()
    service = ExecutionWorkspaceService(execution_settings(), jobs, run_keys)
    gateway = AgentGatewayResolution(
        policy_revision="agent_gateway_initial",
        project_id="default",
        role="developer",
        registration_id="developer",
        registration_version="1.0.0",
        manifest_sha256="a" * 64,
        model_alias="balanced",
        max_budget_usd=1.25,
        toolset="development-restricted",
    )

    await service.provision(
        ExecutionRequest(
            run_id="run-1",
            spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            target_repos=[],
            execution_timeout_seconds=120,
            max_cost_usd=2.5,
            registration=RegistrationReference(
                role="developer",
                registration_id="developer",
                version="1.0.0",
                manifest_sha256="a" * 64,
                component_id="developer",
                component_version="1.0.0",
                grants=[ToolGrant("developer_harness", "1.0.0", "approved_phase")],
            ),
            gateway=gateway,
        )
    )

    assert run_keys.budgets == [
        RunBudget(
            run_id="run-1",
            max_cost_usd=1.25,
            model="balanced",
            expires_in_seconds=120,
            agent_registration_id="developer",
            agent_manifest_sha256="a" * 64,
        )
    ]
    container = jobs.created[0][1]["spec"]["template"]["spec"]["containers"][0]
    assert next(env["value"] for env in container["env"] if env["name"] == "ANTHROPIC_MODEL") == "balanced"


async def test_execution_workspace_rejects_a_gateway_not_matching_the_pinned_registration() -> None:
    service = ExecutionWorkspaceService(execution_settings(), InMemoryExecutionJobClient())
    gateway = AgentGatewayResolution(
        policy_revision="agent_gateway_initial",
        project_id="default",
        role="developer",
        registration_id="developer",
        registration_version="99.0.0",
        manifest_sha256="b" * 64,
        model_alias="unapproved-model",
        max_budget_usd=100.0,
        toolset="development-restricted",
    )

    with pytest.raises(ValueError, match="pinned developer gateway route"):
        await service.provision(
            ExecutionRequest(
                run_id="run-1",
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                target_repos=[],
                registration=RegistrationReference(
                    role="developer",
                    registration_id="developer",
                    version="1.0.0",
                    manifest_sha256="a" * 64,
                    component_id="developer",
                    component_version="1.0.0",
                ),
                gateway=gateway,
            )
        )


async def test_execution_workspace_rejects_a_resolved_developer_without_a_gateway() -> None:
    service = ExecutionWorkspaceService(execution_settings(), InMemoryExecutionJobClient())

    with pytest.raises(ValueError, match="pinned developer gateway route"):
        await service.provision(
            ExecutionRequest(
                run_id="run-1",
                spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
                target_repos=[],
                registration=RegistrationReference(
                    role="developer",
                    registration_id="developer",
                    version="1.0.0",
                    manifest_sha256="a" * 64,
                    component_id="developer",
                    component_version="1.0.0",
                ),
            )
        )


def test_mcp_invocation_evidence_retains_only_pinned_grant_counts() -> None:
    grant = McpToolGrant(
        server_id="cogito_readonly_mcp",
        server_version="1.0.1",
        server_manifest_sha256="b" * 64,
        tool_name="catalog_read",
        input_schema_sha256="c" * 64,
    )
    expected = _expected_mcp_tools([grant], {("cogito_readonly_mcp", "1.0.1"): "cogito_readonly"})

    evidence = _mcp_invocation_evidence(
        [
            {"mcp_namespaced_tool_name": [], "status": "success"},
            {
                "mcp_namespaced_tool_name": "cogito_readonly/catalog_read",
                "status": "success",
                "messages": "raw prompt",
                "response": "raw tool output",
                "api_key": "raw-key",
            },
            {"mcp_namespaced_tool_name": "cogito_readonly/catalog_read", "status": "success"},
            {"mcp_namespaced_tool_name": "unapproved/delete", "status": "success"},
        ],
        expected,
    )

    assert evidence == {
        "version": 1,
        "status": "observed",
        "events": [
            {
                "server_id": "cogito_readonly_mcp",
                "server_version": "1.0.1",
                "server_manifest_sha256": "b" * 64,
                "tool_name": "catalog_read",
                "input_schema_sha256": "c" * 64,
                "outcome": "success",
                "invocation_count": 2,
            }
        ],
    }


def test_mcp_invocation_evidence_rejects_malformed_gateway_responses() -> None:
    with pytest.raises(ValueError, match="response must be a list"):
        _mcp_invocation_evidence({}, {})


def test_mcp_invocation_evidence_bounds_untrusted_outcome_variants() -> None:
    grant = McpToolGrant(
        server_id="cogito_readonly_mcp",
        server_version="1.0.1",
        server_manifest_sha256="b" * 64,
        tool_name="catalog_read",
        input_schema_sha256="c" * 64,
    )
    expected = _expected_mcp_tools([grant], {("cogito_readonly_mcp", "1.0.1"): "cogito_readonly"})

    evidence = _mcp_invocation_evidence(
        [{"mcp_namespaced_tool_name": "cogito_readonly/catalog_read", "status": "success"}]
        + [
            {"mcp_namespaced_tool_name": "cogito_readonly/catalog_read", "status": f"untrusted-{index}"}
            for index in range(200)
        ],
        expected,
    )

    assert [(event["outcome"], event["invocation_count"]) for event in evidence["events"]] == [
        ("failure", 200),
        ("success", 1),
    ]


def test_mcp_invocation_evidence_rejects_oversized_gateway_responses(monkeypatch: pytest.MonkeyPatch) -> None:
    class Response:
        status = 200
        headers = {"Content-Length": str(budgets._MCP_INVOCATION_EVIDENCE_MAX_RESPONSE_BYTES + 1)}

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            raise AssertionError("oversized responses must not be read")

    monkeypatch.setattr(budgets, "urlopen", lambda *args, **kwargs: Response())
    manager = object.__new__(KubernetesLiteLLMRunKeyManager)
    manager._endpoint = "http://cogito-litellm:4000"
    manager._management_key = "test-management-key"

    with pytest.raises(ValueError, match="maximum size"):
        manager._get_json("/spend/logs?user_id=cogito-run")


async def test_mcp_invocation_evidence_uses_a_versioned_not_applicable_schema() -> None:
    manager = object.__new__(KubernetesLiteLLMRunKeyManager)

    evidence = await manager.collect_mcp_invocations("run-1", run_key_secret_name("run-1"), [], {})

    assert evidence == {"version": 1, "status": "not_applicable", "events": []}


@pytest.mark.parametrize(
    ("permissions", "message"),
    [
        ([], "must be a mapping"),
        ({1: ("catalog_read",)}, "gateway server IDs"),
        ({"a" * 32: "catalog_read"}, "explicit unique tool names"),
        ({"a" * 32: {"catalog_read"}}, "explicit unique tool names"),
        ({"a" * 32: (1,)}, "explicit unique tool names"),
    ],
)
def test_run_key_budget_rejects_malformed_mcp_permissions(permissions: object, message: str) -> None:
    budget = RunBudget("run-1", 2.5, "complex", 120, permissions)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=message):
        _validate_budget(budget)


async def test_provisioning_removes_a_job_when_its_pod_never_becomes_active() -> None:
    """A failed startup does not leak a Job or its future workspace."""

    class FailingExecutionJobClient(InMemoryExecutionJobClient):
        async def wait_until_ready(self, job_name: str, timeout_seconds: int) -> None:
            await super().wait_until_ready(job_name, timeout_seconds)
            raise TimeoutError("execution pod did not start")

    jobs = FailingExecutionJobClient()
    service = ExecutionWorkspaceService(
        execution_settings(),
        jobs,
    )

    with pytest.raises(TimeoutError, match="did not start"):
        await service.provision(ExecutionRequest(run_id="run-1", spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64, target_repos=[]))

    assert jobs.deleted == [execution_job_name("run-1")]


async def test_provisioned_run_key_is_scoped_to_one_budget_and_execution_pod() -> None:
    class RecordingRunKeys:
        def __init__(self) -> None:
            self.budgets: list[RunBudget] = []
            self.cleaned: list[tuple[str, str]] = []

        async def provision(self, budget: RunBudget) -> str:
            self.budgets.append(budget)
            return "cogito-run-key-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            self.cleaned.append((run_id, secret_name))

    class RecordingRunGitCredentials:
        def __init__(self) -> None:
            self.provisioned: list[str] = []
            self.cleaned: list[tuple[str, str]] = []

        async def provision(self, run_id: str) -> str:
            self.provisioned.append(run_id)
            return "cogito-run-git-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            self.cleaned.append((run_id, secret_name))

    jobs = InMemoryExecutionJobClient()
    run_keys = RecordingRunKeys()
    run_git_credentials = RecordingRunGitCredentials()
    service = ExecutionWorkspaceService(execution_settings(), jobs, run_keys, run_git_credentials)
    request = ExecutionRequest(
        run_id="run-1",
        spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
        target_repos=[],
        execution_timeout_seconds=120,
        max_cost_usd=2.5,
    )

    workspace = await service.provision(request)
    await service.cleanup(workspace)

    assert run_keys.budgets == [RunBudget("run-1", 2.5, "complex", 120)]
    assert workspace.run_key_secret == "cogito-run-key-abc"
    assert workspace.run_git_secret == "cogito-run-git-abc"
    pod_container = jobs.created[0][1]["spec"]["template"]["spec"]["containers"][0]
    key_ref = next(env["valueFrom"] for env in pod_container["env"] if env["name"] == "ANTHROPIC_AUTH_TOKEN")
    assert key_ref["secretKeyRef"]["name"] == "cogito-run-key-abc"
    git_ref = next(env["valueFrom"] for env in pod_container["env"] if env["name"] == "COGITO_GIT_HTTPS_TOKEN")
    assert git_ref["secretKeyRef"]["name"] == "cogito-run-git-abc"
    assert run_keys.cleaned == [("run-1", "cogito-run-key-abc")]
    assert run_git_credentials.provisioned == ["run-1"]
    assert run_git_credentials.cleaned == [("run-1", "cogito-run-git-abc")]


async def test_provisioned_run_key_maps_only_pinned_mcp_grants_to_gateway_identity() -> None:
    class RecordingRunKeys:
        def __init__(self) -> None:
            self.budgets: list[RunBudget] = []

        async def provision(self, budget: RunBudget) -> str:
            self.budgets.append(budget)
            return "cogito-run-key-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            return None

    gateway_server_id = "a" * 32
    settings = replace(
        execution_settings(),
        mcp_gateway_servers={
            "cogito_readonly_mcp@1.0.1": McpGatewayServer(
                gateway_server_id=gateway_server_id,
                route="cogito_readonly",
                server_manifest_sha256="b" * 64,
                tool_names=("catalog_read",),
            )
        },
    )
    run_keys = RecordingRunKeys()
    service = ExecutionWorkspaceService(settings, InMemoryExecutionJobClient(), run_keys)
    request = ExecutionRequest(
        run_id="run-1",
        spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
        target_repos=[],
        execution_timeout_seconds=120,
        max_cost_usd=2.5,
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

    workspace = await service.provision(request)
    await service.cleanup(workspace)

    assert run_keys.budgets[0].mcp_tool_permissions == {gateway_server_id: ("catalog_read",)}


async def test_mcp_invocation_collection_is_bounded_to_workspace_grants() -> None:
    class RecordingRunKeys:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str, list[McpToolGrant], dict[tuple[str, str], str]]] = []

        async def collect_mcp_invocations(
            self,
            run_id: str,
            secret_name: str,
            grants: list[McpToolGrant],
            routes: dict[tuple[str, str], str],
        ) -> dict[str, object]:
            self.calls.append((run_id, secret_name, grants, routes))
            return {"version": 1, "status": "observed", "events": []}

    grant = McpToolGrant(
        server_id="cogito_readonly_mcp",
        server_version="1.0.1",
        server_manifest_sha256="b" * 64,
        tool_name="catalog_read",
        input_schema_sha256="c" * 64,
    )
    run_keys = RecordingRunKeys()
    service = ExecutionWorkspaceService(execution_settings(), InMemoryExecutionJobClient(), run_keys)
    workspace = ExecutionWorkspace(
        run_id="run-1",
        job_name="cogito-execution-example",
        workspace_root="/workspace",
        run_key_secret="cogito-run-key-example",
        mcp_grants=[grant],
        mcp_servers=[
            McpServerConfiguration(
                registration_id="cogito_readonly_mcp",
                server_version="1.0.1",
                name="cogito_readonly",
                url="http://cogito-litellm:4000/cogito_readonly/mcp",
            )
        ],
    )

    evidence = await service.collect_mcp_invocations(workspace)

    assert evidence == {"version": 1, "status": "observed", "events": []}
    assert run_keys.calls == [
        (
            "run-1",
            "cogito-run-key-example",
            [grant],
            {("cogito_readonly_mcp", "1.0.1"): "cogito_readonly"},
        )
    ]


async def test_mcp_invocation_collection_never_interrupts_an_execution_run() -> None:
    class FailingRunKeys:
        async def collect_mcp_invocations(self, *args: object) -> dict[str, object]:
            raise RuntimeError("gateway audit unavailable")

    service = ExecutionWorkspaceService(execution_settings(), InMemoryExecutionJobClient(), FailingRunKeys())
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

    assert await service.collect_mcp_invocations(workspace) == {
        "version": 1,
        "status": "unavailable",
        "reason": "collector_configuration_invalid",
        "events": [],
    }


async def test_mcp_invocation_collection_excludes_unversioned_historic_routes() -> None:
    class RecordingRunKeys:
        async def collect_mcp_invocations(
            self, run_id: str, secret_name: str, grants: list[McpToolGrant], routes: dict[tuple[str, str], str]
        ) -> dict[str, object]:
            assert routes == {}
            return {"version": 1, "status": "observed", "events": []}

    service = ExecutionWorkspaceService(execution_settings(), InMemoryExecutionJobClient(), RecordingRunKeys())
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
        mcp_servers=[
            McpServerConfiguration(
                registration_id="cogito_readonly_mcp",
                name="cogito_readonly",
                url="http://cogito-litellm:4000/cogito_readonly/mcp",
            )
        ],
    )

    assert await service.collect_mcp_invocations(workspace) == {"version": 1, "status": "observed", "events": []}


async def test_provision_rejects_an_mcp_grant_that_does_not_match_the_configured_release() -> None:
    settings = replace(
        execution_settings(),
        mcp_gateway_servers={
            "cogito_readonly_mcp@1.0.1": McpGatewayServer(
                gateway_server_id="a" * 32,
                route="cogito_readonly",
                server_manifest_sha256="b" * 64,
                tool_names=("catalog_read",),
            )
        },
    )
    service = ExecutionWorkspaceService(settings, InMemoryExecutionJobClient())
    request = ExecutionRequest(
        run_id="run-1",
        spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
        target_repos=[],
        mcp_grants=[
            McpToolGrant(
                server_id="cogito_readonly_mcp",
                server_version="1.0.0",
                server_manifest_sha256="c" * 64,
                tool_name="catalog_read",
                input_schema_sha256="d" * 64,
            )
        ],
    )

    with pytest.raises(ValueError, match="not configured"):
        await service.provision(request)



async def test_cleanup_revokes_run_credentials_when_job_deletion_fails() -> None:
    class FailingDeleteExecutionJobClient(InMemoryExecutionJobClient):
        async def delete_job(self, job_name: str) -> None:
            await super().delete_job(job_name)
            raise RuntimeError("Kubernetes API unavailable")

    class RecordingRunKeys:
        def __init__(self) -> None:
            self.cleaned: list[tuple[str, str]] = []

        async def provision(self, budget: RunBudget) -> str:
            return "cogito-run-key-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            self.cleaned.append((run_id, secret_name))

    class RecordingRunGitCredentials:
        def __init__(self) -> None:
            self.cleaned: list[tuple[str, str]] = []

        async def provision(self, run_id: str) -> str:
            return "cogito-run-git-abc"

        async def cleanup(self, run_id: str, secret_name: str) -> None:
            self.cleaned.append((run_id, secret_name))

    jobs = FailingDeleteExecutionJobClient()
    run_keys = RecordingRunKeys()
    run_git_credentials = RecordingRunGitCredentials()
    service = ExecutionWorkspaceService(execution_settings(), jobs, run_keys, run_git_credentials)
    workspace = await service.provision(
        ExecutionRequest(
            run_id="run-1",
            spec_ref="typescript-backend@v2.1#sha256=" + "a" * 64,
            target_repos=[],
            execution_timeout_seconds=120,
            max_cost_usd=2.5,
        )
    )

    with pytest.raises(RuntimeError, match="Kubernetes API unavailable"):
        await service.cleanup(workspace)

    assert run_keys.cleaned == [("run-1", "cogito-run-key-abc")]
    assert run_git_credentials.cleaned == [("run-1", "cogito-run-git-abc")]


async def test_kubernetes_exec_pipes_stdin_so_the_remote_command_receives_eof() -> None:
    client = object.__new__(KubernetesExecutionJobClient)
    captured: dict[str, object] = {}

    async def running_pod_name(job_name: str) -> str:
        assert job_name == "job-1"
        return "pod-1"

    def execute_in_pod(pod_name: str, command: list[str], stdin: str, timeout: int, limit: int):
        captured.update(pod_name=pod_name, command=command, stdin=stdin, timeout=timeout, limit=limit)
        return type("Result", (), {"exit_code": 0, "stdout": "", "stderr": ""})()

    client._running_pod_name = running_pod_name
    client._execute_in_pod = execute_in_pod

    await client.execute("job-1", ["claude", "--print"], "approved prompt", 30, 1024)

    assert captured["stdin"] == ""
    assert captured["command"] == ["/bin/sh", "-lc", "printf '%s' 'approved prompt' | exec claude --print"]


def test_execution_failure_diagnostics_redact_sensitive_values() -> None:
    output = _sanitize_diagnostics(
        "b'MINIO_SECRET_KEY=super-secret token=abc123 {\\\"ANTHROPIC_AUTH_TOKEN\\\":\\\"gateway-key\\\"} "
        "Authorization: Bearer bearer-key\\nworkspace failed'"
    )

    assert "super-secret" not in output
    assert "abc123" not in output
    assert "gateway-key" not in output
    assert "bearer-key" not in output
    assert "[REDACTED]" in output


def test_streamed_command_output_stays_within_its_memory_budget() -> None:
    parts: list[str] = []
    size, truncated = _append_bounded_output(parts, "x" * 10, 0, 4, False)
    size, truncated = _append_bounded_output(parts, "more", size, 4, truncated)

    assert size == 4
    assert truncated is True
    assert _bounded_output(parts, 4, truncated) == "xxxx\n[output truncated]"


def test_kubernetes_exec_stream_does_not_require_a_nonexistent_close_stdin_method() -> None:
    class Response:
        returncode = 0

        def __init__(self) -> None:
            self.written: list[str] = []
            self.open = True

        def write_stdin(self, value: str) -> None:
            self.written.append(value)

        def is_open(self) -> bool:
            return self.open

        def update(self, timeout: int) -> None:
            self.open = False

        def read_stdout(self) -> str:
            return "ok"

        def read_stderr(self) -> str:
            return ""

        def close(self) -> None:
            pass

    response = Response()
    client = object.__new__(KubernetesExecutionJobClient)
    client._core_api = type("CoreApi", (), {"connect_get_namespaced_pod_exec": object()})()
    client._namespace = "cogito-executions"
    client._stream = lambda *args, **kwargs: response

    result = client._execute_in_pod("test-pod", ["echo", "ok"], "prompt", 5, 1024)

    assert response.written == ["prompt"]
    assert result.exit_code == 0
    assert result.stdout == "ok"
