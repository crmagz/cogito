from __future__ import annotations

import hashlib
import json
import re
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import TimeoutError

with workflow.unsafe.imports_passed_through():
    from .activities import WorkerActivities
    from .models import (
        BackupExecutionRequest,
        ExecutionRequest,
        ImplementationArtifact,
        McpToolGrant,
        PhaseExecutionRequest,
        PhaseResult,
        PlanPhase,
        ReviewRequest,
        ReviewRevisionRequest,
        RunEnvelope,
        RunResult,
        ValidationRequest,
    )
    from .registry import require_role, require_tool

_ACTIVITY_TIMEOUT = timedelta(seconds=30)
_WORKER_START_TIMEOUT = timedelta(seconds=60)
# Workspace provisioning includes pod scheduling, repository preparation, and
# the operator-configured execution startup allowance. It cannot share the
# short status/load activity timeout or Temporal will cancel it first.
_PROVISION_ACTIVITY_TIMEOUT = timedelta(seconds=180)
_CLEANUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)
_RETRY_INITIAL_INTERVAL = timedelta(seconds=1)
_RETRY_MAXIMUM_INTERVAL = timedelta(seconds=30)
_IDEMPOTENT_RETRY_POLICY = RetryPolicy(
    initial_interval=_RETRY_INITIAL_INTERVAL,
    backoff_coefficient=2.0,
    maximum_interval=_RETRY_MAXIMUM_INTERVAL,
    maximum_attempts=3,
)
_PROVISION_RETRY_POLICY = _IDEMPOTENT_RETRY_POLICY
_CLEANUP_RETRY_POLICY = _IDEMPOTENT_RETRY_POLICY
_PUBLISH_RETRY_POLICY = _IDEMPOTENT_RETRY_POLICY
_RUN_PHASE_RETRY_POLICY = RetryPolicy(maximum_attempts=1)
_BACKUP_PHASE_RETRY_POLICY = _IDEMPOTENT_RETRY_POLICY
_BACKUP_ACTIVITY_TIMEOUT = timedelta(seconds=120)
_REVIEW_ACTIVITY_TIMEOUT = timedelta(seconds=120)


@workflow.defn
class DeveloperRunWorkflow:
    def __init__(self) -> None:
        self._awaiting_plan_approval = False
        self._plan_sha256 = ""
        self._plan_decision: dict[str, Any] | None = None
        self._awaiting_implementation_approval = False
        self._implementation_sha256 = ""
        self._implementation_decision: dict[str, str] | None = None
        self._processed_plan_decision_ids: set[str] = set()
        self._processed_implementation_decision_ids: set[str] = set()
        self._pinned_mcp_selection_keys: set[tuple[str, str, str, str, str, str, str]] = set()

    @workflow.update
    async def submit_plan_approval(self, decision: dict[str, Any]) -> bool:
        """Accept legacy plan decisions that do not alter the pinned MCP grant set."""

        if decision.get("mcp_selection") is not None:
            return False
        return self._accept_plan_approval(decision)

    @workflow.update
    async def submit_plan_approval_with_mcp_selection(self, decision: dict[str, Any]) -> bool:
        """Accept a narrowing decision only on workers that explicitly support it."""

        if decision.get("mcp_selection") is None:
            return False
        return self._accept_plan_approval(decision)

    def _accept_plan_approval(self, decision: dict[str, Any]) -> bool:
        """Validate and record one idempotent decision while this workflow waits."""

        decision_id = decision.get("decision_id", "")
        if not decision_id:
            return False
        if decision_id in self._processed_plan_decision_ids:
            # The control-plane outbox can retry after Temporal has accepted
            # the update but before it records delivery. A repeated durable
            # decision ID is therefore an acknowledgement, never a new vote.
            return True
        if not self._awaiting_plan_approval:
            return False
        if decision.get("artifact_sha256") != self._plan_sha256:
            return False
        if decision.get("decision") not in {"approve", "reject", "request_revision"}:
            return False
        if decision.get("decision") != "approve" and decision.get("mcp_selection") is not None:
            return False
        if not _is_pinned_mcp_selection(decision.get("mcp_selection"), self._pinned_mcp_selection_keys):
            return False
        self._processed_plan_decision_ids.add(decision_id)
        self._plan_decision = decision
        return True

    @workflow.update
    async def submit_implementation_approval(self, decision: dict[str, str]) -> bool:
        """Accept a decision only for the exact frozen implementation artifact."""

        decision_id = decision.get("decision_id", "")
        if not decision_id:
            return False
        if decision_id in self._processed_implementation_decision_ids:
            return True
        if not self._awaiting_implementation_approval:
            return False
        if decision.get("artifact_sha256") != self._implementation_sha256:
            return False
        if decision.get("decision") not in {"approve", "reject", "request_revision"}:
            return False
        self._processed_implementation_decision_ids.add(decision_id)
        self._implementation_decision = decision
        return True

    @workflow.run
    async def run(self, envelope: RunEnvelope) -> RunResult:
        try:
            planner_registration = require_role(envelope, "planner")
            require_tool(planner_registration, "planning_model", "plan_generation")
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "claimed"],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            plan = await workflow.execute_activity(
                WorkerActivities.load_plan,
                args=[envelope.plan_ref],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            _validate_plan_snapshot(plan, envelope)
            (
                phases,
                productive_turns,
                run_timeout,
                backup_reserve_turns,
                max_cost_usd,
                max_review_rounds,
                review_profile,
            ) = _execution_plan(plan)
            self._pinned_mcp_selection_keys = {
                _mcp_selection_key(resolution.role, grant)
                for resolution in envelope.registry_resolutions
                if resolution.role == "developer"
                for grant in resolution.mcp_grants
            }
            if envelope.requires_plan_approval:
                self._plan_sha256 = envelope.plan_sha256
                self._awaiting_plan_approval = True
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "awaiting_plan_approval"],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                await workflow.wait_condition(lambda: self._plan_decision is not None)
                self._awaiting_plan_approval = False
                assert self._plan_decision is not None
                decision = self._plan_decision["decision"]
                if decision == "reject":
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[envelope.run_id, "rejected"],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    return RunResult(run_id=envelope.run_id, status="rejected")
                if decision == "request_revision":
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[envelope.run_id, "revision_requested"],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    return RunResult(
                        run_id=envelope.run_id, status="revision_requested"
                    )
            # A resolved registry run must authorize the developer before it
            # creates a workspace or receives a developer-tool capability.
            # Legacy envelopes remain supported while migration is active.
            developer_registration = require_role(envelope, "developer")
            require_tool(developer_registration, "execution_workspace", "run_scoped_workspace")
            require_tool(developer_registration, "developer_harness", "approved_phase")
            approved_selection = self._plan_decision.get("mcp_selection") if self._plan_decision is not None else None
            developer_mcp_grants = _narrow_mcp_grants(
                developer_registration.mcp_grants if developer_registration is not None else [],
                "developer",
                approved_selection,
            )
            # Report execution before potentially slow workspace provisioning
            # so the control plane does not label active work as queued.
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "implementing"],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_WORKER_START_TIMEOUT,
            )
            workspace = await workflow.execute_activity(
                WorkerActivities.provision_execution_workspace,
                args=[
                    ExecutionRequest(
                        run_id=envelope.run_id,
                        spec_ref=envelope.spec_ref,
                        target_repos=envelope.target_repos,
                        execution_timeout_seconds=(
                            int(run_timeout.total_seconds())
                            + int(_BACKUP_ACTIVITY_TIMEOUT.total_seconds())
                        ),
                        max_cost_usd=max_cost_usd,
                        registration=developer_registration,
                        gateway=developer_registration.gateway if developer_registration is not None else None,
                        mcp_grants=developer_mcp_grants,
                        mcp_selection_explicit=approved_selection is not None,
                    )
                ],
                start_to_close_timeout=_PROVISION_ACTIVITY_TIMEOUT,
                schedule_to_start_timeout=_WORKER_START_TIMEOUT,
                retry_policy=_PROVISION_RETRY_POLICY,
            )
            completed_phase_ids: list[str] = []
            phase_results: list[dict] = []
            stopped_phase: tuple[PlanPhase, PhaseResult] | None = None
            review_outcome: dict | None = None
            implementation_artifact: ImplementationArtifact | None = None
            implementation_evidence: dict | None = None
            try:
                deadline = workflow.now() + run_timeout
                for phase in phases:
                    remaining = deadline - workflow.now()
                    if remaining <= timedelta():
                        phase_result = await _backup_phase(
                            phase, workspace, "wall_clock"
                        )
                    else:
                        try:
                            phase_result = await workflow.execute_activity(
                                WorkerActivities.run_phase,
                                args=[
                                    PhaseExecutionRequest(
                                        phase=phase,
                                        workspace=workspace,
                                        max_turns=productive_turns,
                                        timeout_seconds=max(
                                            1, int(remaining.total_seconds()) - 1
                                        ),
                                        backup_reserve_turns=backup_reserve_turns,
                                        traceparent=envelope.traceparent,
                                        tracestate=envelope.tracestate,
                                    )
                                ],
                                start_to_close_timeout=remaining,
                                retry_policy=_RUN_PHASE_RETRY_POLICY,
                            )
                        except Exception as error:
                            if not _is_timeout_error(error):
                                raise
                            phase_result = await _backup_phase(
                                phase, workspace, "wall_clock"
                            )
                        if phase_result.outcome == "ceiling_reached":
                            phase_result = await _backup_phase(
                                phase, workspace, phase_result.ceiling or "unknown"
                            )
                    phase_results.append(phase_result.metadata())
                    if phase_result.outcome == "stopped_with_backup":
                        # Do not record a successful terminal backup state until
                        # the workspace has been removed. If cleanup fails, the
                        # outer handler records one unambiguous failed outcome.
                        stopped_phase = (phase, phase_result)
                        break
                    if phase_result.succeeded:
                        completed_phase_ids.append(phase.id)
                    await workflow.execute_activity(
                        WorkerActivities.report_status,
                        args=[
                            envelope.run_id,
                            "phase_complete"
                            if phase_result.succeeded
                            else "phase_failed",
                            None,
                            {
                                "phase_results": phase_results,
                                "completed_phase_ids": completed_phase_ids,
                            },
                        ],
                        start_to_close_timeout=_ACTIVITY_TIMEOUT,
                    )
                    if not phase_result.succeeded:
                        raise RuntimeError(
                            f"phase {phase.id} failed: {phase_result.summary}"
                        )
                if stopped_phase is None:
                    review_outcome = await _review_implementation(
                        envelope,
                        workspace,
                        phases,
                        phase_results,
                        productive_turns,
                        deadline,
                        max_review_rounds,
                        review_profile,
                    )
                    if review_outcome["status"] == "converged":
                        validator_registration = require_role(envelope, "validator")
                        validation = None
                        if validator_registration is not None:
                            require_tool(validator_registration, "validation_runner", "approved_verification")
                            validation = await workflow.execute_activity(
                                WorkerActivities.validate_implementation,
                                args=[
                                    ValidationRequest(
                                        run_id=envelope.run_id,
                                        phase_results=phase_results,
                                        review=review_outcome,
                                    )
                                ],
                                start_to_close_timeout=_ACTIVITY_TIMEOUT,
                                retry_policy=_RUN_PHASE_RETRY_POLICY,
                            )
                            if validation.status != "passed":
                                raise RuntimeError(f"validation gate failed: {validation.reason or 'unknown'}")
                        implementation_evidence = _implementation_evidence(
                            envelope, workspace, phase_results, review_outcome, validation
                        )
                        implementation_artifact = await workflow.execute_activity(
                            WorkerActivities.freeze_implementation_artifact,
                            args=[
                                envelope.run_id,
                                implementation_evidence,
                                workspace,
                            ],
                            start_to_close_timeout=_ACTIVITY_TIMEOUT,
                        )
            finally:
                await workflow.execute_activity(
                    WorkerActivities.cleanup_execution_workspace,
                    args=[workspace],
                    start_to_close_timeout=_CLEANUP_ACTIVITY_TIMEOUT,
                    retry_policy=_CLEANUP_RETRY_POLICY,
                )
            if stopped_phase is not None:
                phase, phase_result = stopped_phase
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[
                        envelope.run_id,
                        "stopped_with_backup",
                        None,
                        {
                            "phase_results": phase_results,
                            "completed_phase_ids": completed_phase_ids,
                            "stopped_phase_id": phase.id,
                            "unfinished_phase_ids": [
                                candidate.id
                                for candidate in phases
                                if candidate.id not in completed_phase_ids
                            ],
                            "branch_name": phase_result.branch_name,
                            "ceiling": phase_result.ceiling,
                        },
                    ],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                return RunResult(run_id=envelope.run_id, status="stopped_with_backup")
            if review_outcome is not None and review_outcome["status"] == "escalated":
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "escalated", None, {"review": review_outcome}],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                return RunResult(run_id=envelope.run_id, status="escalated")
            if not envelope.requires_implementation_approval:
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "completed", None, {"review": review_outcome}],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                return RunResult(run_id=envelope.run_id, status="completed")
            if implementation_artifact is None:
                raise RuntimeError("converged review did not produce an implementation artifact")
            assert implementation_evidence is not None
            self._implementation_sha256 = implementation_artifact.sha256
            self._awaiting_implementation_approval = True
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[
                    envelope.run_id,
                    "awaiting_implementation_approval",
                    None,
                    {
                        "implementation_artifact": {
                            "ref": implementation_artifact.ref,
                            "sha256": implementation_artifact.sha256,
                        },
                        "review": implementation_evidence["review"],
                    },
                ],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            await workflow.wait_condition(lambda: self._implementation_decision is not None)
            self._awaiting_implementation_approval = False
            assert self._implementation_decision is not None
            decision = self._implementation_decision["decision"]
            if decision == "reject":
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "rejected", None, {"implementation_artifact": {"sha256": implementation_artifact.sha256}}],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                return RunResult(run_id=envelope.run_id, status="rejected")
            if decision == "request_revision":
                await workflow.execute_activity(
                    WorkerActivities.report_status,
                    args=[envelope.run_id, "revision_requested", None, {"implementation_artifact": {"sha256": implementation_artifact.sha256}}],
                    start_to_close_timeout=_ACTIVITY_TIMEOUT,
                )
                return RunResult(run_id=envelope.run_id, status="revision_requested")
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "finalizing", None, {"implementation_artifact": {"sha256": implementation_artifact.sha256}}],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            publisher_registration = require_role(envelope, "pull_request_publisher")
            require_tool(publisher_registration, "github_publisher", "approved_pull_request")
            pull_request = await workflow.execute_activity(
                WorkerActivities.open_pull_request,
                args=[implementation_artifact.sha256, implementation_evidence],
                start_to_close_timeout=_REVIEW_ACTIVITY_TIMEOUT,
                retry_policy=_PUBLISH_RETRY_POLICY,
            )
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[
                    envelope.run_id,
                    "completed",
                    None,
                    {
                        "review": implementation_evidence["review"],
                        "pull_request": {
                            "number": pull_request.number,
                            "url": pull_request.url,
                            "reused": pull_request.reused,
                        },
                        "implementation_artifact": {"sha256": implementation_artifact.sha256},
                    },
                ],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            return RunResult(run_id=envelope.run_id, status="completed")
        except Exception as error:  # noqa: BLE001 - Temporal wraps activity failures variably.
            # If no worker can consume the first implementation activity, a
            # status activity cannot repair the projection either. Return the
            # terminal outcome so the API reconciler records a durable failure
            # instead of leaving the handoff queued forever.
            if _is_timeout_error(error):
                return RunResult(run_id=envelope.run_id, status="failed")
            await workflow.execute_activity(
                WorkerActivities.report_status,
                args=[envelope.run_id, "failed", _failure_detail(error)],
                start_to_close_timeout=_ACTIVITY_TIMEOUT,
            )
            # A failed run is a durable terminal business outcome. Returning
            # prevents Temporal from replaying this workflow task indefinitely
            # after the status has already been recorded as failed.
            return RunResult(run_id=envelope.run_id, status="failed")


def _implementation_evidence(envelope: RunEnvelope, workspace, phase_results: list[dict], review: dict, validation=None) -> dict:
    """Build compact, canonical approval evidence without source, prompts, or raw diffs."""

    commits: dict[str, str] = {}
    branch_name = ""
    turns_used = 0
    cost_usd = 0.0
    models: set[str] = set()
    safe_phase_results: list[dict] = []
    for phase in phase_results:
        if not isinstance(phase, dict):
            continue
        if not branch_name and isinstance(phase.get("branch_name"), str):
            branch_name = phase["branch_name"]
        phase_commits = phase.get("commits")
        if isinstance(phase_commits, dict):
            commits.update({key: value for key, value in phase_commits.items() if isinstance(key, str) and isinstance(value, str)})
        if isinstance(phase.get("turns_used"), int):
            turns_used += phase["turns_used"]
        if isinstance(phase.get("cost_usd"), (int, float)):
            cost_usd += float(phase["cost_usd"])
        safe_phase_results.append(
            {
                key: phase[key]
                for key in ("phase_id", "branch_name", "succeeded", "turns_used", "cost_usd", "changed_files", "commits", "outcome", "ceiling")
                if key in phase
            }
            | {
                "verification": [
                    {"command": item.get("command"), "passed": item.get("passed")}
                    for item in phase.get("verification", [])
                    if isinstance(item, dict)
                ]
            }
        )
    rounds = review.get("rounds") if isinstance(review.get("rounds"), list) else []
    for round_evidence in rounds:
        if not isinstance(round_evidence, dict):
            continue
        findings = round_evidence.get("findings")
        if not isinstance(findings, list):
            continue
        for finding in findings:
            if isinstance(finding, dict) and isinstance(finding.get("model"), str):
                models.add(finding["model"])
        revision = round_evidence.get("revision")
        if isinstance(revision, dict) and isinstance(revision.get("commits"), dict):
            commits.update(
                {key: value for key, value in revision["commits"].items() if isinstance(key, str) and isinstance(value, str)}
            )
    return {
        "version": 1,
        "run_id": envelope.run_id,
        "plan_sha256": envelope.plan_sha256,
        "branch_name": branch_name,
        "commits": dict(sorted(commits.items())),
        "repository_origin": next(iter(workspace.repository_origins.values()), ""),
        "phase_results": safe_phase_results,
        "review": _safe_review_evidence(review),
        "validation": {
            "status": validation.status,
            "checked_phases": validation.checked_phases,
            "reason": validation.reason,
        }
        if validation is not None
        else None,
        "registry_resolutions": [
            {
                "role": resolution.role,
                "registration_id": resolution.registration_id,
                "version": resolution.version,
                "manifest_sha256": resolution.manifest_sha256,
                "component_id": resolution.component_id,
                "component_version": resolution.component_version,
                "grants": [
                    {
                        "tool_id": grant.tool_id,
                        "tool_version": grant.tool_version,
                        "scope": grant.scope,
                    }
                    for grant in resolution.grants
                ],
                "mcp_grants": [
                    {
                        "server_id": grant.server_id,
                        "server_version": grant.server_version,
                        "server_manifest_sha256": grant.server_manifest_sha256,
                        "tool_name": grant.tool_name,
                        "input_schema_sha256": grant.input_schema_sha256,
                        "repository_scope": grant.repository_scope,
                    }
                    for grant in resolution.mcp_grants
                ],
                "gateway": (
                    {
                        "policy_revision": resolution.gateway.policy_revision,
                        "project_id": resolution.gateway.project_id,
                        "role": resolution.gateway.role,
                        "registration_id": resolution.gateway.registration_id,
                        "registration_version": resolution.gateway.registration_version,
                        "manifest_sha256": resolution.gateway.manifest_sha256,
                        "model_alias": resolution.gateway.model_alias,
                        "max_budget_usd": resolution.gateway.max_budget_usd,
                        "toolset": resolution.gateway.toolset,
                    }
                    if resolution.gateway is not None
                    else None
                ),
            }
            for resolution in envelope.registry_resolutions
        ],
        "models": sorted(models),
        "turns_used": turns_used,
        "cost_usd": round(cost_usd, 6),
    }


def _mcp_selection_key(role: str, grant: McpToolGrant) -> tuple[str, str, str, str, str, str, str]:
    """Return an exact run-pinned MCP identity, including release and schema digests."""

    return (
        role,
        grant.server_id,
        grant.server_version,
        grant.server_manifest_sha256,
        grant.tool_name,
        grant.input_schema_sha256,
        grant.repository_scope or "",
    )


def _selection_key_from_value(value: object) -> tuple[str, str, str, str, str, str, str] | None:
    """Parse only the exact non-secret identity allowed in an approval update."""

    base_fields = {
        "role",
        "server_id",
        "server_version",
        "server_manifest_sha256",
        "tool_name",
        "input_schema_sha256",
    }
    if not isinstance(value, dict) or set(value) not in (base_fields, base_fields | {"repository_scope"}):
        return None
    fields = ("role", "server_id", "server_version", "server_manifest_sha256", "tool_name", "input_schema_sha256")
    if not all(isinstance(value[field], str) for field in fields):
        return None
    repository_scope = value.get("repository_scope")
    if repository_scope is not None and not isinstance(repository_scope, str):
        return None
    return (*tuple(value[field] for field in fields), repository_scope or "")  # type: ignore[return-value]


def _is_pinned_mcp_selection(
    selection: object, pinned: set[tuple[str, str, str, str, str, str, str]]
) -> bool:
    """Accept only a duplicate-free subset of the immutable envelope grant pins."""

    if selection is None:
        return True
    if not isinstance(selection, list):
        return False
    keys = [_selection_key_from_value(value) for value in selection]
    return all(key is not None and key in pinned for key in keys) and len(keys) == len(set(keys))


def _narrow_mcp_grants(
    grants: list[McpToolGrant], role: str, selection: object
) -> list[McpToolGrant]:
    """Return only the approved subset; omitted selection preserves legacy full pins."""

    if selection is None:
        return list(grants)
    selected = {_selection_key_from_value(value) for value in selection}
    return [grant for grant in grants if _mcp_selection_key(role, grant) in selected]


def _safe_review_evidence(review: dict) -> dict:
    """Retain classified findings while excluding raw reviewer evidence and command output."""

    safe_rounds: list[dict] = []
    for item in review.get("rounds", []):
        if not isinstance(item, dict):
            continue
        findings: list[dict] = []
        for finding in item.get("findings", []):
            if not isinstance(finding, dict):
                continue
            safe = {
                key: finding[key]
                for key in ("severity", "lens", "model", "file", "line", "verified")
                if key in finding
            }
            if isinstance(finding.get("description"), str):
                safe["description"] = finding["description"][:500]
            findings.append(safe)
        round_evidence: dict = {"round": item.get("round"), "findings": findings}
        revision = item.get("revision")
        if isinstance(revision, dict):
            round_evidence["revision"] = {
                key: revision[key]
                for key in ("succeeded", "commits", "changed_files")
                if key in revision
            }
        safe_rounds.append(round_evidence)
    return {"status": review.get("status"), "rounds": safe_rounds, "reason": review.get("reason")}


def _validate_plan_snapshot(plan: dict, envelope: RunEnvelope) -> None:
    """Reject a workflow envelope that does not match its immutable plan snapshot."""

    actual_sha256 = hashlib.sha256(
        json.dumps(
            plan, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()
    if actual_sha256 != envelope.plan_sha256:
        raise ValueError(
            "run plan snapshot digest does not match the submitted envelope"
        )
    if (
        plan.get("spec_set") != envelope.spec_ref
        or plan.get("target_repos") != envelope.target_repos
    ):
        raise ValueError("run envelope does not match its immutable plan snapshot")
    if envelope.specification_evaluation_sha256 is not None:
        if plan.get("specification_evaluation_sha256") != envelope.specification_evaluation_sha256:
            raise ValueError("plan evaluation digest does not match the submitted envelope")
        expected_requirements = envelope.specification_requirement_ids
        if (
            not expected_requirements
            or not all(isinstance(requirement_id, str) and requirement_id.strip() == requirement_id for requirement_id in expected_requirements)
            or len(set(expected_requirements)) != len(expected_requirements)
        ):
            raise ValueError("submitted envelope has invalid requirement traceability")
        phase_requirements: list[str] = []
        for phase in plan.get("phases", []):
            if not isinstance(phase, dict):
                raise ValueError("plan phase is not an object")
            requirement_ids = phase.get("requirement_ids")
            if (
                not isinstance(requirement_ids, list)
                or not requirement_ids
                or not all(isinstance(requirement_id, str) and requirement_id.strip() == requirement_id for requirement_id in requirement_ids)
            ):
                raise ValueError("plan phase has no requirement traceability")
            phase_requirements.extend(requirement_ids)
        if set(phase_requirements) != set(expected_requirements) or len(phase_requirements) != len(set(phase_requirements)):
            raise ValueError("plan requirement traceability does not match the selected specification")


async def _backup_phase(phase: PlanPhase, workspace, ceiling: str):
    """Run the deterministic recovery activity with bounded, safe retries."""

    return await workflow.execute_activity(
        WorkerActivities.backup_phase,
        args=[
            BackupExecutionRequest(
                phase=phase,
                workspace=workspace,
                ceiling=ceiling,
                timeout_seconds=120,
            )
        ],
        start_to_close_timeout=_BACKUP_ACTIVITY_TIMEOUT,
        retry_policy=_BACKUP_PHASE_RETRY_POLICY,
    )


async def _review_implementation(
    envelope: RunEnvelope,
    workspace,
    phases: list[PlanPhase],
    phase_results: list[dict],
    productive_turns: int,
    deadline,
    max_review_rounds: int,
    review_profile: str,
) -> dict:
    """Run bounded review/revision rounds without allowing advisory churn."""

    reviewer_registration = require_role(envelope, "reviewer")
    require_tool(reviewer_registration, "execution_workspace", "read_only_workspace")
    require_tool(reviewer_registration, "review_model", "read_only_review")
    rounds: list[dict] = []
    for round_number in range(1, max_review_rounds + 1):
        remaining = deadline - workflow.now()
        if remaining <= timedelta():
            return {"status": "escalated", "rounds": rounds, "reason": "wall_clock"}
        request = ReviewRequest(
            workspace=workspace,
            phase_results=phase_results,
            round_number=round_number,
            review_profile=review_profile,
        )
        await workflow.execute_activity(
            WorkerActivities.report_status,
            args=[
                envelope.run_id,
                "adversarial_review",
                None,
                {"review": {"status": "in_progress", "round": round_number, "rounds": rounds}},
            ],
            start_to_close_timeout=_ACTIVITY_TIMEOUT,
        )
        try:
            review = await workflow.execute_activity(
                WorkerActivities.review,
                args=[request],
                start_to_close_timeout=min(_REVIEW_ACTIVITY_TIMEOUT, remaining),
                retry_policy=_RUN_PHASE_RETRY_POLICY,
            )
            findings = await workflow.execute_activity(
                WorkerActivities.verify_review_findings,
                args=[request, review.findings],
                start_to_close_timeout=min(_REVIEW_ACTIVITY_TIMEOUT, remaining),
                retry_policy=_RUN_PHASE_RETRY_POLICY,
            )
        except Exception as error:  # noqa: BLE001 - reviewer failures must escalate safely.
            del error
            rounds.append(
                {
                    "round": round_number,
                    "error": "review activity did not complete",
                }
            )
            return {
                "status": "escalated",
                "rounds": rounds,
                "reason": "review_unavailable",
            }
        blocking = [
            finding
            for finding in findings
            if finding.severity == "blocking" and finding.verified is True
        ]
        evidence = {
            "round": round_number,
            "findings": [finding.metadata() for finding in findings],
        }
        rounds.append(evidence)
        if not blocking:
            return {"status": "converged", "rounds": rounds}
        if round_number == max_review_rounds:
            return {"status": "escalated", "rounds": rounds, "reason": "max_review_rounds"}
        try:
            revision = await workflow.execute_activity(
                WorkerActivities.address_review_findings,
                args=[
                    ReviewRevisionRequest(
                        workspace=workspace,
                        findings=blocking,
                        phases=phases,
                        max_turns=productive_turns,
                        timeout_seconds=max(1, int(remaining.total_seconds()) - 1),
                    )
                ],
                start_to_close_timeout=min(_REVIEW_ACTIVITY_TIMEOUT, remaining),
                retry_policy=_RUN_PHASE_RETRY_POLICY,
            )
        except Exception as error:  # noqa: BLE001 - revision failures must escalate safely.
            del error
            evidence["revision"] = {
                "succeeded": False,
                "error": "revision activity did not complete",
            }
            return {
                "status": "escalated",
                "rounds": rounds,
                "reason": "revision_unavailable",
            }
        evidence["revision"] = {
            "succeeded": revision.succeeded,
            "summary": revision.summary,
            "commits": revision.commits,
            "changed_files": revision.changed_files,
            "verification": [item.__dict__ for item in revision.verification],
        }
        if not revision.succeeded:
            return {"status": "escalated", "rounds": rounds, "reason": "revision_failed"}
    return {"status": "escalated", "rounds": rounds, "reason": "max_review_rounds"}


def _execution_plan(plan: dict) -> tuple[list[PlanPhase], int, timedelta, int, float, int, str]:
    """Parse limits and return a source-order-stable topological phase order."""

    phases = plan.get("phases")
    if not isinstance(phases, list) or not phases:
        raise ValueError("execution requires at least one approved plan phase")
    constraints = plan.get("constraints")
    if not isinstance(constraints, dict):
        raise ValueError("plan constraints are missing")
    max_turns = constraints.get("max_turns_per_phase")
    max_wall_clock_minutes = constraints.get("max_wall_clock_minutes")
    max_cost_usd = constraints.get("max_cost_usd")
    max_review_rounds = constraints.get("max_review_rounds", 3)
    backup_reserve_turns = constraints.get("backup_reserve_turns", 25)
    review_profile = plan.get("review_profile", "standard")
    if not isinstance(max_turns, int) or isinstance(max_turns, bool) or max_turns < 1:
        raise ValueError("plan max_turns_per_phase must be a positive integer")
    if (
        not isinstance(max_wall_clock_minutes, int)
        or isinstance(max_wall_clock_minutes, bool)
        or max_wall_clock_minutes < 1
    ):
        raise ValueError("plan max_wall_clock_minutes must be a positive integer")
    if (
        not isinstance(backup_reserve_turns, int)
        or isinstance(backup_reserve_turns, bool)
        or not 20 <= backup_reserve_turns <= 30
    ):
        raise ValueError(
            "plan backup_reserve_turns must be an integer between 20 and 30"
        )
    if max_turns <= backup_reserve_turns:
        raise ValueError("plan max_turns_per_phase must exceed backup_reserve_turns")
    if (
        not isinstance(max_review_rounds, int)
        or isinstance(max_review_rounds, bool)
        or max_review_rounds < 1
    ):
        raise ValueError("plan max_review_rounds must be a positive integer")
    if review_profile not in {"strict", "standard", "minimal"}:
        raise ValueError("plan review_profile must be strict, standard, or minimal")
    if (
        not isinstance(max_cost_usd, int | float)
        or isinstance(max_cost_usd, bool)
        or max_cost_usd <= 0
        or max_cost_usd == float("inf")
        or max_cost_usd != max_cost_usd
    ):
        raise ValueError("plan max_cost_usd must be a positive finite number")
    parsed_phases = [PlanPhase.from_dict(phase) for phase in phases]
    phase_ids = [phase.id for phase in parsed_phases]
    if len(set(phase_ids)) != len(phase_ids):
        raise ValueError("plan phase IDs must be unique")
    known_ids = set(phase_ids)
    if any(
        dependency not in known_ids
        for phase in parsed_phases
        for dependency in phase.depends_on
    ):
        raise ValueError("plan phase dependencies must reference approved phases")
    remaining_dependencies = {
        phase.id: set(phase.depends_on) for phase in parsed_phases
    }
    ordered: list[PlanPhase] = []
    while remaining_dependencies:
        ready = next(
            (
                phase
                for phase in parsed_phases
                if phase.id in remaining_dependencies
                and not remaining_dependencies[phase.id]
            ),
            None,
        )
        if ready is None:
            raise ValueError("plan phase dependencies must not contain a cycle")
        ordered.append(ready)
        del remaining_dependencies[ready.id]
        for dependencies in remaining_dependencies.values():
            dependencies.discard(ready.id)
    return (
        ordered,
        max_turns - backup_reserve_turns,
        timedelta(minutes=max_wall_clock_minutes),
        backup_reserve_turns,
        float(max_cost_usd),
        max_review_rounds,
        review_profile,
    )


def _failure_detail(error: Exception) -> str:
    """Return a bounded failure summary suitable for durable workflow status."""

    messages: list[str] = []
    current: BaseException | None = error
    while current is not None and len(messages) < 5:
        message = _redact_failure_message(" ".join(str(current).split()))
        if message and message not in messages:
            messages.append(message)
        # The activity deliberately wraps provider failures in this safe
        # category. Do not traverse into a provider's exception chain: those
        # exceptions can retain request headers or credentials.
        if message == "GitHub pull-request publication failed":
            break
        next_error = getattr(current, "cause", None)
        current = next_error if isinstance(next_error, BaseException) else None
    return " | ".join(messages)[:4096] or error.__class__.__name__


_FAILURE_SECRETS = (
    re.compile(r"(?i)Bearer\s+[^\s'\"]+"),
    re.compile(r"\b(?:gh[opsu]_[A-Za-z0-9_]+|github_pat_[A-Za-z0-9_]+)\b"),
)


def _redact_failure_message(message: str) -> str:
    """Prevent known bearer-token forms from entering durable status evidence."""

    for pattern in _FAILURE_SECRETS:
        message = pattern.sub("[REDACTED]", message)
    return message


def _is_timeout_error(error: BaseException) -> bool:
    """Recognize Temporal's nested activity-timeout failure without broad recovery."""

    current: BaseException | None = error
    while current is not None:
        if isinstance(current, TimeoutError):
            return True
        cause = getattr(current, "__cause__", None) or getattr(current, "cause", None)
        current = cause if isinstance(cause, BaseException) else None
    return False
