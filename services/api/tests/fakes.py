from __future__ import annotations

from datetime import datetime, timezone

from cogito_api.models import AiPlan, ArtifactReference, PlanApprovalDecision, PlanningRunStatus, RunEnvelope
from cogito_api.planner import PlanningContext
from cogito_api.storage import PlanSnapshot, plan_snapshot_bytes, source_specification_bytes
from cogito_api.supervisor import ApprovalConflictError, ApprovalRecord, OutboxDelivery, PlanningRunRecord


class InMemoryPlanStore:
    def __init__(self) -> None:
        self.plans: dict[str, AiPlan] = {}
        self.statuses: dict[str, dict] = {}
        self.source_specifications: dict[str, str] = {}

    def put_plan(self, run_id: str, plan: AiPlan) -> PlanSnapshot:
        self.plans[run_id] = plan
        from hashlib import sha256

        return PlanSnapshot(
            ref=f"s3://plans/plans/{run_id}/plan.json",
            sha256=sha256(plan_snapshot_bytes(plan)).hexdigest(),
        )

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)

    def put_source_specification(self, run_id: str, initial_specification: str) -> ArtifactReference:
        from hashlib import sha256

        self.source_specifications[run_id] = initial_specification
        return ArtifactReference(
            ref=f"s3://plan-snapshots/runs/{run_id}/source-spec.json",
            sha256=sha256(source_specification_bytes(initial_specification)).hexdigest(),
        )

    def get_source_specification(self, source_artifact_ref: str) -> str:
        run_id = source_artifact_ref.split("/")[4]
        return self.source_specifications[run_id]


class InMemorySupervisorStore:
    def __init__(self) -> None:
        self.planning_runs: dict[str, PlanningRunRecord] = {}
        self.approvals: dict[tuple[str, str], ApprovalRecord] = {}
        self.approval_request_hashes: dict[tuple[str, str], str] = {}
        self.outbox: dict[str, OutboxDelivery] = {}
        self.leased_decision_ids: set[str] = set()

    async def create_planning_run(self, record: PlanningRunRecord) -> None:
        self.planning_runs[record.run_id] = record

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        return self.planning_runs.get(run_id)

    async def attach_generated_plan(
        self,
        run_id: str,
        plan_artifact: ArtifactReference,
        planner_model: str,
    ) -> PlanningRunRecord:
        record = self.planning_runs[run_id]
        if record.status.value != "planning":
            raise ValueError("planning run is not eligible to accept a generated plan")
        updated = PlanningRunRecord(
            run_id=record.run_id,
            status=PlanningRunStatus.AWAITING_PLAN_APPROVAL,
            source_artifact=record.source_artifact,
            target_repos=record.target_repos,
            spec_set=record.spec_set,
            constraints=record.constraints,
            priority=record.priority,
            submitted_at=record.submitted_at,
            submitted_by=record.submitted_by,
            plan_artifact=plan_artifact,
            planner_model=planner_model,
        )
        self.planning_runs[run_id] = updated
        return updated

    async def record_plan_approval(
        self,
        run_id: str,
        artifact_sha256: str,
        decision: PlanApprovalDecision,
        actor_id: str,
        comment: str | None,
        idempotency_key: str,
        request_sha256: str,
    ) -> ApprovalRecord:
        existing = self.approvals.get((run_id, idempotency_key))
        if existing is not None:
            if self.approval_request_hashes[(run_id, idempotency_key)] != request_sha256:
                raise ApprovalConflictError("idempotency key was reused with a different decision")
            return existing
        run = self.planning_runs.get(run_id)
        if run is None or run.status is not PlanningRunStatus.AWAITING_PLAN_APPROVAL:
            raise ApprovalConflictError("planning run is not awaiting plan approval")
        if run.plan_artifact is None or run.plan_artifact.sha256 != artifact_sha256:
            raise ApprovalConflictError("plan approval artifact digest is stale")
        record = ApprovalRecord(
            decision_id=f"decision-{len(self.approvals) + 1}",
            run_id=run_id,
            decision=decision,
            artifact_sha256=artifact_sha256,
            actor_id=actor_id,
            created_at=datetime.now(timezone.utc).isoformat(),
            delivered=False,
        )
        self.approvals[(run_id, idempotency_key)] = record
        self.approval_request_hashes[(run_id, idempotency_key)] = request_sha256
        self.outbox[record.decision_id] = OutboxDelivery(
            decision_id=record.decision_id,
            run_id=record.run_id,
            payload={
                "decision_id": record.decision_id,
                "artifact_sha256": record.artifact_sha256,
                "decision": record.decision.value,
            },
            attempt_count=0,
        )
        return record

    async def mark_plan_approval_delivered(self, decision_id: str) -> None:
        for key, record in self.approvals.items():
            if record.decision_id == decision_id:
                self.approvals[key] = ApprovalRecord(
                    decision_id=record.decision_id,
                    run_id=record.run_id,
                    decision=record.decision,
                    artifact_sha256=record.artifact_sha256,
                    actor_id=record.actor_id,
                    created_at=record.created_at,
                    delivered=True,
                )
                run = self.planning_runs[record.run_id]
                status = {
                    PlanApprovalDecision.APPROVE: PlanningRunStatus.IMPLEMENTING,
                    PlanApprovalDecision.REJECT: PlanningRunStatus.REJECTED,
                    PlanApprovalDecision.REQUEST_REVISION: PlanningRunStatus.REVISION_REQUESTED,
                }[record.decision]
                self.planning_runs[record.run_id] = PlanningRunRecord(
                    run_id=run.run_id,
                    status=status,
                    source_artifact=run.source_artifact,
                    target_repos=run.target_repos,
                    spec_set=run.spec_set,
                    constraints=run.constraints,
                    priority=run.priority,
                    submitted_at=run.submitted_at,
                    submitted_by=run.submitted_by,
                    plan_artifact=run.plan_artifact,
                    planner_model=run.planner_model,
                )
                self.outbox.pop(decision_id, None)
                self.leased_decision_ids.discard(decision_id)
                return

    async def claim_plan_approval_deliveries(
        self, *, limit: int, lease_seconds: int, decision_id: str | None = None
    ) -> list[OutboxDelivery]:
        del lease_seconds
        claimed: list[OutboxDelivery] = []
        for item in self.outbox.values():
            if decision_id and item.decision_id != decision_id:
                continue
            if item.decision_id in self.leased_decision_ids:
                continue
            self.leased_decision_ids.add(item.decision_id)
            updated = OutboxDelivery(
                decision_id=item.decision_id,
                run_id=item.run_id,
                payload=item.payload,
                attempt_count=item.attempt_count + 1,
            )
            self.outbox[item.decision_id] = updated
            claimed.append(updated)
            if len(claimed) == limit:
                break
        return claimed

    async def release_plan_approval_delivery(
        self, decision_id: str, *, retry_seconds: int, error: str
    ) -> None:
        del retry_seconds, error
        self.leased_decision_ids.discard(decision_id)


class FakePlanner:
    def __init__(self, plan: AiPlan) -> None:
        self.plan = plan
        self.contexts: list[PlanningContext] = []

    async def generate(self, context: PlanningContext) -> AiPlan:
        self.contexts.append(context)
        return self.plan


class FakeRunStarter:
    def __init__(self) -> None:
        self.started_runs: list[RunEnvelope] = []
        self.plan_approvals: list[tuple[str, dict[str, str]]] = []
        self.approval_error: Exception | None = None
        self.approval_result = True
        self.start_error: Exception | None = None

    async def start_run(self, envelope: RunEnvelope) -> None:
        if self.start_error is not None:
            raise self.start_error
        if any(run.run_id == envelope.run_id for run in self.started_runs):
            return
        self.started_runs.append(envelope)

    async def submit_plan_approval(self, run_id: str, decision: dict[str, str]) -> bool:
        self.plan_approvals.append((run_id, decision))
        if self.approval_error is not None:
            raise self.approval_error
        return self.approval_result
