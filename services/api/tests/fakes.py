from __future__ import annotations

from cogito_api.models import AiPlan, ArtifactReference, PlanningRunStatus, RunEnvelope
from cogito_api.planner import PlanningContext
from cogito_api.storage import PlanSnapshot, plan_snapshot_bytes, source_specification_bytes
from cogito_api.supervisor import PlanningRunRecord


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

    async def start_run(self, envelope: RunEnvelope) -> None:
        self.started_runs.append(envelope)
