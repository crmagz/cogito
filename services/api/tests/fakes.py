from __future__ import annotations

from cogito_api.models import AiPlan, ArtifactReference, RunEnvelope
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


class InMemorySupervisorStore:
    def __init__(self) -> None:
        self.planning_runs: dict[str, PlanningRunRecord] = {}

    async def create_planning_run(self, record: PlanningRunRecord) -> None:
        self.planning_runs[record.run_id] = record

    async def get_planning_run(self, run_id: str) -> PlanningRunRecord | None:
        return self.planning_runs.get(run_id)


class FakeRunStarter:
    def __init__(self) -> None:
        self.started_runs: list[RunEnvelope] = []

    async def start_run(self, envelope: RunEnvelope) -> None:
        self.started_runs.append(envelope)
