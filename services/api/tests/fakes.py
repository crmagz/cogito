from __future__ import annotations

from cogito_api.models import AiPlan, RunEnvelope
from cogito_api.storage import PlanSnapshot, plan_snapshot_bytes


class InMemoryPlanStore:
    def __init__(self) -> None:
        self.plans: dict[str, AiPlan] = {}
        self.statuses: dict[str, dict] = {}

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


class FakeRunStarter:
    def __init__(self) -> None:
        self.started_runs: list[RunEnvelope] = []

    async def start_run(self, envelope: RunEnvelope) -> None:
        self.started_runs.append(envelope)
