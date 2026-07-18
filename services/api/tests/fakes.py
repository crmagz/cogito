from __future__ import annotations

from cogito_api.models import AiPlan, RunEnvelope


class InMemoryPlanStore:
    def __init__(self) -> None:
        self.plans: dict[str, AiPlan] = {}
        self.statuses: dict[str, dict] = {}

    def put_plan(self, run_id: str, plan: AiPlan) -> str:
        self.plans[run_id] = plan
        return f"s3://plans/plans/{run_id}/plan.json"

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)


class FakeRunStarter:
    def __init__(self) -> None:
        self.started_runs: list[RunEnvelope] = []

    async def start_run(self, envelope: RunEnvelope) -> None:
        self.started_runs.append(envelope)
