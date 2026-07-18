from __future__ import annotations


class InMemoryRunStore:
    def __init__(self) -> None:
        self.plans: dict[str, dict] = {}
        self.statuses: dict[str, dict] = {}

    def get_plan(self, plan_ref: str) -> dict:
        return self.plans[plan_ref]

    def get_status(self, run_id: str) -> dict | None:
        return self.statuses.get(run_id)

    def put_status(self, run_id: str, status: dict) -> None:
        self.statuses[run_id] = status
