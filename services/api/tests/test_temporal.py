from __future__ import annotations

import pytest

from cogito_api.temporal import TemporalRunStarter


class _FakeHandle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, str], str | None]] = []

    async def execute_update(self, name: str, decision: dict[str, str], *, id: str | None = None) -> bool:
        self.calls.append((name, decision, id))
        return True


class _FakeClient:
    def __init__(self, handle: _FakeHandle) -> None:
        self.handle = handle
        self.workflow_ids: list[str] = []

    def get_workflow_handle(self, workflow_id: str) -> _FakeHandle:
        self.workflow_ids.append(workflow_id)
        return self.handle


@pytest.mark.asyncio
async def test_temporal_approval_uses_the_durable_decision_id_as_update_id() -> None:
    handle = _FakeHandle()
    starter = TemporalRunStarter("temporal:7233", "default", "tasks")
    starter._client = _FakeClient(handle)  # type: ignore[assignment]
    decision = {"decision_id": "decision-1", "artifact_sha256": "a" * 64, "decision": "approve"}

    accepted = await starter.submit_plan_approval("run-1:plan:1:abcdef", decision)

    assert accepted is True
    assert handle.calls == [("submit_plan_approval", decision, "decision-1")]


@pytest.mark.asyncio
async def test_temporal_approval_rejects_a_missing_decision_id() -> None:
    starter = TemporalRunStarter("temporal:7233", "default", "tasks")

    assert await starter.submit_plan_approval("workflow", {"decision": "approve"}) is False
