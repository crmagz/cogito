from __future__ import annotations

from typing import Any

import pytest

from cogito_api.temporal import TemporalRunStarter, _terminal_outcome


class _FakeHandle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], str | None]] = []
        self.gate_calls: list[tuple[str, list[Any], str | None]] = []

    async def execute_update(
        self, name: str, decision: dict[str, Any] | None = None, *, args: list[Any] | None = None, id: str | None = None
    ) -> bool:
        if args is not None:
            self.gate_calls.append((name, args, id))
            return True
        assert decision is not None
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


@pytest.mark.asyncio
async def test_temporal_mcp_selection_uses_the_versioned_worker_update() -> None:
    handle = _FakeHandle()
    starter = TemporalRunStarter("temporal:7233", "default", "tasks")
    starter._client = _FakeClient(handle)  # type: ignore[assignment]
    decision = {
        "decision_id": "decision-mcp-1",
        "artifact_sha256": "a" * 64,
        "decision": "approve",
        "mcp_selection": [],
    }

    assert await starter.submit_plan_approval("run-1:plan:1:abcdef", decision) is True
    assert handle.calls == [("submit_plan_approval_with_mcp_selection", decision, "decision-mcp-1")]


@pytest.mark.asyncio
async def test_temporal_resolved_gate_uses_the_sdk_multi_argument_shape() -> None:
    handle = _FakeHandle()
    starter = TemporalRunStarter("temporal:7233", "default", "tasks")
    starter._client = _FakeClient(handle)  # type: ignore[assignment]
    decision = {"decision_id": "decision-gate-1", "artifact_sha256": "a" * 64, "decision": "approve"}

    assert await starter.submit_workflow_gate("run-1:plan:1:abcdef", "plan_scope_review", decision) is True
    assert handle.gate_calls == [
        ("submit_workflow_gate", ["plan_scope_review", decision], "decision-gate-1")
    ]


def test_terminal_outcome_accepts_only_known_reconcilable_results() -> None:
    assert _terminal_outcome({"status": "completed"}) == "completed"
    assert _terminal_outcome({"status": "failed"}) == "failed"
    assert _terminal_outcome({"status": "revision_requested"}) is None
    assert _terminal_outcome({"unexpected": "shape"}) is None
