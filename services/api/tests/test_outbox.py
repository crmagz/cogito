from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from cogito_api.models import AgentRunStatus, ArtifactReference, PlanConstraints, PlanningRunStatus
from cogito_api.outbox import PlanningGenerationDispatcher, PlanApprovalOutboxDispatcher, _error_detail, stop_dispatcher
from cogito_api.supervisor import AgentRunRecord, PlanningGenerationDelivery, PlanningRunRecord

from .fakes import FakeRunStarter, InMemorySupervisorStore


async def test_dispatcher_survives_a_transient_store_failure() -> None:
    class FlakyStore(InMemorySupervisorStore):
        def __init__(self) -> None:
            super().__init__()
            self.claim_calls = 0

        async def claim_plan_approval_deliveries(self, **kwargs: object):  # type: ignore[no-untyped-def]
            self.claim_calls += 1
            if self.claim_calls == 1:
                raise ConnectionError("database temporarily unavailable")
            return await super().claim_plan_approval_deliveries(**kwargs)  # type: ignore[arg-type]

    store = FlakyStore()
    dispatcher = PlanApprovalOutboxDispatcher(store, FakeRunStarter(), poll_seconds=0.001)
    task = asyncio.create_task(dispatcher.run())
    while store.claim_calls < 2:
        await asyncio.sleep(0.001)
    await stop_dispatcher(task)

    assert store.claim_calls >= 2


def test_outbox_error_detail_never_persists_exception_text() -> None:
    assert _error_detail(RuntimeError("password=super-secret")) == "transient Temporal delivery failure"


async def test_planning_dispatcher_renews_a_live_claim_before_completion() -> None:
    store = InMemorySupervisorStore()
    now = datetime.now(timezone.utc).isoformat()
    store.planning_runs["run-1"] = PlanningRunRecord(
        run_id="run-1", status=PlanningRunStatus.PLANNING,
        source_artifact=ArtifactReference(ref="s3://plans/run-1/source", sha256="a" * 64),
        target_repos=["https://github.com/acme/project.git#0123456789abcdef0123456789abcdef01234567"],
        spec_set="typescript@v1#sha256=" + "b" * 64, constraints=PlanConstraints(), priority="normal",
        submitted_at=now, submitted_by="api",
        selected_product_specification_artifact=ArtifactReference(ref="s3://plans/run-1/product", sha256="c" * 64),
        selected_product_specification_revision=1,
        selected_specification_evaluation_artifact=ArtifactReference(ref="s3://plans/run-1/evaluation", sha256="d" * 64),
    )
    store.agent_runs["run-1"] = AgentRunRecord(
        run_id="run-1", root_run_id="run-1", parent_run_id=None, agent_name="planner",
        status=AgentRunStatus.QUEUED, trace_id="e" * 32, created_at=now, updated_at=now,
    )
    renewed = asyncio.Event()

    async def deliver(item: PlanningGenerationDelivery) -> bool:
        await renewed.wait()
        return True

    dispatcher = PlanningGenerationDispatcher(store, deliver, lease_seconds=2, lease_renewal_seconds=0.001)
    task = asyncio.create_task(dispatcher.deliver_once())
    while not store.planning_generation_renewals:
        await asyncio.sleep(0.001)
    renewed.set()
    assert await task == 1
    assert store.planning_generation_renewals[0][0] == "run-1"
