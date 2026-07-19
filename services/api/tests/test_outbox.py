from __future__ import annotations

import asyncio

from cogito_api.outbox import PlanApprovalOutboxDispatcher, _error_detail, stop_dispatcher

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
