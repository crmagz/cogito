from __future__ import annotations

from cogito_worker import worker


async def test_connect_temporal_retries_until_the_service_is_ready(monkeypatch) -> None:
    calls = 0
    client = object()

    async def connect(host: str, *, namespace: str) -> object:
        nonlocal calls
        calls += 1
        assert host == "temporal:7233"
        assert namespace == "default"
        if calls == 1:
            raise RuntimeError("connection refused")
        return client

    async def no_delay(seconds: float) -> None:
        assert seconds == 2

    monkeypatch.setattr(worker.Client, "connect", connect)
    monkeypatch.setattr(worker.asyncio, "sleep", no_delay)

    assert await worker._connect_temporal("temporal:7233", "default") is client
    assert calls == 2
