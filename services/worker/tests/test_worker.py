from __future__ import annotations

from cogito_worker import worker
from cogito_worker.workflows import _BACKUP_PHASE_RETRY_POLICY, _PROVISION_RETRY_POLICY, _RUN_PHASE_RETRY_POLICY


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


def test_worker_readiness_sentinel_is_local_and_removable(tmp_path, monkeypatch) -> None:
    sentinel = tmp_path / "worker-ready"
    monkeypatch.setattr(worker, "_READINESS_FILE", sentinel)

    worker._clear_readiness_file()
    assert sentinel.exists() is False

    worker._mark_ready()
    assert sentinel.exists() is True

    worker._clear_readiness_file()
    assert sentinel.exists() is False


def test_idempotent_activities_use_bounded_exponential_retries_while_model_runs_do_not() -> None:
    assert _PROVISION_RETRY_POLICY.maximum_attempts == 3
    assert _PROVISION_RETRY_POLICY.initial_interval.total_seconds() == 1
    assert _PROVISION_RETRY_POLICY.maximum_interval.total_seconds() == 30
    assert _PROVISION_RETRY_POLICY.backoff_coefficient == 2.0
    assert _BACKUP_PHASE_RETRY_POLICY.maximum_attempts == 3
    assert _RUN_PHASE_RETRY_POLICY.maximum_attempts == 1
