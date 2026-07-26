"""Provider-neutral, signed delivery of authoritative coordination events."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
from typing import Protocol

import httpx

from .config import Settings
from .supervisor import CoordinationEvent, SupervisorStore

_LOGGER = logging.getLogger(__name__)


class NotificationSink(Protocol):
    """Deliver one immutable event without authority to change Cogito state."""

    async def deliver(self, event: CoordinationEvent) -> bool: ...


class WebhookNotificationSink:
    """Deliver canonical event snapshots with an HMAC over the exact body."""

    def __init__(self, webhook_url: str, signing_secret: str, timeout_seconds: float):
        self._webhook_url = webhook_url
        self._signing_secret = signing_secret.encode()
        self._timeout_seconds = timeout_seconds

    async def deliver(self, event: CoordinationEvent) -> bool:
        """Post one event and accept only a direct 2xx acknowledgement."""

        body = webhook_event_bytes(event)
        signature = hmac.new(self._signing_secret, body, hashlib.sha256).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-Cogito-Event-Id": event.event_id,
            "X-Cogito-Schema-Version": "1.0",
            "X-Cogito-Signature": f"sha256={signature}",
        }
        async with httpx.AsyncClient(follow_redirects=False, timeout=self._timeout_seconds) as client:
            response = await client.post(self._webhook_url, content=body, headers=headers)
        return 200 <= response.status_code < 300


class NotificationOutboxDispatcher:
    """Lease and deliver notifications independently from Temporal approval delivery."""

    def __init__(self, store: SupervisorStore, sink: NotificationSink, poll_seconds: float = 1.0):
        self._store = store
        self._sink = sink
        self._poll_seconds = poll_seconds

    async def deliver_once(self, limit: int = 10) -> set[str]:
        """Attempt a bounded delivery batch and return acknowledged event IDs."""

        delivered: set[str] = set()
        for item in await self._store.claim_notification_deliveries(limit=limit, lease_seconds=30):
            try:
                accepted = await self._sink.deliver(item.event)
            except Exception:
                await self._store.release_notification_delivery(
                    item.event.event_id,
                    retry_seconds=_retry_delay(item.attempt_count),
                    error="transient notification delivery failure",
                )
                continue
            if accepted:
                await self._store.mark_notification_delivered(item.event.event_id)
                delivered.add(item.event.event_id)
            else:
                await self._store.release_notification_delivery(
                    item.event.event_id,
                    retry_seconds=_retry_delay(item.attempt_count),
                    error="notification webhook did not acknowledge event",
                )
        return delivered

    async def run(self) -> None:
        """Poll until cancelled; leases make concurrent API replicas safe."""

        while True:
            try:
                await self.deliver_once()
            except Exception:
                _LOGGER.warning("notification delivery pass failed", exc_info=False)
            await asyncio.sleep(self._poll_seconds)


async def stop_notification_dispatcher(task: asyncio.Task[None] | None) -> None:
    """Cancel a dispatcher only when notifications were explicitly enabled."""

    if task is None:
        return
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def webhook_event_bytes(event: CoordinationEvent) -> bytes:
    """Return canonical, allow-listed webhook bytes without artifact contents or secrets."""

    body = {
        "schema_version": "1.0",
        "event_id": event.event_id,
        "event_type": event.event_type,
        "run_id": event.run_id,
        "occurred_at": event.occurred_at,
        "gate": event.gate,
        "artifact": (
            {"ref": event.artifact_ref, "sha256": event.artifact_sha256}
            if event.artifact_ref and event.artifact_sha256
            else None
        ),
        "decision": event.decision,
        "lifecycle_status": event.lifecycle_status,
        "read_url": event.payload.get("read_url"),
        "action_url": event.payload.get("action_url"),
    }
    return json.dumps(body, sort_keys=True, separators=(",", ":")).encode()


def notification_sink(settings: Settings) -> NotificationSink | None:
    """Return the enabled webhook sink, leaving disabled deployments network-silent."""

    if not settings.notification_enabled:
        return None
    return WebhookNotificationSink(
        settings.notification_webhook_url,
        settings.notification_webhook_hmac_secret,
        settings.notification_timeout_seconds,
    )


def _retry_delay(attempt_count: int) -> int:
    """Use the same bounded exponential retry cadence as approval outboxes."""

    return min(60, 2 ** min(attempt_count, 6))
