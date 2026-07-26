"""Signed webhook event and independent outbox delivery coverage."""

from __future__ import annotations

import hashlib
import hmac
import json

from cogito_api.notifications import NotificationOutboxDispatcher, webhook_event_bytes
from cogito_api.supervisor import CoordinationEvent

from .fakes import InMemorySupervisorStore


def _event() -> CoordinationEvent:
    return CoordinationEvent(
        event_id="event-1",
        run_id="run-1",
        event_type="plan_approval_requested",
        occurred_at="2026-07-25T00:00:00+00:00",
        gate="plan",
        artifact_ref="s3://plans/runs/run-1/revisions/1/plan.json",
        artifact_sha256="a" * 64,
        decision=None,
        lifecycle_status=None,
        payload={
            "read_url": "/api/v1/planning-runs/run-1/coordination",
            "action_url": "/api/v1/coordination/runs/run-1/actions/plan",
            "comment": "must not escape",
            "provider_token": "must not escape",
        },
    )


def test_webhook_event_bytes_are_canonical_and_allow_list_safe() -> None:
    body = webhook_event_bytes(_event())
    parsed = json.loads(body)

    assert body == json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode()
    assert parsed["artifact"]["sha256"] == "a" * 64
    assert "comment" not in parsed
    assert "provider_token" not in parsed
    assert hmac.compare_digest(
        hmac.new(b"test-secret", body, hashlib.sha256).hexdigest(),
        "036d69b1f0058381babcd0a1b1692b65ff820c472c01ff6113fd75a43159dbcf",
    )


async def test_notification_failure_does_not_change_authoritative_run_state() -> None:
    class FailingSink:
        async def deliver(self, event: CoordinationEvent) -> bool:
            del event
            raise ConnectionError("authorization=secret")

    store = InMemorySupervisorStore()
    store.coordination_events["event-1"] = _event()
    store.notification_deliveries["event-1"] = (False, 0, None)
    dispatcher = NotificationOutboxDispatcher(store, FailingSink())

    delivered = await dispatcher.deliver_once()

    assert delivered == set()
    assert store.notification_deliveries["event-1"] == (False, 1, "transient notification delivery failure")
    assert store.planning_runs == {}


async def test_notification_acknowledgement_is_idempotent() -> None:
    class AcceptingSink:
        calls = 0

        async def deliver(self, event: CoordinationEvent) -> bool:
            del event
            self.calls += 1
            return True

    store = InMemorySupervisorStore()
    store.coordination_events["event-1"] = _event()
    store.notification_deliveries["event-1"] = (False, 0, None)
    sink = AcceptingSink()
    dispatcher = NotificationOutboxDispatcher(store, sink)

    first = await dispatcher.deliver_once()
    second = await dispatcher.deliver_once()

    assert first == {"event-1"}
    assert second == set()
    assert sink.calls == 1
