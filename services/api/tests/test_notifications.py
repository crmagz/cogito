"""Signed webhook event and independent outbox delivery coverage."""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import replace

from cogito_api.notifications import NotificationOutboxDispatcher, SlackNotificationSink, slack_event_payload, webhook_event_bytes
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


def test_slack_payload_uses_block_kit_with_a_workbench_link_only() -> None:
    payload = slack_event_payload(_event(), workbench_url="https://workbench.example.test", thread_ts=None)

    assert payload["text"] == "Cogito run run-1: plan approval requested"
    assert "thread_ts" not in payload
    assert "artifact" not in str(payload)
    assert "action_id" not in str(payload)
    button = payload["blocks"][2]["elements"][0]
    assert button["url"] == "https://workbench.example.test/runs/run-1/workflow"
    assert button["text"]["text"] == "Open in Workbench"


async def test_slack_delivery_creates_one_root_and_replies_in_the_same_thread() -> None:
    store = InMemorySupervisorStore()
    posts: list[tuple[str, str | None]] = []

    async def post(channel_id: str, thread_ts: str | None) -> str:
        posts.append((channel_id, thread_ts))
        return f"{len(posts)}.000000"

    first = _event()
    second = replace(first, event_id="event-2", event_type="planning_started")

    assert await store.deliver_slack_notification(first, channel_id="C01234567", post=post)
    assert await store.deliver_slack_notification(second, channel_id="C01234567", post=post)
    assert await store.deliver_slack_notification(first, channel_id="C01234567", post=post)

    assert posts == [("C01234567", None), ("C01234567", "1.000000")]
    assert store.slack_notification_threads == {"run-1": ("C01234567", "1.000000")}


async def test_slack_sink_posts_a_threaded_block_kit_message_without_leaking_the_token(monkeypatch) -> None:
    requests: list[tuple[str, dict, dict]] = []

    class Response:
        status_code = 200

        def json(self) -> dict[str, object]:
            return {"ok": True, "ts": "1.000000"}

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, url: str, *, json: dict, headers: dict):
            requests.append((url, json, headers))
            return Response()

    monkeypatch.setattr("cogito_api.notifications.httpx.AsyncClient", lambda **_kwargs: Client())
    sink = SlackNotificationSink(
        InMemorySupervisorStore(),
        bot_token="xoxb-secret",
        channel_id="C01234567",
        workbench_url="https://workbench.example.test",
        timeout_seconds=10,
    )

    assert await sink.deliver(_event())

    url, payload, headers = requests[0]
    assert url == "https://slack.com/api/chat.postMessage"
    assert payload["channel"] == "C01234567"
    assert headers == {"Authorization": "Bearer xoxb-secret"}
    assert "xoxb-secret" not in str(payload)
    assert "artifact" not in str(payload)


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
