from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from cogito_api.audit_logs import LokiAuditLogReader


async def test_loki_reader_uses_fixed_invocation_query_and_redacts_again() -> None:
    invocation_id = "a" * 64
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "data": {
                    "result": [
                        {
                            "stream": {"namespace": "cogito-executions", "pod": "worker", "container": "execution"},
                            "values": [["1700000000000000000", f'{invocation_id} stdout token=super-secret']],
                        }
                    ]
                }
            },
        )

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))
    page = await reader.read_invocation(invocation_id)

    assert len(requests) == 1
    assert requests[0].url.path == "/loki/api/v1/query_range"
    assert requests[0].url.params["query"] == '{namespace=~"cogito|cogito-executions"} |= "' + invocation_id + ' "'
    assert page.availability == "available"
    assert page.lines[0].message.endswith("token=[REDACTED]")


async def test_loki_reader_redacts_basic_and_equals_form_authorization_credentials() -> None:
    invocation_id = "a" * 64

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "result": [{
                        "stream": {},
                        "values": [[
                            "1700000000000000000",
                            f'{invocation_id} Authorization: Basic dXNlcjpwYXNz Authorization=Bearer gateway-token Bearer standalone-token '
                            '{"authorization":"Bearer json-token"}',
                        ]],
                    }]
                }
            },
        )

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))
    page = await reader.read_invocation(invocation_id)

    message = page.lines[0].message
    assert "dXNlcjpwYXNz" not in message
    assert "gateway-token" not in message
    assert "standalone-token" not in message
    assert "json-token" not in message
    assert message.count("[REDACTED]") == 4


async def test_loki_reader_rejects_an_invalid_correlation_before_network_access() -> None:
    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(lambda _: (_ for _ in ()).throw(AssertionError())))

    page = await reader.read_invocation("not-an-invocation")

    assert page.availability == "unavailable"
    assert page.lines == []


async def test_loki_reader_anchors_a_completed_invocation_to_its_audit_event() -> None:
    invocation_id = "a" * 64
    occurred_at = datetime.now(timezone.utc) - timedelta(minutes=20)
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": {"result": []}})

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))

    await reader.read_invocation(invocation_id, occurred_at=occurred_at.isoformat())

    assert len(requests) == 1
    start = datetime.fromtimestamp(int(requests[0].url.params["start"]) / 1_000_000_000, timezone.utc)
    assert start <= occurred_at


async def test_loki_reader_keeps_a_cursor_when_the_byte_limit_truncates_a_page() -> None:
    invocation_id = "a" * 64
    requests: list[httpx.Request] = []
    values = [[str(1_700_000_000_000_000_000 - index), "x" * 4096] for index in range(17)]

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"data": {"result": [{"stream": {}, "values": values}]}})

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))
    page = await reader.read_invocation(invocation_id)

    assert len(page.lines) == 16
    assert page.next_cursor == f"ns:{values[15][0]}"
    await reader.read_invocation(invocation_id, cursor=page.next_cursor)
    assert requests[1].url.params["end"] == str(int(values[15][0]) - 1)


async def test_loki_reader_keeps_a_cursor_for_a_full_line_limited_page() -> None:
    invocation_id = "a" * 64
    values = [[str(1_700_000_000_000_000_000 - index), "line"] for index in range(200)]

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": {"result": [{"stream": {}, "values": values}]}})

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))
    page = await reader.read_invocation(invocation_id)

    assert len(page.lines) == 200
    assert page.next_cursor == f"ns:{values[-1][0]}"


async def test_loki_reader_skips_an_out_of_range_timestamp() -> None:
    invocation_id = "a" * 64

    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": {
                    "result": [{
                        "stream": {},
                        "values": [
                            ["999999999999999999999999999999999999", "unparseable"],
                            ["1700000000000000000", "valid output"],
                        ],
                    }]
                }
            },
        )

    reader = LokiAuditLogReader("http://loki.test", 3, transport=httpx.MockTransport(handler))
    page = await reader.read_invocation(invocation_id)

    assert page.availability == "available"
    assert [line.message for line in page.lines] == ["valid output"]
