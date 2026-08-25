"""Bounded, server-authorized Loki reads for one audited invocation."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol

import httpx

_INVOCATION_ID = re.compile(r"^[a-f0-9]{64}$")
_SECRET = re.compile(
    r"(?i)(authorization[\"']?\s*[:=]\s*[\"']?(?:[a-z][a-z0-9_-]*\s+)?|bearer\s+|(?:api[_ -]?key|access[_ -]?key|secret(?:[_ -]?key)?|token|password)[\"']?\s*[:=]\s*[\"']?)[^\s,}\"']+"
)
_LOKI_CURSOR_PREFIX = "ns:"
_MAX_LINES = 200
_MAX_BYTES = 64 * 1024
_WINDOW = timedelta(minutes=15)
_INVOCATION_MAX_DURATION = timedelta(hours=2)
_INVOCATION_LOOKBACK = timedelta(minutes=2)


@dataclass(frozen=True)
class AuditLogLine:
    """One redacted Loki line returned through the Cogito authorization boundary."""

    timestamp: str
    stream: str
    message: str


@dataclass(frozen=True)
class AuditLogPage:
    """A bounded result page or a non-sensitive availability state."""

    availability: str
    lines: list[AuditLogLine]
    next_cursor: str | None = None


class AuditLogReader(Protocol):
    async def read_invocation(
        self, invocation_id: str, cursor: str | None = None, occurred_at: str | None = None
    ) -> AuditLogPage: ...


class DisabledAuditLogReader:
    async def read_invocation(
        self, invocation_id: str, cursor: str | None = None, occurred_at: str | None = None
    ) -> AuditLogPage:
        del invocation_id, cursor, occurred_at
        return AuditLogPage(availability="disabled", lines=[])


class LokiAuditLogReader:
    """Query Loki with a fixed selector, never caller-provided LogQL."""

    def __init__(self, endpoint: str, timeout_seconds: float, transport: httpx.AsyncBaseTransport | None = None):
        self._endpoint = endpoint.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._transport = transport

    async def read_invocation(
        self, invocation_id: str, cursor: str | None = None, occurred_at: str | None = None
    ) -> AuditLogPage:
        if not _INVOCATION_ID.fullmatch(invocation_id):
            return AuditLogPage(availability="unavailable", lines=[])
        start, end = _invocation_window(cursor, occurred_at)
        query = '{namespace=~"cogito|cogito-executions"} |= "' + invocation_id + ' "'
        try:
            async with httpx.AsyncClient(timeout=self._timeout_seconds, transport=self._transport) as client:
                response = await client.get(
                    f"{self._endpoint}/loki/api/v1/query_range",
                    params={
                        "query": query,
                        "start": str(int(start.timestamp() * 1_000_000_000)),
                        "end": str(end),
                        "limit": str(_MAX_LINES),
                        "direction": "BACKWARD",
                    },
                )
                response.raise_for_status()
                body = response.json()
        except (httpx.HTTPError, ValueError):
            return AuditLogPage(availability="unavailable", lines=[])
        lines, last_timestamp, truncated = _parse_loki_lines(body)
        next_cursor = _loki_cursor(last_timestamp) if lines and (truncated or len(lines) == _MAX_LINES) else None
        return AuditLogPage(availability="available", lines=lines, next_cursor=next_cursor)


def _parse_cursor(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _parse_loki_cursor(value: str | None) -> int | None:
    """Return an opaque nanosecond cursor emitted by this reader."""

    if not value or not value.startswith(_LOKI_CURSOR_PREFIX):
        return None
    try:
        timestamp = int(value.removeprefix(_LOKI_CURSOR_PREFIX))
    except ValueError:
        return None
    return timestamp if timestamp > 0 else None


def _loki_cursor(timestamp: int | None) -> str | None:
    return f"{_LOKI_CURSOR_PREFIX}{timestamp}" if timestamp is not None else None


def _datetime_nanoseconds(value: datetime) -> int:
    return int(value.timestamp() * 1_000_000_000)


def _invocation_window(cursor: str | None, occurred_at: str | None) -> tuple[datetime, int]:
    """Return a bounded time range anchored to the immutable audit event."""

    event_time = _parse_cursor(occurred_at)
    opaque_cursor = _parse_loki_cursor(cursor)
    if event_time is None:
        end = (
            datetime.fromtimestamp((opaque_cursor - 1) / 1_000_000_000, timezone.utc)
            if opaque_cursor is not None
            else _parse_cursor(cursor) or datetime.now(timezone.utc)
        )
        end_nanoseconds = opaque_cursor - 1 if opaque_cursor is not None else _datetime_nanoseconds(end)
        return end - _WINDOW, end_nanoseconds
    initial_end = min(datetime.now(timezone.utc), event_time + _INVOCATION_MAX_DURATION)
    end = _parse_cursor(cursor) or initial_end
    maximum_nanoseconds = _datetime_nanoseconds(event_time + _INVOCATION_MAX_DURATION)
    end_nanoseconds = opaque_cursor - 1 if opaque_cursor is not None else _datetime_nanoseconds(end)
    return event_time - _INVOCATION_LOOKBACK, min(end_nanoseconds, maximum_nanoseconds)


def _parse_loki_lines(body: object) -> tuple[list[AuditLogLine], int | None, bool]:
    if not isinstance(body, dict) or not isinstance(body.get("data"), dict):
        return [], None, False
    streams = body["data"].get("result")
    if not isinstance(streams, list):
        return [], None, False
    lines: list[AuditLogLine] = []
    byte_count = 0
    last_timestamp: int | None = None
    for stream in streams:
        if not isinstance(stream, dict) or not isinstance(stream.get("values"), list):
            continue
        labels = stream.get("stream") if isinstance(stream.get("stream"), dict) else {}
        source = "/".join(str(labels.get(key, "")) for key in ("namespace", "pod", "container")).strip("/")
        for value in stream["values"]:
            if not (isinstance(value, list) and len(value) == 2 and all(isinstance(item, str) for item in value)):
                continue
            timestamp, raw = value
            try:
                timestamp_nanoseconds = int(timestamp)
                timestamp_iso = datetime.fromtimestamp(timestamp_nanoseconds / 1_000_000_000, timezone.utc).isoformat()
            except (OverflowError, OSError, ValueError):
                continue
            message = _SECRET.sub(r"\1[REDACTED]", raw)[:4096]
            encoded = len(message.encode())
            if len(lines) >= _MAX_LINES or byte_count + encoded > _MAX_BYTES:
                return lines, last_timestamp, True
            lines.append(AuditLogLine(timestamp=timestamp_iso, stream=source[:256], message=message))
            byte_count += encoded
            last_timestamp = timestamp_nanoseconds
    return lines, last_timestamp, False
