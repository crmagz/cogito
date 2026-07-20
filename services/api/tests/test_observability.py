from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from cogito_api.observability import Telemetry, TelemetrySettings


def test_disabled_telemetry_creates_no_sdk_providers() -> None:
    telemetry = Telemetry(TelemetrySettings())

    assert telemetry.enabled is False
    assert telemetry.trace_id() == ""


@pytest.mark.parametrize("protocol", ["http/protobuf", "grpc"])
def test_enabled_telemetry_accepts_supported_protocols(protocol: str) -> None:
    TelemetrySettings(traces_enabled=True, endpoint="http://collector:4318", protocol=protocol).validate()


def test_enabled_telemetry_rejects_missing_endpoint() -> None:
    with pytest.raises(ValueError, match="absolute HTTP"):
        TelemetrySettings(traces_enabled=True).validate()


def test_resource_attributes_are_constrained(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COGITO_TELEMETRY_RESOURCE_ATTRIBUTES", '{"bad.key":"value"}')

    with pytest.raises(ValueError, match="resource attributes"):
        TelemetrySettings.from_environment()


def test_otlp_http_exporter_sends_a_trace_to_an_external_receiver() -> None:
    received: list[tuple[str, bytes]] = []

    class Receiver(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler signature
            received.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), Receiver)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    telemetry = Telemetry(
        TelemetrySettings(
            traces_enabled=True,
            endpoint=f"http://127.0.0.1:{server.server_port}",
            resource_attributes={"cogito.validation": "test"},
        )
    )
    try:
        with telemetry.span("cogito.api.request", {"http.request.method": "GET"}):
            pass
    finally:
        telemetry.shutdown()
        server.shutdown()
        thread.join()

    assert received and received[0][0] == "/v1/traces"
    assert received[0][1]
