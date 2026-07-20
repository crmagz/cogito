"""Minimal worker-side OTLP tracing, disabled unless explicitly configured."""

from __future__ import annotations

import os
from contextlib import AbstractContextManager
from urllib.parse import urlparse

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as GrpcSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HttpSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


class WorkerTelemetry:
    def __init__(self) -> None:
        self._provider: TracerProvider | None = None
        self._tracer = trace.get_tracer("cogito.worker")
        if os.environ.get("COGITO_TELEMETRY_TRACES_ENABLED", "false").lower() != "true":
            return
        endpoint = os.environ.get("COGITO_TELEMETRY_OTLP_ENDPOINT", "")
        protocol = os.environ.get("COGITO_TELEMETRY_OTLP_PROTOCOL", "http/protobuf")
        parsed = urlparse(endpoint)
        if protocol not in {"http/protobuf", "grpc"} or parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("enabled worker telemetry requires a valid OTLP endpoint and protocol")
        resource = Resource.create(
            {
                "service.name": "cogito-worker",
                "deployment.environment.name": os.environ.get("COGITO_DEPLOYMENT_MODE", "development"),
            }
        )
        self._provider = TracerProvider(resource=resource)
        headers = _headers(os.environ.get("COGITO_TELEMETRY_OTLP_HEADERS", ""))
        if protocol == "grpc":
            exporter = GrpcSpanExporter(
                endpoint=endpoint.removeprefix("http://").removeprefix("https://"),
                insecure=os.environ.get("COGITO_TELEMETRY_OTLP_INSECURE", "true").lower() == "true",
                headers=headers,
            )
        else:
            exporter = HttpSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces", headers=headers)
        self._provider.add_span_processor(BatchSpanProcessor(exporter))
        self._tracer = self._provider.get_tracer("cogito.worker")

    def span(self, name: str, traceparent: str | None = None, tracestate: str | None = None) -> AbstractContextManager:
        carrier = {key: value for key, value in {"traceparent": traceparent, "tracestate": tracestate}.items() if value}
        return self._tracer.start_as_current_span(name, context=propagate.extract(carrier))

    def shutdown(self) -> None:
        if self._provider is not None:
            self._provider.shutdown()


def _headers(value: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for part in filter(None, (item.strip() for item in value.split(","))):
        key, separator, header_value = part.partition("=")
        if not separator or not key.strip() or not header_value.strip():
            raise ValueError("COGITO_TELEMETRY_OTLP_HEADERS must contain key=value pairs")
        headers[key.strip()] = header_value.strip()
    return headers
