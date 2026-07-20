"""Safe, explicitly opt-in OTLP tracing and metrics for the API service."""

from __future__ import annotations

import json
import os
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlparse

from opentelemetry import metrics, propagate, trace
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter as GrpcMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as GrpcSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HttpMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HttpSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


_ALLOWED_RESOURCE_PREFIXES = ("service.", "deployment.", "cogito.")


@dataclass(frozen=True)
class TelemetrySettings:
    traces_enabled: bool = False
    metrics_enabled: bool = False
    span_events_enabled: bool = False
    endpoint: str = ""
    protocol: str = "http/protobuf"
    insecure: bool = True
    headers: str = ""
    service_name: str = "cogito-api"
    deployment_environment: str = "development"
    metric_export_interval_millis: int = 60_000
    resource_attributes: Mapping[str, str] | None = None

    @classmethod
    def from_environment(cls) -> "TelemetrySettings":
        attributes = _resource_attributes(os.environ.get("COGITO_TELEMETRY_RESOURCE_ATTRIBUTES", "{}"))
        settings = cls(
            traces_enabled=_as_bool("COGITO_TELEMETRY_TRACES_ENABLED"),
            metrics_enabled=_as_bool("COGITO_TELEMETRY_METRICS_ENABLED"),
            span_events_enabled=_as_bool("COGITO_TELEMETRY_SPAN_EVENTS_ENABLED"),
            endpoint=os.environ.get("COGITO_TELEMETRY_OTLP_ENDPOINT", ""),
            protocol=os.environ.get("COGITO_TELEMETRY_OTLP_PROTOCOL", "http/protobuf"),
            insecure=_as_bool("COGITO_TELEMETRY_OTLP_INSECURE", default=True),
            headers=os.environ.get("COGITO_TELEMETRY_OTLP_HEADERS", ""),
            service_name=os.environ.get("COGITO_TELEMETRY_SERVICE_NAME", "cogito-api"),
            deployment_environment=os.environ.get("COGITO_DEPLOYMENT_MODE", "development"),
            metric_export_interval_millis=_positive_int("COGITO_TELEMETRY_METRIC_EXPORT_INTERVAL_MILLIS", 60_000),
            resource_attributes=attributes,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not (self.traces_enabled or self.metrics_enabled):
            return
        if self.protocol not in {"http/protobuf", "grpc"}:
            raise ValueError("COGITO_TELEMETRY_OTLP_PROTOCOL must be http/protobuf or grpc")
        parsed = urlparse(self.endpoint)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("COGITO_TELEMETRY_OTLP_ENDPOINT must be an absolute HTTP(S) URL when telemetry is enabled")
        if not self.service_name.strip():
            raise ValueError("COGITO_TELEMETRY_SERVICE_NAME must not be empty")


class Telemetry:
    """Owns local SDK providers without changing process-global providers."""

    def __init__(self, settings: TelemetrySettings):
        self._settings = settings
        self._tracer_provider: TracerProvider | None = None
        self._meter_provider: MeterProvider | None = None
        resource = Resource.create(
            {
                "service.name": settings.service_name,
                "deployment.environment.name": settings.deployment_environment,
                **dict(settings.resource_attributes or {}),
            }
        )
        self._tracer = trace.get_tracer("cogito.api")
        self._meter = metrics.get_meter("cogito.api")
        self._runs = None
        self._requests = None
        if settings.traces_enabled:
            self._tracer_provider = TracerProvider(resource=resource)
            self._tracer_provider.add_span_processor(BatchSpanProcessor(_span_exporter(settings)))
            self._tracer = self._tracer_provider.get_tracer("cogito.api")
        if settings.metrics_enabled:
            self._meter_provider = MeterProvider(
                resource=resource,
                metric_readers=[
                    PeriodicExportingMetricReader(
                        _metric_exporter(settings), export_interval_millis=settings.metric_export_interval_millis
                    )
                ],
            )
            self._meter = self._meter_provider.get_meter("cogito.api")
            self._runs = self._meter.create_counter("cogito.run.transitions", unit="1")
            self._requests = self._meter.create_counter("cogito.api.requests", unit="1")

    @property
    def enabled(self) -> bool:
        return self._tracer_provider is not None or self._meter_provider is not None

    def span(self, name: str, attributes: Mapping[str, str] | None = None) -> AbstractContextManager:
        return self._tracer.start_as_current_span(name, attributes=dict(attributes or {}))

    def extract(self, carrier: Mapping[str, str]):
        return propagate.extract(dict(carrier))

    def inject(self, carrier: dict[str, str]) -> None:
        propagate.inject(carrier)

    def trace_id(self) -> str:
        span_context = trace.get_current_span().get_span_context()
        return f"{span_context.trace_id:032x}" if span_context.is_valid else ""

    def transition(self, state: str, agent: str) -> None:
        if self._runs is not None:
            self._runs.add(1, {"cogito.state": state, "cogito.agent": agent})

    def request(self, method: str, status_code: int) -> None:
        if self._requests is not None:
            self._requests.add(1, {"http.request.method": method, "http.response.status_code": status_code})

    def event(self, name: str, attributes: Mapping[str, str] | None = None) -> None:
        if self._settings.span_events_enabled:
            trace.get_current_span().add_event(name, dict(attributes or {}))

    def shutdown(self) -> None:
        if self._tracer_provider is not None:
            self._tracer_provider.shutdown()
        if self._meter_provider is not None:
            self._meter_provider.shutdown()


def _as_bool(name: str, default: bool = False) -> bool:
    return os.environ.get(name, str(default)).lower() == "true"


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value < 1_000:
        raise ValueError(f"{name} must be at least 1000")
    return value


def _resource_attributes(value: str) -> dict[str, str]:
    try:
        attributes = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValueError("COGITO_TELEMETRY_RESOURCE_ATTRIBUTES must be a JSON object") from error
    if not isinstance(attributes, dict) or not all(
        isinstance(key, str)
        and isinstance(item, str)
        and key.startswith(_ALLOWED_RESOURCE_PREFIXES)
        and len(item) <= 256
        for key, item in attributes.items()
    ):
        raise ValueError("telemetry resource attributes must be short strings with service., deployment., or cogito. keys")
    return attributes


def _headers(value: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in filter(None, (part.strip() for part in value.split(","))):
        key, separator, header_value = item.partition("=")
        if not separator or not key.strip() or not header_value.strip():
            raise ValueError("COGITO_TELEMETRY_OTLP_HEADERS must be comma-separated key=value pairs")
        headers[key.strip()] = header_value.strip()
    return headers


def _span_exporter(settings: TelemetrySettings):
    headers = _headers(settings.headers)
    if settings.protocol == "grpc":
        return GrpcSpanExporter(endpoint=settings.endpoint.removeprefix("http://").removeprefix("https://"), insecure=settings.insecure, headers=headers)
    return HttpSpanExporter(endpoint=f"{settings.endpoint.rstrip('/')}/v1/traces", headers=headers)


def _metric_exporter(settings: TelemetrySettings):
    headers = _headers(settings.headers)
    if settings.protocol == "grpc":
        return GrpcMetricExporter(endpoint=settings.endpoint.removeprefix("http://").removeprefix("https://"), insecure=settings.insecure, headers=headers)
    return HttpMetricExporter(endpoint=f"{settings.endpoint.rstrip('/')}/v1/metrics", headers=headers)
