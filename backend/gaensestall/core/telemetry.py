from __future__ import annotations

import uuid
from typing import Any

from gaensestall import __version__
from gaensestall.core.settings import Settings
from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Span, Status, StatusCode

_provider: TracerProvider | None = None
_capture_content = False


def configure_telemetry(settings: Settings) -> None:
    global _provider, _capture_content
    _capture_content = settings.telemetry_capture_content
    if _provider is not None:
        return

    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "service.version": __version__,
            "service.instance.id": settings.worker_id,
        }
    )
    # Goose stream activity is recorded as span events, so allow more than the
    # SDK default of 128 events per span.
    provider = TracerProvider(
        resource=resource,
        span_limits=SpanLimits(max_events=512),
    )
    if settings.mlflow_enabled:
        endpoint = _traces_endpoint(settings.mlflow_tracking_uri)
        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            headers={"x-mlflow-experiment-id": settings.mlflow_experiment_id},
            timeout=2,
        )
        provider.add_span_processor(
            BatchSpanProcessor(exporter, schedule_delay_millis=500)
        )
    trace.set_tracer_provider(provider)
    _provider = provider


def _traces_endpoint(base: str) -> str:
    endpoint = base.rstrip("/")
    if not endpoint.endswith("/v1/traces"):
        endpoint = f"{endpoint}/v1/traces"
    return endpoint


def shutdown_telemetry() -> None:
    global _provider
    if _provider is not None:
        _provider.shutdown()
        _provider = None


def get_tracer() -> trace.Tracer:
    return trace.get_tracer("gaensestall", __version__)


def set_content(span: Span, key: str, value: str | None) -> None:
    if _capture_content and value is not None:
        span.set_attribute(key, value)


def set_session(span: Span, session_id: str) -> None:
    """Group all spans of one job into an MLflow trace session.

    MLflow maps the OpenTelemetry ``session.id`` attribute to the trace metadata
    key ``mlflow.trace.session``, which drives the Sessions view.
    """
    span.set_attribute("session.id", session_id)


def set_error(span: Span, error: Exception) -> None:
    span.record_exception(error)
    span.set_status(Status(StatusCode.ERROR, str(error)[:1000]))


def current_trace_id() -> str | None:
    context = trace.get_current_span().get_span_context()
    if not context or not context.is_valid:
        return None
    return f"{context.trace_id:032x}"


def new_trace_id() -> str:
    return uuid.uuid4().hex


def inject_trace_headers() -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(key, value.encode("utf-8")) for key, value in carrier.items()]


def extract_trace_context(headers: Any) -> Any:
    carrier: dict[str, str] = {}
    for key, value in headers or []:
        if value is None or key.lower() not in {"traceparent", "tracestate", "baggage"}:
            continue
        try:
            carrier[key] = (
                value.decode("utf-8") if isinstance(value, bytes) else str(value)
            )
        except UnicodeDecodeError:
            continue
    return propagate.extract(carrier)
