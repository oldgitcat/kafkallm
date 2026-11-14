from gaensestall.core.telemetry import extract_trace_context, set_session
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)


def test_trace_extraction_ignores_unrelated_binary_headers() -> None:
    headers = [
        ("binary", b"\xff\x00"),
        (
            "traceparent",
            b"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        ),
    ]

    context = extract_trace_context(headers)

    assert context is not None


def test_set_session_marks_the_mlflow_session_attribute() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    span = provider.get_tracer("test").start_span("job.handle")

    set_session(span, "job-123")
    span.end()

    finished = exporter.get_finished_spans()[0]
    assert finished.attributes["session.id"] == "job-123"
