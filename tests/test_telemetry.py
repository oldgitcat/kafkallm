from hermes.telemetry import extract_trace_context


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
