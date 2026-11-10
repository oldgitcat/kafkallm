from __future__ import annotations

import asyncio
import importlib.util
import re
import socket
from pathlib import Path
from typing import Any

import pytest

_PROXY_PATH = Path(__file__).resolve().parents[1] / "goose_job" / "gateway_proxy.py"
_SPEC = importlib.util.spec_from_file_location("kafkallm_gateway_proxy", _PROXY_PATH)
assert _SPEC is not None and _SPEC.loader is not None
gateway_proxy = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(gateway_proxy)


def _head(
    method: str,
    target: str,
    headers: list[str] | None = None,
) -> bytes:
    lines = [f"{method} {target} HTTP/1.1", "Host: gateway"]
    lines.extend(headers or [])
    return ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")


def _parse(head: bytes) -> tuple[str, str, str, int]:
    return gateway_proxy._parse_request(head)


@pytest.mark.parametrize(
    ("method", "target"),
    [
        ("POST", "/v1/chat/completions"),
        ("POST", "/v1/chat/completions?trace=1"),
        ("GET", "/v1/models"),
        ("GET", "/v1/models/gpt-4o"),
        ("GET", "/v1/models/meta-llama/llama-3.1"),
        ("GET", "/v1/models/price%2B100"),
    ],
)
def test_allowed_endpoints_are_parsed(method: str, target: str) -> None:
    parsed_method, path, query, length = _parse(_head(method, target))

    assert parsed_method == method
    assert path.startswith("/v1/")
    assert query == ("trace=1" if target.endswith("trace=1") else "")
    assert length == 0


@pytest.mark.parametrize(
    ("method", "target", "headers", "status"),
    [
        ("GET", "/v1/chat/completions", [], 403),
        ("POST", "/v1/models", [], 403),
        ("OPTIONS", "/v1/models", [], 403),
        ("GET", "/admin", [], 403),
        ("CONNECT", "gateway:443", [], 400),
        ("GET", "/v1/models/../admin", [], 400),
        ("GET", "/v1/models/%2e%2e/admin", [], 400),
        ("GET", "/v1/models/%252e%252e/admin", [], 400),
        ("GET", "/v1/models/foo%252f..%252fadmin", [], 400),
        ("GET", "/v1/models/foo/..", [], 400),
        ("GET", "/v1/models/%2e", [], 400),
        ("GET", "/v1/models/%0d%0aInjected:%20yes", [], 400),
        ("GET", "//v1/models", [], 400),
        ("GET", "http://gateway/v1/models/gpt-4o", [], 400),
        ("GET", "http://[::1/v1/models", [], 400),
        ("GET", "/v1/models\\..\\admin", [], 400),
        ("POST", "/v1/chat/completions", ["Transfer-Encoding: chunked"], 400),
        (
            "POST",
            "/v1/chat/completions",
            ["Content-Length: 1", "Content-Length: 2"],
            400,
        ),
        ("POST", "/v1/chat/completions", ["Content-Length: nope"], 400),
        ("POST", "/v1/chat/completions", ["Content-Length: ²"], 400),
        ("POST", "/v1/chat/completions", ["Content-Length: " + "9" * 5000], 400),
        ("GET", "/v1/models", ["Content-Length: 1"], 400),
        ("GET", "/v1/models", ["Bad Header: value"], 400),
        ("GET", "/v1/models", [" folded: value"], 400),
        ("GET", "/v1/models", ["X-Test: a\x00b"], 400),
        ("GET", "/v1/models", ["X-Test: a\nb"], 400),
        ("GET", "/v1/models", ["X-Test: a\rb"], 400),
        ("GE\x1bT", "/v1/models", [], 400),
    ],
)
def test_disallowed_or_ambiguous_requests_are_rejected(
    method: str, target: str, headers: list[str], status: int
) -> None:
    with pytest.raises(gateway_proxy._RequestError) as error:
        _parse(_head(method, target, headers))

    assert error.value.status == status


def test_oversized_request_bodies_and_headers_are_rejected() -> None:
    too_large = str(gateway_proxy._MAX_BODY_BYTES + 1)
    with pytest.raises(gateway_proxy._RequestError) as error:
        _parse(_head("POST", "/v1/chat/completions", [f"Content-Length: {too_large}"]))
    assert error.value.status == 413

    async def read_large_head() -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"GET /v1/models HTTP/1.1\r\n" + b"X: y\r\n" * 6000)
        reader.feed_eof()
        await gateway_proxy._read_request_head(reader)

    with pytest.raises(gateway_proxy._RequestError) as error:
        asyncio.run(read_large_head())
    assert error.value.status == 431


def test_rewrite_forces_one_connection_close_and_one_content_length() -> None:
    head = _head(
        "POST",
        "/v1/chat/completions?trace=1",
        [
            "Content-Type: application/json",
            "Content-Length: 2",
            "Content-Length: 2",
            "Connection: keep-alive",
            "Proxy-Connection: keep-alive",
            "Transfer-Encoding: chunked",
        ],
    )
    # Transfer-Encoding is rejected by the parser, so remove it for the rewrite
    # test and verify that framing and hop-by-hop headers are normalized.
    head = head.replace(b"Transfer-Encoding: chunked\r\n", b"")
    method, path, query, length = _parse(head)

    rewritten = gateway_proxy._rewrite_request(head, method, path, query, length)

    assert rewritten.startswith(b"POST /v1/chat/completions?trace=1 HTTP/1.1\r\n")
    assert rewritten.count(b"Content-Length:") == 1
    assert b"Content-Length: 2\r\n" in rewritten
    assert b"Connection: close\r\n" in rewritten
    assert b"keep-alive" not in rewritten.lower()
    assert b"Proxy-Connection" not in rewritten


class _RecordingWriter:
    def __init__(self) -> None:
        self.data = bytearray()

    def write(self, data: bytes) -> None:
        self.data.extend(data)

    async def drain(self) -> None:
        return None


def test_send_body_forwards_exactly_the_declared_length() -> None:
    async def scenario() -> tuple[bytes, bytes]:
        reader = asyncio.StreamReader()
        reader.feed_data(b"cdefSMUGGLED")
        reader.feed_eof()
        writer = _RecordingWriter()
        await gateway_proxy._send_body(reader, writer, b"ab", 6)
        return bytes(writer.data), await reader.read(1024)

    body, leftover = asyncio.run(scenario())

    assert body == b"abcdef"
    assert leftover == b"SMUGGLED"


def test_send_body_rejects_a_truncated_body() -> None:
    async def scenario() -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(b"ab")
        reader.feed_eof()
        writer = _RecordingWriter()
        await gateway_proxy._send_body(reader, writer, b"", 4)

    with pytest.raises(ConnectionError):
        asyncio.run(scenario())


async def _start_upstream(
    received: dict[str, Any],
) -> tuple[asyncio.AbstractServer, int]:
    async def handle(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        text = head.decode("iso-8859-1")
        match = re.search(r"(?i)content-length:\s*(\d+)", text)
        length = int(match.group(1)) if match else 0
        body = await reader.readexactly(length)
        received["head"] = text
        received["body"] = body
        writer.write(
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 2\r\n"
            b"Connection: close\r\n\r\nok"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def _proxy_roundtrip(
    request: bytes,
    upstream_port: int,
) -> bytes:
    server = await asyncio.start_server(
        lambda reader, writer: gateway_proxy._handle(
            reader, writer, "127.0.0.1", upstream_port
        ),
        "127.0.0.1",
        0,
    )
    port = server.sockets[0].getsockname()[1]
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.write(request)
        await writer.drain()
        return await asyncio.wait_for(reader.read(), 5)
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


def test_allowed_request_is_forwarded_and_response_streamed_back() -> None:
    received: dict[str, Any] = {}

    async def scenario() -> tuple[dict[str, Any], bytes]:
        upstream, upstream_port = await _start_upstream(received)
        try:
            request = (
                _head(
                    "POST",
                    "/v1/chat/completions?trace=1",
                    [
                        "Content-Type: application/json",
                        "Content-Length: 2",
                        "Connection: keep-alive",
                    ],
                )
                + b"{}"
            )
            response = await _proxy_roundtrip(request, upstream_port)
        finally:
            upstream.close()
            await upstream.wait_closed()
        return received, response

    received, response = asyncio.run(scenario())

    assert response.endswith(b"ok")
    assert received["body"] == b"{}"
    assert received["head"].startswith("POST /v1/chat/completions?trace=1 HTTP/1.1\r\n")
    assert "Connection: close" in received["head"]
    assert "keep-alive" not in received["head"].lower()
    assert received["head"].count("Content-Length:") == 1
    assert "Content-Length: 2" in received["head"]


def test_forbidden_request_never_reaches_the_upstream() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]

    response = asyncio.run(_proxy_roundtrip(_head("GET", "/v1/admin"), closed_port))

    assert response.startswith(b"HTTP/1.1 403 Forbidden")
    assert b"not allowed" in response
