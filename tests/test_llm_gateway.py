from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from kafkallm.llm_gateway import (
    GatewayServer,
    Upstream,
    issue_token,
    verify_token,
)
from kafkallm.settings import Settings


class FakeUpstreamServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: tuple[str, int]) -> None:
        super().__init__(address, FakeUpstreamHandler)
        self.requests: list[dict[str, Any]] = []
        self.status = 200
        self.payload: dict[str, Any] = {
            "id": "chatcmpl-1",
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "PONG"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 3,
                "completion_tokens": 1,
                "total_tokens": 4,
            },
        }
        self.stream = False


class FakeUpstreamHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        del format, args

    def do_GET(self) -> None:  # noqa: N802
        self._handle()

    def do_POST(self) -> None:  # noqa: N802
        self._handle()

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        self.server.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "body": body,
            }
        )
        if self.server.stream:
            self.send_response(self.server.status)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(
                b'data: {"choices":[{"delta":{"content":"PONG"}}]}\n\n'
            )
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            self.close_connection = True
            return
        data = json.dumps(self.server.payload).encode("utf-8")
        self.send_response(self.server.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.close_connection = True


def _settings(upstream_url: str, **overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "llm_gateway_host": "127.0.0.1",
        "llm_gateway_port": 0,
        "llm_gateway_secret": "test-gateway-secret-0123456789",
        "llm_gateway_upstream_url": upstream_url,
        "llm_gateway_upstream_api_key": "upstream-secret",
        "llm_gateway_upstream_auth_header": "Authorization",
        "llm_gateway_upstream_auth_scheme": "Bearer",
        "llm_gateway_upstream_timeout_seconds": 5,
        "llm_gateway_max_request_bytes": 1024 * 1024,
    }
    values.update(overrides)
    return replace(Settings.from_env(), **values)


def _start(server: Any) -> threading.Thread:
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def _stop(server: Any, thread: threading.Thread) -> None:
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _request(
    url: str,
    *,
    token: str | None = None,
    payload: dict[str, Any] | None = None,
    method: str = "POST",
) -> tuple[int, dict[str, str], bytes]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = urllib.request.Request(
        url, data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def _gateway(
    upstream_url: str, **overrides: Any
) -> tuple[GatewayServer, threading.Thread, Settings]:
    settings = _settings(upstream_url, **overrides)
    server = GatewayServer(
        ("127.0.0.1", 0),
        settings,
        Upstream.from_settings(settings),
    )
    return server, _start(server), settings


def _base_url(server: Any) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}"


def test_token_round_trip_tampering_and_expiry() -> None:
    secret = "test-gateway-secret-0123456789"
    token = issue_token(secret, "job-1", 60, now=1000)

    claims = verify_token(secret, token, now=1000)
    assert claims is not None
    assert claims.job_id == "job-1"
    assert claims.expires_at == 1060

    assert verify_token(secret, token, now=1061) is None
    assert verify_token("another-secret", token, now=1000) is None
    assert verify_token(secret, token + "x", now=1000) is None
    assert verify_token(secret, "not-a-token", now=1000) is None


def test_upstream_path_mapping() -> None:
    versioned = Upstream.from_settings(
        _settings("https://openrouter.ai/api/v1?api-version=1")
    )
    assert (
        versioned.path_for("/v1/chat/completions")
        == "/api/v1/chat/completions?api-version=1"
    )
    assert (
        versioned.path_for("/v1/chat/completions", "stream=true")
        == "/api/v1/chat/completions?api-version=1&stream=true"
    )
    assert versioned.path_for("/v1/models") == "/api/v1/models?api-version=1"

    root = Upstream.from_settings(_settings("http://127.0.0.1:8080"))
    assert root.path_for("/v1/chat/completions") == "/v1/chat/completions"


def test_gateway_proxies_and_injects_the_provider_key() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream_thread = _start(upstream)
    gateway, gateway_thread, settings = _gateway(_base_url(upstream))
    token = issue_token(settings.llm_gateway_secret, "job-42", 60)
    try:
        status, headers, body = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token=token,
            payload={"model": "test-model", "messages": []},
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert status == 200
    assert "application/json" in headers["Content-Type"]
    assert json.loads(body)["choices"][0]["message"]["content"] == "PONG"
    assert len(upstream.requests) == 1
    request = upstream.requests[0]
    assert request["path"] == "/v1/chat/completions"
    assert request["authorization"] == "Bearer upstream-secret"
    assert token.encode() not in request["body"]


def test_gateway_rejects_missing_invalid_and_expired_tokens() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream_thread = _start(upstream)
    gateway, gateway_thread, settings = _gateway(_base_url(upstream))
    expired = issue_token(
        settings.llm_gateway_secret, "job-1", 60, now=1000
    )
    try:
        missing = _request(f"{_base_url(gateway)}/v1/chat/completions", payload={})
        invalid = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token="not-a-token",
            payload={},
        )
        old = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token=expired,
            payload={},
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert missing[0] == 401
    assert invalid[0] == 401
    assert old[0] == 401
    assert upstream.requests == []


def test_gateway_forwards_upstream_errors() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream.status = 429
    upstream.payload = {"error": {"message": "rate limited"}}
    upstream_thread = _start(upstream)
    gateway, gateway_thread, settings = _gateway(_base_url(upstream))
    token = issue_token(settings.llm_gateway_secret, "job-1", 60)
    try:
        status, _, body = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token=token,
            payload={},
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert status == 429
    assert json.loads(body)["error"]["message"] == "rate limited"


def test_gateway_streams_sse_responses() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream.stream = True
    upstream_thread = _start(upstream)
    gateway, gateway_thread, settings = _gateway(_base_url(upstream))
    token = issue_token(settings.llm_gateway_secret, "job-1", 60)
    try:
        status, headers, body = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token=token,
            payload={"model": "test-model", "stream": True},
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert status == 200
    assert "text/event-stream" in headers["Content-Type"]
    assert b"PONG" in body
    assert b"[DONE]" in body


def test_gateway_rejects_oversized_requests() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream_thread = _start(upstream)
    gateway, gateway_thread, settings = _gateway(
        _base_url(upstream), llm_gateway_max_request_bytes=10
    )
    token = issue_token(settings.llm_gateway_secret, "job-1", 60)
    try:
        status, _, _ = _request(
            f"{_base_url(gateway)}/v1/chat/completions",
            token=token,
            payload={"model": "test-model", "messages": []},
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert status == 413
    assert upstream.requests == []


def test_gateway_health_does_not_require_a_token() -> None:
    upstream = FakeUpstreamServer(("127.0.0.1", 0))
    upstream_thread = _start(upstream)
    gateway, gateway_thread, _ = _gateway(_base_url(upstream))
    try:
        status, _, body = _request(
            f"{_base_url(gateway)}/healthz", method="GET"
        )
    finally:
        _stop(gateway, gateway_thread)
        _stop(upstream, upstream_thread)

    assert status == 200
    assert json.loads(body) == {"status": "ok"}


def test_upstream_uses_a_custom_auth_header_without_a_scheme() -> None:
    upstream = Upstream.from_settings(
        _settings(
            "https://example.com/v1",
            llm_gateway_upstream_auth_header="api-key",
            llm_gateway_upstream_auth_scheme="",
        )
    )

    headers = upstream.headers_for(
        {
            "Authorization": "Bearer job-token",
            "x-api-key": "job-token",
            "Content-Type": "application/json",
        }
    )

    assert headers["api-key"] == "upstream-secret"
    assert "Authorization" not in headers
    assert "x-api-key" not in headers
    assert headers["Content-Type"] == "application/json"


def test_upstream_requires_an_auth_header_when_a_key_is_set() -> None:
    with pytest.raises(RuntimeError, match="AUTH_HEADER"):
        Upstream.from_settings(
            _settings(
                "https://example.com/v1",
                llm_gateway_upstream_auth_header=" ",
            )
        )


def test_gateway_rejects_an_invalid_upstream_url() -> None:
    with pytest.raises(RuntimeError, match="absolute http"):
        Upstream.from_settings(_settings("not-a-url"))
