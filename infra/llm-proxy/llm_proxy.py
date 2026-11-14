"""HTTP allowlisting LLM proxy for the isolated goose job network.

The job microVM is attached only to an IPv6-only, internal Podman network. This
proxy is the only other container on that network and forwards to one fixed
upstream: the MLflow AI Gateway listener on the Compose network.

The proxy understands just enough HTTP to accept the endpoints goose needs and
reject everything else. Each client connection carries exactly one request; the
proxy validates the proxy token and normalizes the framing, rewrites it to the
MLflow gateway path and ``Connection: close``, and streams the response. A job
can therefore reach the model gateway API and nothing else, including paths
that MLflow might expose for tracking, administration, or tracing.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import os
import re
import sys
from urllib.parse import unquote, urlsplit

_READ_SIZE = 64 * 1024
_MAX_HEAD_BYTES = 32 * 1024
_MAX_BODY_BYTES = 16 * 1024 * 1024
_HEAD_TIMEOUT_SECONDS = 15.0

# Per-job proxies receive the job span's W3C trace context and job id. They
# overwrite any client-supplied values so a job cannot spoof another job's trace
# or attach itself to a different session.
_TRACEPARENT = os.getenv("GAENSESTALL_TRACEPARENT", "").strip()
_JOB_ID = os.getenv("GAENSESTALL_JOB_ID", "").strip()
_PROXY_TOKEN = os.getenv("GAENSESTALL_LLM_PROXY_TOKEN", "").strip()
_TRACEPARENT_RE = re.compile(r"^[0-9a-f]{2}-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$")
_JOB_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
if _TRACEPARENT and not _TRACEPARENT_RE.fullmatch(_TRACEPARENT):
    print("LLM proxy: ignoring malformed GAENSESTALL_TRACEPARENT", file=sys.stderr)
    _TRACEPARENT = ""
if _JOB_ID and not _JOB_ID_RE.fullmatch(_JOB_ID):
    print("LLM proxy: ignoring malformed GAENSESTALL_JOB_ID", file=sys.stderr)
    _JOB_ID = ""

# goose uses the OpenAI chat-completions protocol. Some versions probe the
# model list; nothing else is needed by a job.
_ALLOWED_EXACT = {
    ("POST", "/v1/chat/completions"),
    ("GET", "/v1/models"),
}
_ALLOWED_PREFIXES = {
    ("GET", "/v1/models/"),
}

# MLflow exposes the OpenAI-compatible gateway below /gateway/mlflow/v1. The
# job only ever sees the plain OpenAI paths.
_UPSTREAM_PATHS = {
    "/v1/chat/completions": "/gateway/mlflow/v1/chat/completions",
    "/v1/models": "/gateway/mlflow/v1/models",
}
_MODELS_PREFIX = "/v1/models/"

_REASONS = {
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    408: "Request Timeout",
    411: "Length Required",
    413: "Payload Too Large",
    431: "Request Header Fields Too Large",
    502: "Bad Gateway",
}

# Headers that describe one connection and must never be forwarded. Request
# framing headers are handled separately so a job cannot smuggle a second
# request past the allowlist.
_HOP_BY_HOP = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"proxy-connection",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}


class _RequestError(ValueError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


async def _read_request_head(
    reader: asyncio.StreamReader,
) -> tuple[bytes, bytes] | None:
    """Read one HTTP request head and any body bytes already buffered."""
    data = bytearray()
    while True:
        marker = data.find(b"\r\n\r\n")
        if marker >= 0:
            if marker + 4 > _MAX_HEAD_BYTES:
                raise _RequestError(431, "request header is too large")
            return bytes(data[: marker + 4]), bytes(data[marker + 4 :])
        if len(data) > _MAX_HEAD_BYTES:
            raise _RequestError(431, "request header is too large")
        chunk = await reader.read(4096)
        if not chunk:
            if not data:
                return None
            raise _RequestError(400, "incomplete request header")
        data.extend(chunk)


def _valid_path(path: str) -> bool:
    if not path.startswith("/") or not path.isascii() or "\\" in path:
        return False
    if any(char <= " " or char == "\x7f" for char in path):
        return False
    segments = path.split("/")[1:]
    if path.endswith("/") and segments:
        segments = segments[:-1]
    return not any(segment in {"", ".", ".."} for segment in segments)


def _parse_request(head: bytes) -> tuple[str, str, str, int]:
    """Return ``(method, path, query, content_length)`` for an allowed request."""
    lines = head[: -len(b"\r\n\r\n")].decode("iso-8859-1").split("\r\n")
    if not lines or not lines[0]:
        raise _RequestError(400, "missing request line")
    parts = lines[0].split(" ")
    if len(parts) != 3:
        raise _RequestError(400, "malformed request line")
    method, target, version = parts
    if version not in {"HTTP/1.0", "HTTP/1.1"}:
        raise _RequestError(400, "unsupported HTTP version")
    if (
        not method
        or not method.isascii()
        or any(ord(char) < 33 or ord(char) > 126 for char in method)
    ):
        raise _RequestError(400, "malformed request method")
    if not target or "\x00" in target or "\r" in target or "\n" in target:
        raise _RequestError(400, "malformed request target")
    if target.startswith("//"):
        raise _RequestError(400, "malformed request target")
    if target.startswith(("http://", "https://")):
        raise _RequestError(400, "only origin-form targets are accepted")
    if not target.startswith("/"):
        raise _RequestError(400, "only origin-form targets are accepted")
    try:
        split = urlsplit(target)
    except ValueError as exc:
        raise _RequestError(400, "malformed request target") from exc
    path = unquote(split.path or "/")
    # A residual percent sign means the raw target was double encoded. Reject
    # it instead of forwarding a value that the upstream could decode again
    # into ``..`` and route outside the allowlisted prefix.
    if "%" in path:
        raise _RequestError(400, "double-encoded path is not allowed")
    if not _valid_path(path):
        raise _RequestError(400, "path traversal is not allowed")
    query = split.query
    if not query.isascii() or any(char <= " " or char == "\x7f" for char in query):
        raise _RequestError(400, "malformed query string")

    content_length: int | None = None
    for line in lines[1:]:
        if not line or line[0] in {" ", "\t"}:
            raise _RequestError(400, "malformed request header")
        name, separator, value = line.partition(":")
        if not separator or not name:
            raise _RequestError(400, "malformed request header")
        if any(ord(char) < 33 or ord(char) > 126 for char in name):
            raise _RequestError(400, "malformed request header name")
        # Keep CR, LF, NUL, and other controls out of values. A bare LF would
        # otherwise survive the rewrite and let the upstream parse an injected
        # header such as a second Content-Length.
        if any((ord(char) < 32 and char != "\t") or ord(char) == 127 for char in value):
            raise _RequestError(400, "malformed request header value")
        lowered = name.lower()
        if lowered == "transfer-encoding":
            raise _RequestError(400, "transfer encoding is not allowed")
        if lowered == "content-length":
            raw = value.strip()
            if not raw.isascii() or not raw.isdigit():
                raise _RequestError(400, "malformed content length")
            try:
                length = int(raw)
            except ValueError as exc:
                raise _RequestError(400, "malformed content length") from exc
            if length > _MAX_BODY_BYTES:
                raise _RequestError(413, "request body is too large")
            if content_length is not None and content_length != length:
                raise _RequestError(400, "conflicting content lengths")
            content_length = length
    if method != "POST" and content_length not in {None, 0}:
        raise _RequestError(400, "this method does not accept a body")
    allowed = (method, path) in _ALLOWED_EXACT or any(
        method == allowed_method and path.startswith(prefix)
        for allowed_method, prefix in _ALLOWED_PREFIXES
    )
    if not allowed:
        raise _RequestError(403, f"{method} {path} is not allowed")
    return method, path, query, content_length or 0


def _authorized(head: bytes) -> bool:
    """Return True when the request carries the configured proxy token."""
    if not _PROXY_TOKEN:
        return True
    for line in head[: -len(b"\r\n\r\n")].split(b"\r\n")[1:]:
        name, separator, value = line.partition(b":")
        if not separator or name.strip().lower() != b"authorization":
            continue
        scheme, _, token = value.strip().decode("iso-8859-1").partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(token, _PROXY_TOKEN)
    return False


def _upstream_path(path: str) -> str:
    """Map the job-facing OpenAI path to MLflow's gateway route."""
    if path.startswith(_MODELS_PREFIX):
        return _UPSTREAM_PATHS["/v1/models"]
    try:
        return _UPSTREAM_PATHS[path]
    except KeyError as exc:
        raise _RequestError(403, f"{path} is not allowed") from exc


def _rewrite_request(
    head: bytes,
    method: str,
    path: str,
    query: str,
    content_length: int,
    upstream_authority: str,
) -> bytes:
    lines = head[: -len(b"\r\n\r\n")].split(b"\r\n")[1:]
    kept: list[bytes] = []
    for line in lines:
        name = line.split(b":", 1)[0].strip().lower()
        if name in _HOP_BY_HOP or name in {
            b"content-length",
            b"host",
            b"traceparent",
            b"x-gaensestall-job-id",
            # The proxy token is validated here. MLflow does not need it, and
            # MLflow's own RBAC header must never come from the job.
            b"authorization",
            b"x-mlflow-authorization",
        }:
            continue
        kept.append(line)
    kept.append(f"Host: {upstream_authority}".encode("ascii"))
    if _TRACEPARENT:
        kept.append(f"traceparent: {_TRACEPARENT}".encode("ascii"))
    if _JOB_ID:
        kept.append(f"x-gaensestall-job-id: {_JOB_ID}".encode("ascii"))
    if method == "POST":
        kept.append(f"Content-Length: {content_length}".encode("ascii"))
    target = _upstream_path(path) + (f"?{query}" if query else "")
    request_line = f"{method} {target} HTTP/1.1".encode("ascii")
    return b"\r\n".join([request_line, *kept, b"Connection: close"]) + b"\r\n\r\n"


async def _send_body(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    prefix: bytes,
    content_length: int,
) -> None:
    """Forward exactly the declared body, never bytes for a second request."""
    remaining = content_length
    buffered = prefix[:remaining]
    if buffered:
        writer.write(buffered)
        remaining -= len(buffered)
        await writer.drain()
    while remaining > 0:
        chunk = await reader.read(min(_READ_SIZE, remaining))
        if not chunk:
            raise ConnectionError("request body ended before its content length")
        writer.write(chunk)
        remaining -= len(chunk)
        await writer.drain()


async def _reject(writer: asyncio.StreamWriter, status: int, detail: str) -> None:
    body = f"LLM proxy: {detail}\n".encode("utf-8", errors="replace")
    reason = _REASONS.get(status, "Error")
    head = (
        f"HTTP/1.1 {status} {reason}\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).encode("ascii")
    try:
        writer.write(head + body)
        await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        writer.close()


async def _pipe(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    try:
        while data := await reader.read(_READ_SIZE):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError, asyncio.CancelledError):
        pass
    finally:
        writer.close()


async def _handle(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    upstream_host: str,
    upstream_port: int,
) -> None:
    try:
        request = await asyncio.wait_for(
            _read_request_head(client_reader), _HEAD_TIMEOUT_SECONDS
        )
    except TimeoutError:
        await _reject(client_writer, 408, "request header timed out")
        return
    except _RequestError as exc:
        await _reject(client_writer, exc.status, exc.detail)
        return
    if request is None:
        client_writer.close()
        return
    head, body_prefix = request
    try:
        method, path, query, content_length = _parse_request(head)
        if not _authorized(head):
            raise _RequestError(401, "invalid gateway key")
        rewritten = _rewrite_request(
            head,
            method,
            path,
            query,
            content_length,
            f"{upstream_host}:{upstream_port}",
        )
    except _RequestError as exc:
        print(
            f"LLM proxy: rejected request: {exc.detail}",
            file=sys.stderr,
            flush=True,
        )
        await _reject(client_writer, exc.status, exc.detail)
        return

    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            upstream_host, upstream_port
        )
    except OSError as exc:
        print(
            f"LLM proxy: upstream {upstream_host}:{upstream_port} "
            f"is unavailable: {exc}",
            file=sys.stderr,
            flush=True,
        )
        await _reject(client_writer, 502, "model gateway is unavailable")
        return

    try:
        upstream_writer.write(rewritten)
        await upstream_writer.drain()
        await _send_body(client_reader, upstream_writer, body_prefix, content_length)
        await _pipe(upstream_reader, client_writer)
    except (ConnectionError, OSError) as exc:
        print(
            f"LLM proxy: connection failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
    finally:
        upstream_writer.close()
        client_writer.close()


async def main() -> None:
    listen_port = int(os.environ.get("LISTEN_PORT", "8091"))
    upstream_host = os.environ.get("UPSTREAM_HOST", "host.containers.internal")
    upstream_port = int(os.environ.get("UPSTREAM_PORT", str(listen_port)))

    server = await asyncio.start_server(
        lambda reader, writer: _handle(reader, writer, upstream_host, upstream_port),
        host="::",
        port=listen_port,
        limit=_MAX_HEAD_BYTES + 4096,
    )
    print(
        f"LLM proxy: listening on [::]:{listen_port}, forwarding only "
        "OpenAI chat-completions/model-list requests to the MLflow AI Gateway "
        f"at {upstream_host}:{upstream_port}",
        file=sys.stderr,
        flush=True,
    )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
