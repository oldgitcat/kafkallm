from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import signal
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from kafkallm.settings import Settings


logger = logging.getLogger(__name__)

_TOKEN_PREFIX = "kg1"
_CHUNK_SIZE = 64 * 1024
_HOP_BY_HOP_HEADERS = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }
)
_REQUEST_HEADERS_NOT_FORWARDED = frozenset(
    {
        "authorization",
        "content-length",
        "expect",
        "host",
        "x-api-key",
    }
)


class GatewayConfigError(RuntimeError):
    pass


class _RequestTooLarge(Exception):
    pass


@dataclass(frozen=True, slots=True)
class GatewayToken:
    job_id: str
    expires_at: int


@dataclass(frozen=True, slots=True)
class Upstream:
    """An OpenAI-compatible upstream endpoint.

    ``base_path`` is the URL path of the OpenAI SDK base URL, for example
    ``/api/v1`` for OpenRouter or ``/v1`` for OpenAI. Requests to the gateway
    are accepted under ``/v1/...`` and mapped onto that base path.
    """

    scheme: str
    host: str
    port: int
    host_header: str
    base_path: str
    query: str
    api_key: str = field(repr=False)
    auth_header: str = "Authorization"
    auth_scheme: str = "Bearer"
    timeout_seconds: float = 600

    @classmethod
    def from_settings(cls, settings: Settings) -> "Upstream":
        raw_url = settings.llm_gateway_upstream_url.strip()
        parts = urlsplit(raw_url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise GatewayConfigError(
                "LLM_GATEWAY_UPSTREAM_URL must be an absolute http(s) URL."
            )
        if parts.username or parts.password:
            raise GatewayConfigError(
                "LLM_GATEWAY_UPSTREAM_URL must not contain credentials."
            )
        if (
            settings.llm_gateway_upstream_api_key
            and not settings.llm_gateway_upstream_auth_header.strip()
        ):
            raise GatewayConfigError(
                "LLM_GATEWAY_UPSTREAM_AUTH_HEADER must not be empty when an "
                "upstream API key is configured."
            )
        default_port = 443 if parts.scheme == "https" else 80
        port = parts.port or default_port
        host_header = parts.hostname
        if ":" in host_header and not host_header.startswith("["):
            host_header = f"[{host_header}]"
        if port != default_port:
            host_header = f"{host_header}:{port}"
        return cls(
            scheme=parts.scheme,
            host=parts.hostname,
            port=port,
            host_header=host_header,
            base_path=parts.path.rstrip("/"),
            query=parts.query,
            api_key=settings.llm_gateway_upstream_api_key,
            auth_header=settings.llm_gateway_upstream_auth_header,
            auth_scheme=settings.llm_gateway_upstream_auth_scheme,
            timeout_seconds=float(
                settings.llm_gateway_upstream_timeout_seconds
            ),
        )

    def path_for(self, request_path: str, request_query: str = "") -> str:
        path = request_path or "/"
        if not path.startswith("/"):
            path = f"/{path}"
        if self.base_path:
            if path == "/v1":
                path = "/"
            elif path.startswith("/v1/"):
                path = path[3:]
            path = f"{self.base_path}{path}"
        query = "&".join(
            part for part in (self.query, request_query) if part
        )
        return f"{path}?{query}" if query else path

    def headers_for(self, request_headers: Any) -> dict[str, str]:
        headers: dict[str, str] = {}
        for name, value in request_headers.items():
            lower = name.lower()
            if (
                lower in _HOP_BY_HOP_HEADERS
                or lower in _REQUEST_HEADERS_NOT_FORWARDED
            ):
                continue
            headers[name] = value
        headers["Host"] = self.host_header
        if self.api_key:
            value = self.api_key
            if self.auth_scheme:
                value = f"{self.auth_scheme} {value}"
            headers[self.auth_header] = value
        return headers

    def connection(self) -> HTTPConnection:
        if self.scheme == "https":
            return HTTPSConnection(
                self.host, self.port, timeout=self.timeout_seconds
            )
        return HTTPConnection(
            self.host, self.port, timeout=self.timeout_seconds
        )


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def issue_token(
    secret: str,
    job_id: str,
    ttl_seconds: int,
    now: float | None = None,
) -> str:
    """Mint a short-lived job token for the gateway.

    The token is a signed bearer credential. It is not an upstream provider
    key, and the job cannot mint new tokens without the gateway secret.
    """
    if not secret:
        raise ValueError("LLM_GATEWAY_SECRET is not set.")
    if ttl_seconds <= 0:
        raise ValueError("The gateway token TTL must be positive.")
    expires_at = int(time.time() if now is None else now) + int(ttl_seconds)
    payload = json.dumps(
        {"exp": expires_at, "job_id": job_id},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    encoded = _b64url_encode(payload)
    signature = hmac.new(
        secret.encode("utf-8"),
        encoded.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{_TOKEN_PREFIX}.{encoded}.{_b64url_encode(signature)}"


def verify_token(
    secret: str,
    token: str,
    now: float | None = None,
) -> GatewayToken | None:
    if not secret or not token:
        return None
    try:
        prefix, encoded, signature = token.split(".")
        if prefix != _TOKEN_PREFIX:
            return None
        expected = hmac.new(
            secret.encode("utf-8"),
            encoded.encode("ascii"),
            hashlib.sha256,
        ).digest()
        if not hmac.compare_digest(expected, _b64url_decode(signature)):
            return None
        payload = json.loads(_b64url_decode(encoded))
        if not isinstance(payload, dict):
            return None
        job_id = payload.get("job_id")
        expires_at = payload.get("exp")
        if not isinstance(job_id, str) or not job_id:
            return None
        if not isinstance(expires_at, int) or isinstance(expires_at, bool):
            return None
        if expires_at < int(time.time() if now is None else now):
            return None
        return GatewayToken(job_id=job_id, expires_at=expires_at)
    except (
        ValueError,
        TypeError,
        AttributeError,
        KeyError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return None


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        address: tuple[str, int],
        settings: Settings,
        upstream: Upstream,
    ) -> None:
        super().__init__(address, GatewayHandler)
        self.settings = settings
        self.upstream = upstream


class GatewayHandler(BaseHTTPRequestHandler):
    server: GatewayServer
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("gateway: %s", format % args)

    def do_GET(self) -> None:  # noqa: N802
        self._proxy()

    def do_HEAD(self) -> None:  # noqa: N802
        self._proxy()

    def do_POST(self) -> None:  # noqa: N802
        self._proxy()

    def do_PUT(self) -> None:  # noqa: N802
        self._proxy()

    def do_PATCH(self) -> None:  # noqa: N802
        self._proxy()

    def do_DELETE(self) -> None:  # noqa: N802
        self._proxy()

    def _proxy(self) -> None:
        started = time.perf_counter()
        path = urlsplit(self.path).path
        if path in {"/health", "/healthz"}:
            self._send_json({"status": "ok"})
            return

        claims = verify_token(
            self.server.settings.llm_gateway_secret,
            self._bearer_token(),
        )
        if claims is None:
            self._send_gateway_error(
                HTTPStatus.UNAUTHORIZED,
                "Missing or invalid gateway token.",
                "authentication_error",
            )
            logger.warning(
                "llm_request job=unknown method=%s path=%s status=401",
                self.command,
                path,
            )
            return

        try:
            body = self._read_body()
        except _RequestTooLarge:
            self._send_gateway_error(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                "The request body is too large.",
                "invalid_request_error",
            )
            return
        except ValueError as exc:
            self._send_gateway_error(
                HTTPStatus.BAD_REQUEST,
                str(exc),
                "invalid_request_error",
            )
            return

        upstream = self.server.upstream
        connection = upstream.connection()
        try:
            connection.request(
                self.command,
                upstream.path_for(path, urlsplit(self.path).query),
                body=(
                    None
                    if self.command in {"GET", "HEAD"}
                    else body
                ),
                headers=upstream.headers_for(self.headers),
            )
            response = connection.getresponse()
        except TimeoutError:
            connection.close()
            self._send_gateway_error(
                HTTPStatus.GATEWAY_TIMEOUT,
                "The upstream model provider timed out.",
                "upstream_timeout",
            )
            logger.warning(
                "llm_request job=%s method=%s path=%s status=504",
                claims.job_id,
                self.command,
                path,
            )
            return
        except (OSError, HTTPException) as exc:
            connection.close()
            self._send_gateway_error(
                HTTPStatus.BAD_GATEWAY,
                f"The upstream model provider request failed: {exc}",
                "upstream_error",
            )
            logger.warning(
                "llm_request job=%s method=%s path=%s status=502 error=%s",
                claims.job_id,
                self.command,
                path,
                exc,
            )
            return

        try:
            self._relay(response)
        except (BrokenPipeError, ConnectionResetError):
            logger.debug(
                "Client disconnected while reading the upstream response "
                "for job %s",
                claims.job_id,
            )
        finally:
            connection.close()
        logger.info(
            "llm_request job=%s method=%s path=%s status=%s duration_ms=%.1f",
            claims.job_id,
            self.command,
            path,
            response.status,
            (time.perf_counter() - started) * 1000,
        )

    def _bearer_token(self) -> str:
        value = self.headers.get("Authorization", "")
        scheme, _, token = value.partition(" ")
        if scheme.lower() != "bearer":
            return ""
        return token.strip()

    def _read_body(self) -> bytes:
        max_bytes = self.server.settings.llm_gateway_max_request_bytes
        length_header = self.headers.get("Content-Length")
        if length_header:
            try:
                length = int(length_header)
            except ValueError as exc:
                raise ValueError("Invalid Content-Length header.") from exc
            if length < 0:
                raise ValueError("Invalid Content-Length header.")
            if length > max_bytes:
                raise _RequestTooLarge
            if length == 0:
                return b""
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError("Unexpected end of request body.")
            return body
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            return self._read_chunked_body(max_bytes)
        return b""

    def _read_chunked_body(self, max_bytes: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            line = self.rfile.readline(1024)
            if not line:
                raise ValueError("Unexpected end of chunked request body.")
            size_text = line.split(b";", 1)[0].strip()
            try:
                size = int(size_text, 16)
            except ValueError as exc:
                raise ValueError("Invalid chunk size.") from exc
            if size == 0:
                while True:
                    trailer = self.rfile.readline(1024)
                    if trailer in (b"", b"\n", b"\r\n"):
                        break
                break
            total += size
            if total > max_bytes:
                raise _RequestTooLarge
            chunk = self.rfile.read(size)
            if len(chunk) != size:
                raise ValueError("Unexpected end of chunked request body.")
            chunks.append(chunk)
            self.rfile.read(2)
        return b"".join(chunks)

    def _relay(self, response: Any) -> None:
        self.send_response(response.status, response.reason)
        for name, value in response.getheaders():
            lower = name.lower()
            if (
                lower in _HOP_BY_HOP_HEADERS
                or lower in {"connection", "content-length"}
            ):
                continue
            self.send_header(name, value)
        length = response.getheader("Content-Length")
        if length is not None:
            self.send_header("Content-Length", length)
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        if self.command == "HEAD":
            return
        while True:
            chunk = response.read1(_CHUNK_SIZE)
            if not chunk:
                return
            self.wfile.write(chunk)
            self.wfile.flush()

    def _send_gateway_error(
        self,
        status: HTTPStatus,
        message: str,
        error_type: str,
    ) -> None:
        body = json.dumps(
            {"error": {"message": message, "type": error_type}}
        ).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(body)


def run_gateway(settings: Settings) -> None:
    secret = settings.llm_gateway_secret
    if not secret:
        raise GatewayConfigError(
            "LLM_GATEWAY_SECRET is not set. The worker and the gateway must "
            "share the same secret."
        )
    if len(secret) < 16:
        raise GatewayConfigError(
            "LLM_GATEWAY_SECRET must be at least 16 characters long."
        )
    upstream = Upstream.from_settings(settings)
    if not upstream.api_key:
        logger.warning(
            "LLM_GATEWAY_UPSTREAM_API_KEY is empty; the gateway will send "
            "unauthenticated requests to %s",
            settings.llm_gateway_upstream_url,
        )
    server = GatewayServer(
        (settings.llm_gateway_host, settings.llm_gateway_port),
        settings,
        upstream,
    )

    def shutdown(signum: int, frame: Any) -> None:
        del frame
        logger.info("Received signal %s; shutting down LLM gateway", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logger.info(
        "Kafkallm LLM gateway listening on http://%s:%s (upstream=%s)",
        settings.llm_gateway_host,
        settings.llm_gateway_port,
        settings.llm_gateway_upstream_url,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
