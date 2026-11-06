from __future__ import annotations

import json
import logging
import mimetypes
import signal
import threading
import uuid
from collections.abc import Callable
from email.parser import BytesParser
from email.policy import default as email_policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit

from confluent_kafka import Consumer, KafkaError, KafkaException
from pydantic import ValidationError

from kafkallm.agents import (
    AgentPublisher,
    AgentStore,
    new_agent_id,
    seed_default_agents,
)
from kafkallm.kafka import KafkaProducer, ensure_topics, wait_for_kafka
from kafkallm.models import AgentSpec, FileRef, Job, SandboxSpec, utc_now
from kafkallm.sandboxes import (
    SandboxPublisher,
    SandboxStore,
    new_sandbox_id,
    seed_default_sandboxes,
)
from kafkallm.settings import Settings
from kafkallm.state import StatePublisher
from kafkallm.storage import S3Storage
from kafkallm.telemetry import (
    current_trace_id,
    inject_trace_headers,
    new_trace_id,
)

logger = logging.getLogger(__name__)

_MAX_EVENTS_PER_JOB = 200
_MULTIPART_OVERHEAD_BYTES = 1024 * 1024


def _multipart_files(content_type: str, body: bytes) -> list[tuple[str, bytes]]:
    """Extract uploaded file parts without a third-party multipart parser."""
    if "\r" in content_type or "\n" in content_type:
        raise ValueError("Invalid Content-Type header.")
    if not content_type.lower().startswith("multipart/form-data"):
        raise ValueError("Expected multipart/form-data.")
    header = (f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n").encode()
    message = BytesParser(policy=email_policy).parsebytes(header + body)
    if not message.is_multipart():
        raise ValueError("The request body is not multipart form data.")
    files: list[tuple[str, bytes]] = []
    for part in message.iter_parts():
        filename = part.get_filename()
        if not filename:
            continue
        data = part.get_payload(decode=True)
        files.append((filename, data if isinstance(data, bytes) else b""))
    return files


class JobStore:
    """In-memory projection of the compacted state topic and the event stream.

    Kafka is the database. This projection is rebuilt from the beginning of the
    topics whenever the GUI restarts, so there is no local state to back up.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._states: dict[str, dict[str, Any]] = {}
        self._events: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        for target, name in (
            (self._consume_states, "kafkallm-gui-state"),
            (self._consume_events, "kafkallm-gui-events"),
        ):
            thread = threading.Thread(target=target, name=name, daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)

    def jobs(self) -> list[dict[str, Any]]:
        with self._lock:
            states = list(self._states.values())
        states.sort(key=lambda state: state.get("submitted_at") or "", reverse=True)
        return states

    def job(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            state = self._states.get(job_id)
            events = list(self._events.get(job_id, []))
        if state is None:
            return None
        return {**state, "events": events}

    def put_state(self, state: dict[str, Any]) -> None:
        job_id = state.get("job_id")
        if not isinstance(job_id, str):
            return
        with self._lock:
            self._states[job_id] = state

    def put_event(self, event: dict[str, Any]) -> None:
        job_id = event.get("job_id")
        if not isinstance(job_id, str):
            return
        with self._lock:
            events = self._events.setdefault(job_id, [])
            events.append(event)
            if len(events) > _MAX_EVENTS_PER_JOB:
                del events[: len(events) - _MAX_EVENTS_PER_JOB]

    def _consume_states(self) -> None:
        self._consume_topic(self.settings.state_topic, self.put_state)

    def _consume_events(self) -> None:
        self._consume_topic(self.settings.events_topic, self.put_event)

    def _consume_topic(self, topic: str, handler: Any) -> None:
        while not self._stop.is_set():
            consumer = Consumer(
                {
                    "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                    "group.id": f"kafkallm-gui-{topic}-{uuid.uuid4()}",
                    "auto.offset.reset": "earliest",
                    "enable.auto.commit": False,
                    "session.timeout.ms": 15000,
                }
            )
            consumer.subscribe([topic])
            try:
                while not self._stop.is_set():
                    message = consumer.poll(timeout=1.0)
                    if message is None:
                        continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(message.error())
                    value = message.value()
                    if value is None:
                        continue
                    try:
                        payload = json.loads(value.decode("utf-8"))
                    except (UnicodeDecodeError, ValueError):
                        logger.warning("Skipping malformed message in %s", topic)
                        continue
                    if isinstance(payload, dict):
                        handler(payload)
            except Exception:
                logger.exception("GUI consumer for %s failed; reconnecting", topic)
                self._stop.wait(2)
            finally:
                consumer.close()


class JobServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        address: tuple[str, int],
        store: JobStore,
        producer: KafkaProducer,
        settings: Settings,
        storage_factory: Callable[[Settings], S3Storage] | None = None,
        agents: AgentStore | None = None,
        sandboxes: SandboxStore | None = None,
    ) -> None:
        super().__init__(address, JobHandler)
        self.store = store
        self.producer = producer
        self.settings = settings
        self.producer_lock = threading.Lock()
        self.storage_factory = storage_factory or S3Storage
        self.storage_lock = threading.Lock()
        self._storage: S3Storage | None = None
        self.agents = agents or AgentStore(settings)
        self.agent_publisher = AgentPublisher(settings, producer)
        self.sandboxes = sandboxes or SandboxStore(settings)
        self.sandbox_publisher = SandboxPublisher(settings, producer)

    def get_storage(self) -> S3Storage:
        if self._storage is None:
            with self.storage_lock:
                if self._storage is None:
                    self._storage = self.storage_factory(self.settings)
        return self._storage


class JobHandler(BaseHTTPRequestHandler):
    server: JobServer

    def log_message(self, format: str, *args: Any) -> None:
        logger.info("%s - %s", self.address_string(), format % args)

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in {"/", "/index.html"}:
            self._send_file()
        elif path == "/healthz":
            self._send_json({"status": "ok"})
        elif path == "/api/jobs":
            self._send_json({"jobs": self.server.store.jobs()})
        elif path == "/api/agents":
            self._send_json(
                {
                    "agents": [
                        agent.model_dump(mode="json")
                        for agent in self.server.agents.agents()
                    ]
                }
            )
        elif path == "/api/sandboxes":
            self._send_json(
                {
                    "sandboxes": [
                        sandbox.model_dump(mode="json")
                        for sandbox in self.server.sandboxes.sandboxes()
                    ]
                }
            )
        elif path == "/api/schema/job":
            self._send_json(Job.model_json_schema())
        elif path == "/api/schema/agent":
            self._send_json(AgentSpec.model_json_schema())
        elif path == "/api/schema/sandbox":
            self._send_json(SandboxSpec.model_json_schema())
        elif path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                self._send_job(parts[2])
            elif len(parts) == 4 and parts[3] == "result":
                self._send_job_result(parts[2])
            elif len(parts) == 5 and parts[3] == "artifacts":
                self._send_job_artifact(parts[2], parts[4])
            else:
                self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
        else:
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _send_job(self, job_id: str) -> None:
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
        else:
            self._send_json(job)

    def _send_job_result(self, job_id: str) -> None:
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        result = job.get("result")
        if not isinstance(result, str) or not result:
            self._send_json(
                {"error": "result not available"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        self._send_download(
            result.encode("utf-8"),
            f"{job_id}-result.md",
            "text/markdown; charset=utf-8",
        )

    def _send_job_artifact(self, job_id: str, index_raw: str) -> None:
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        artifacts = job.get("artifacts") or []
        try:
            index = int(index_raw)
        except (TypeError, ValueError):
            index = -1
        if index < 0 or index >= len(artifacts):
            self._send_json(
                {"error": "artifact not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        try:
            reference = FileRef.model_validate(artifacts[index])
            body, length = self.server.get_storage().open_download(reference)
        except Exception as exc:
            logger.exception("Could not download artifact")
            self._send_json(
                {"error": f"Could not download artifact: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        filename = Path(reference.key).name or "artifact"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        try:
            self._send_download_stream(body, length, filename, content_type)
        finally:
            body.close()

    def do_POST(self) -> None:
        if self.path == "/api/uploads":
            self._handle_upload()
            return
        if self.path == "/api/agents":
            self._handle_agent_save()
            return
        if self.path == "/api/sandboxes":
            self._handle_sandbox_save()
            return
        if self.path != "/api/jobs":
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("The request body must be a JSON object.")
            sandbox_id = str(payload.get("sandbox", "default"))
            sandbox = self.server.sandboxes.get(sandbox_id)
            if sandbox is None and sandbox_id not in {"default", "reverse"}:
                sandbox = self.server.sandboxes.wait_for(sandbox_id, timeout=1.0)
            if sandbox is None:
                if sandbox_id not in {"default", "reverse"}:
                    raise ValueError(f"Unknown sandbox: {sandbox_id}")
            elif not sandbox.enabled:
                raise ValueError(f"Sandbox {sandbox_id!r} is disabled.")
            agent_ids = payload.pop("agent_ids", None)
            multi_agent = bool(payload.get("multi_agent", True))
            if multi_agent and agent_ids is not None:
                if not isinstance(agent_ids, list):
                    raise ValueError("agent_ids must be a list.")
                selected: list[AgentSpec] = []
                for raw_id in agent_ids:
                    agent = self.server.agents.get(str(raw_id))
                    if agent is None:
                        raise ValueError(f"Unknown agent: {raw_id}")
                    if agent.enabled:
                        selected.append(agent)
                payload["agents"] = [
                    agent.model_dump(mode="json") for agent in selected
                ]
            elif not multi_agent:
                payload["agents"] = []
            payload["multi_agent"] = multi_agent
            job = Job.model_validate(
                {
                    **payload,
                    "trace_id": payload.get("trace_id")
                    or current_trace_id()
                    or new_trace_id(),
                }
            )
        except (ValueError, ValidationError) as exc:
            self._send_json(
                {"error": f"Invalid job: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        try:
            with self.server.producer_lock:
                self.server.producer.send(
                    self.server.settings.jobs_topic,
                    job.job_id,
                    job,
                    headers=inject_trace_headers(),
                )
                StatePublisher(self.server.settings, self.server.producer).queued(
                    job, worker_id=None
                )
        except Exception as exc:
            logger.exception("Could not submit job")
            self._send_json(
                {"error": f"Could not submit job: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._send_json(job.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def do_DELETE(self) -> None:
        if self.path.startswith("/api/agents/"):
            self._handle_agent_delete()
            return
        if self.path.startswith("/api/sandboxes/"):
            self._handle_sandbox_delete()
            return
        self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_agent_delete(self) -> None:
        agent_id = self.path.rsplit("/", 1)[-1]
        agent = self.server.agents.get(agent_id)
        if agent is None:
            self._send_json({"error": "agent not found"}, status=HTTPStatus.NOT_FOUND)
            return
        updated = agent.model_copy(update={"enabled": False, "updated_at": utc_now()})
        try:
            with self.server.producer_lock:
                self.server.agent_publisher.save(updated)
        except Exception as exc:
            logger.exception("Could not disable agent")
            self._send_json(
                {"error": f"Could not disable agent: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.agents.put(updated)
        self._send_json(updated.model_dump(mode="json"))

    def _handle_sandbox_delete(self) -> None:
        sandbox_id = self.path.rsplit("/", 1)[-1]
        if sandbox_id == "default":
            self._send_json(
                {"error": "The default sandbox cannot be disabled."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        sandbox = self.server.sandboxes.get(sandbox_id)
        if sandbox is None:
            self._send_json(
                {"error": "sandbox not found"},
                status=HTTPStatus.NOT_FOUND,
            )
            return
        updated = sandbox.model_copy(update={"enabled": False, "updated_at": utc_now()})
        try:
            with self.server.producer_lock:
                self.server.sandbox_publisher.save(updated)
        except Exception as exc:
            logger.exception("Could not disable sandbox")
            self._send_json(
                {"error": f"Could not disable sandbox: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.sandboxes.put(updated)
        self._send_json(updated.model_dump(mode="json"))

    def _handle_agent_save(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("The agent must be a JSON object.")
            name = str(payload.get("name", "")).strip()
            if not name:
                raise ValueError("The agent name is required.")
            agent = AgentSpec.model_validate(
                {
                    **payload,
                    "agent_id": payload.get("agent_id") or new_agent_id(name),
                    "name": name,
                    "updated_at": utc_now().isoformat(),
                }
            )
        except (ValueError, ValidationError) as exc:
            self._send_json(
                {"error": f"Invalid agent: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            with self.server.producer_lock:
                self.server.agent_publisher.save(agent)
        except Exception as exc:
            logger.exception("Could not save agent")
            self._send_json(
                {"error": f"Could not save agent: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.agents.put(agent)
        self._send_json(agent.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def _handle_sandbox_save(self) -> None:
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(payload, dict):
                raise ValueError("The sandbox must be a JSON object.")
            name = str(payload.get("name", "")).strip()
            if not name:
                raise ValueError("The sandbox name is required.")
            image = str(payload.get("image", "")).strip()
            if not image:
                raise ValueError("The container image is required.")
            env = payload.get("env") or {}
            if not isinstance(env, dict):
                raise ValueError("env must be a JSON object.")
            extensions = payload.get("extensions") or []
            if not isinstance(extensions, list):
                raise ValueError("extensions must be a list.")
            sandbox = SandboxSpec.model_validate(
                {
                    **payload,
                    "sandbox_id": payload.get("sandbox_id") or new_sandbox_id(name),
                    "name": name,
                    "image": image,
                    "env": {str(key): str(value) for key, value in env.items()},
                    "extensions": [
                        str(item).strip() for item in extensions if str(item).strip()
                    ],
                    "updated_at": utc_now().isoformat(),
                }
            )
        except (ValueError, ValidationError) as exc:
            self._send_json(
                {"error": f"Invalid sandbox: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            with self.server.producer_lock:
                self.server.sandbox_publisher.save(sandbox)
        except Exception as exc:
            logger.exception("Could not save sandbox")
            self._send_json(
                {"error": f"Could not save sandbox: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.sandboxes.put(sandbox)
        self._send_json(sandbox.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def _handle_upload(self) -> None:
        settings = self.server.settings
        if not settings.storage_configured:
            self._send_json(
                {
                    "error": "File uploads require S3/RustFS. Set "
                    "S3_ENDPOINT_URL and the S3 credentials."
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            self._send_json(
                {"error": "A non-empty multipart body is required."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if length > settings.upload_max_total_bytes + _MULTIPART_OVERHEAD_BYTES:
            self._send_json(
                {"error": "The upload exceeds the configured size limit."},
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return
        body = self.rfile.read(length)
        try:
            files = _multipart_files(self.headers.get("Content-Type", ""), body)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if not files:
            self._send_json(
                {"error": "No files were found in the upload."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if len(files) > settings.upload_max_files:
            self._send_json(
                {
                    "error": f"At most {settings.upload_max_files} files "
                    "can be uploaded at once."
                },
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return
        total = 0
        for _, data in files:
            if len(data) > settings.upload_max_file_bytes:
                self._send_json(
                    {"error": "A file exceeds the configured per-file size limit."},
                    status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                )
                return
            total += len(data)
            if total > settings.upload_max_total_bytes:
                self._send_json(
                    {"error": "The upload exceeds the configured size limit."},
                    status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                )
                return
        try:
            storage = self.server.get_storage()
            references = [
                storage.upload_input(filename, data) for filename, data in files
            ]
        except Exception as exc:
            logger.exception("Could not store uploaded input files")
            self._send_json(
                {"error": f"Could not store uploaded files: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._send_json(
            {
                "files": [
                    {
                        "name": filename,
                        "size": len(data),
                        "bucket": reference.bucket,
                        "key": reference.key,
                    }
                    for (filename, data), reference in zip(
                        files, references, strict=True
                    )
                ],
                "input_files": [
                    reference.model_dump(mode="json") for reference in references
                ],
            },
            status=HTTPStatus.CREATED,
        )

    def _send_file(self) -> None:
        path = Path(__file__).parent / "static" / "index.html"
        try:
            body = path.read_bytes()
        except OSError:
            self._send_json(
                {"error": "GUI assets are missing"},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )
            return
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_download(
        self,
        body: bytes,
        filename: str,
        content_type: str,
    ) -> None:
        self._send_download_headers(filename, content_type, len(body))
        self.wfile.write(body)

    def _send_download_stream(
        self,
        body: Any,
        length: int,
        filename: str,
        content_type: str,
    ) -> None:
        self._send_download_headers(filename, content_type, length)
        while True:
            chunk = body.read(64 * 1024)
            if not chunk:
                break
            self.wfile.write(chunk)

    def _send_download_headers(
        self,
        filename: str,
        content_type: str,
        length: int,
    ) -> None:
        quoted = quote(filename, safe="")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header(
            "Content-Disposition",
            f"attachment; filename=\"{quoted}\"; filename*=UTF-8''{quoted}",
        )
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()

    def _send_json(
        self,
        payload: Any,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def run_gui(settings: Settings) -> None:
    wait_for_kafka(settings)
    ensure_topics(settings)
    store = JobStore(settings)
    store.start()
    producer = KafkaProducer(settings)
    agents = AgentStore(settings)
    seed_default_agents(settings, producer)
    agents.start()
    sandboxes = SandboxStore(settings)
    seed_default_sandboxes(settings, producer)
    sandboxes.start()
    server = JobServer(
        (settings.gui_host, settings.gui_port),
        store,
        producer,
        settings,
        agents=agents,
        sandboxes=sandboxes,
    )

    def shutdown(signum: int, frame: Any) -> None:
        del frame
        logger.info("Received signal %s; shutting down GUI", signum)
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    logger.info(
        "Kafkallm GUI listening on http://%s:%s",
        settings.gui_host,
        settings.gui_port,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()
        agents.stop()
        sandboxes.stop()
        store.stop()
        producer.close()
