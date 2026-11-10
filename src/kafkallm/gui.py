from __future__ import annotations

import json
import logging
import mimetypes
import signal
import threading
from collections.abc import Callable
from email.parser import BytesParser
from email.policy import default as email_policy
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlsplit

from pydantic import ValidationError

from kafkallm.agents import (
    AgentPublisher,
    AgentStore,
    default_agent_ids,
    new_agent_id,
    sync_agents,
    write_agent_profile,
)
from kafkallm.job_store import JobStore
from kafkallm.kafka import KafkaProducer, ensure_topics, wait_for_kafka
from kafkallm.models import (
    AgentSpec,
    FileRef,
    Job,
    JobCancel,
    JobState,
    SandboxSpec,
    utc_now,
)
from kafkallm.sandboxes import (
    SandboxPublisher,
    SandboxStore,
    builtin_sandbox,
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


class RequestTooLarge(ValueError):
    """The request body exceeds the configured JSON body limit."""


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

    def _read_json_request(self, subject: str) -> dict[str, Any] | None:
        """Read a size-bounded JSON object or send a 4xx response.

        Returns ``None`` after the response has been sent. Every write endpoint
        uses this helper so an oversized body is rejected before it is read.
        """
        limit = self.server.settings.gui_max_request_bytes
        try:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length)
            except ValueError as exc:
                raise ValueError("Invalid Content-Length header.") from exc
            if length < 0:
                raise ValueError("Invalid Content-Length header.")
            if length > limit:
                raise RequestTooLarge(
                    f"The request body exceeds the {limit} byte limit."
                )
            body = self.rfile.read(length) if length else b"{}"
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError("The request body must be a JSON object.")
        except RequestTooLarge as exc:
            self._send_json(
                {"error": str(exc)},
                status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
            )
            return None
        except (UnicodeDecodeError, ValueError) as exc:
            self._send_json(
                {"error": f"Invalid {subject}: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return None
        return payload

    def do_GET(self) -> None:
        path = urlsplit(self.path).path
        if path in {"/", "/index.html"}:
            self._send_file()
        elif path == "/healthz":
            self._send_json({"status": "ok"})
        elif path == "/api/jobs":
            self._send_json({"jobs": self.server.store.jobs()})
        elif path == "/api/agents":
            builtin = default_agent_ids()
            self._send_json(
                {
                    "agents": [
                        {
                            **agent.model_dump(mode="json"),
                            "builtin": agent.agent_id in builtin,
                        }
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
        elif path == "/api/features":
            self._send_json(
                {
                    "unlimited_jobs": self.server.settings.allow_unlimited_jobs,
                    "reflect_default": (self.server.settings.goose_job_reflect_enabled),
                }
            )
        elif path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 3:
                self._send_job(parts[2])
            elif len(parts) == 4 and parts[3] == "result":
                self._send_job_result(parts[2])
            elif len(parts) == 4 and parts[3] == "logs":
                self._send_job_logs(parts[2])
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

    def _send_job_logs(self, job_id: str) -> None:
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        query = parse_qs(urlsplit(self.path).query)
        try:
            after = int(query.get("after", ["0"])[0])
        except (TypeError, ValueError):
            after = 0
        records = self.server.store.logs(job_id, after)
        next_sequence = (
            records[-1]["sequence"]
            if records and isinstance(records[-1].get("sequence"), int)
            else after
        )
        self._send_json({"logs": records, "next": next_sequence})

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

    def _handle_job_cancel(self) -> None:
        parts = self.path.strip("/").split("/")
        if len(parts) != 4 or parts[0] != "api" or parts[1] != "jobs":
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        job_id = parts[2]
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if job.get("cancelled"):
            self._send_json({"status": "cancelled", "job_id": job_id})
            return
        if job.get("status") not in {"queued", "running"}:
            self._send_json(
                {"error": "Only queued or running jobs can be stopped."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        state = JobState.model_validate(
            {key: value for key, value in job.items() if key != "events"}
        )
        updated = state.model_copy(
            update={
                "status": "failed",
                "cancelled": True,
                "error": "Cancellation requested by user.",
                "finished_at": utc_now(),
            }
        )
        try:
            with self.server.producer_lock:
                self.server.producer.send(
                    self.server.settings.cancels_topic,
                    job_id,
                    JobCancel(job_id=job_id),
                )
                self.server.producer.send(
                    self.server.settings.state_topic,
                    job_id,
                    updated,
                )
        except Exception as exc:
            logger.exception("Could not request job cancellation")
            self._send_json(
                {"error": f"Could not stop job: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.store.put_state(updated.model_dump(mode="json"))
        self._send_json(
            {"status": "cancelling", "job_id": job_id},
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_clear_finished(self) -> None:
        """Delete the state records of all finished jobs from the compacted topic."""
        finished = [
            job
            for job in self.server.store.jobs()
            if job.get("status") not in {"queued", "running"}
        ]
        if not finished:
            self._send_json({"removed": 0})
            return
        try:
            with self.server.producer_lock:
                self.server.producer.send_tombstones(
                    self.server.settings.state_topic,
                    [str(job["job_id"]) for job in finished],
                )
        except Exception as exc:
            logger.exception("Could not clear finished jobs")
            self._send_json(
                {"error": f"Could not clear finished jobs: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        for job in finished:
            self.server.store.remove_job(str(job["job_id"]))
        self._send_json({"removed": len(finished)})

    def _handle_job_restart(self) -> None:
        """Queue a new job with the stored instruction, inputs, and settings."""
        parts = urlsplit(self.path).path.strip("/").split("/")
        if len(parts) != 4 or parts[0] != "api" or parts[1] != "jobs":
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        job_id = parts[2]
        job = self.server.store.job(job_id)
        if job is None:
            self._send_json({"error": "job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        if job.get("status") in {"queued", "running"}:
            self._send_json(
                {"error": "Only finished jobs can be restarted."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            state = JobState.model_validate(
                {key: value for key, value in job.items() if key != "events"}
            )
        except ValidationError as exc:
            self._send_json(
                {"error": f"Invalid job state: {exc}"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if not state.instruction.strip():
            self._send_json(
                {"error": "This job has no instruction to restart."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        if state.unlimited and not self.server.settings.allow_unlimited_jobs:
            self._send_json(
                {
                    "error": "Unlimited jobs are disabled. Set "
                    "ALLOW_UNLIMITED_JOBS=true."
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        restarted = Job(
            instruction=state.instruction,
            input=state.input,
            input_files=state.input_files,
            sandbox=state.sandbox,
            sandbox_image=state.sandbox_image,
            multi_agent=state.multi_agent,
            agents=state.agents,
            unlimited=state.unlimited,
            timeout_seconds=state.timeout_seconds,
            max_steps=state.max_steps,
            reflect=state.reflect,
            trace_id=current_trace_id() or new_trace_id(),
        )
        try:
            with self.server.producer_lock:
                self.server.producer.send(
                    self.server.settings.jobs_topic,
                    restarted.job_id,
                    restarted,
                    headers=inject_trace_headers(),
                )
                StatePublisher(self.server.settings, self.server.producer).queued(
                    restarted, worker_id=None
                )
        except Exception as exc:
            logger.exception("Could not restart job %s", job_id)
            self._send_json(
                {"error": f"Could not restart job: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.store.put_state(JobState.queued(restarted).model_dump(mode="json"))
        self._send_json(restarted.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def do_POST(self) -> None:
        if self.path == "/api/uploads":
            self._handle_upload()
            return
        if self.path == "/api/agents/reload":
            self._handle_agents_reload()
            return
        if self.path == "/api/agents":
            self._handle_agent_save()
            return
        if self.path == "/api/sandboxes":
            self._handle_sandbox_save()
            return
        if self.path == "/api/jobs/clear-finished":
            self._handle_clear_finished()
            return
        if self.path.startswith("/api/jobs/") and self.path.endswith("/cancel"):
            self._handle_job_cancel()
            return
        if self.path.startswith("/api/jobs/") and self.path.endswith("/restart"):
            self._handle_job_restart()
            return
        if self.path != "/api/jobs":
            self._send_json({"error": "not found"}, status=HTTPStatus.NOT_FOUND)
            return
        try:
            payload = self._read_json_request("job")
            if payload is None:
                return
            sandbox_id = str(payload.get("sandbox", "default"))
            sandbox = self.server.sandboxes.get(sandbox_id)
            if sandbox is None:
                sandbox = self.server.sandboxes.wait_for(sandbox_id, timeout=1.0)
            if sandbox is None:
                sandbox = builtin_sandbox(sandbox_id, self.server.settings)
            if sandbox is None:
                raise ValueError(f"Unknown sandbox: {sandbox_id}")
            if not sandbox.enabled:
                raise ValueError(f"Sandbox {sandbox_id!r} is disabled.")
            # Carry the image reference in the job so the worker runs exactly
            # what the operator selected and can pull it from its registry.
            payload["sandbox_image"] = sandbox.image
            if (
                payload.get("unlimited")
                and not self.server.settings.allow_unlimited_jobs
            ):
                raise ValueError(
                    "Unlimited jobs are disabled. Set ALLOW_UNLIMITED_JOBS=true."
                )
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
        self.server.store.put_state(JobState.queued(job).model_dump(mode="json"))
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
        if agent_id in default_agent_ids():
            self._send_json(
                {"error": "Built-in agents cannot be disabled."},
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        agent = self.server.agents.get(agent_id)
        if agent is None:
            self._send_json({"error": "agent not found"}, status=HTTPStatus.NOT_FOUND)
            return
        updated = agent.model_copy(update={"enabled": False, "updated_at": utc_now()})
        try:
            write_agent_profile(self.server.settings, updated)
        except OSError as exc:
            logger.exception("Could not write agent profile")
            self._send_json(
                {"error": f"Could not write agent profile: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
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

    def _handle_agents_reload(self) -> None:
        try:
            with self.server.producer_lock:
                sync_agents(self.server.settings, self.server.producer)
        except Exception as exc:
            logger.exception("Could not reload agent profiles")
            self._send_json(
                {"error": f"Could not reload agents: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self._send_json({"status": "ok"})

    def _handle_agent_save(self) -> None:
        try:
            payload = self._read_json_request("agent")
            if payload is None:
                return
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
        if agent.agent_id in default_agent_ids():
            self._send_json(
                {
                    "error": "Built-in agents are read-only. Choose a different "
                    "name for a new agent."
                },
                status=HTTPStatus.BAD_REQUEST,
            )
            return
        try:
            write_agent_profile(self.server.settings, agent)
        except OSError as exc:
            logger.exception("Could not write agent profile")
            self._send_json(
                {"error": f"Could not write agent profile: {exc}"},
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        try:
            with self.server.producer_lock:
                self.server.agent_publisher.save(agent)
        except Exception as exc:
            logger.exception("Could not save agent")
            self._send_json(
                {
                    "error": f"Could not save agent: {exc}. The profile file was "
                    "written and will be imported on the next GUI start."
                },
                status=HTTPStatus.SERVICE_UNAVAILABLE,
            )
            return
        self.server.agents.put(agent)
        self._send_json(agent.model_dump(mode="json"), status=HTTPStatus.CREATED)

    def _handle_sandbox_save(self) -> None:
        try:
            payload = self._read_json_request("sandbox")
            if payload is None:
                return
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
    sync_agents(settings, producer)
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
        "Gänsestall GUI listening on http://%s:%s",
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
