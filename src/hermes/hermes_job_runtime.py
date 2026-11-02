from __future__ import annotations

import io
import json
import logging
import os
import tarfile
import threading
import time
from pathlib import Path
from typing import Any

from hermes.agent import AgentResult
from hermes.audit import JobAudit
from hermes.models import Job
from hermes.settings import Settings
from hermes.telemetry import get_tracer, set_content, set_error


logger = logging.getLogger(__name__)

_JOB_LABEL = "hermes.job"
_JOB_ID_LABEL = "hermes.job_id"
_RUN_ID_LABEL = "hermes.run_id"
_CREATED_AT_LABEL = "hermes.created_at"
_VOLUME_LABEL = "hermes.workspace_volume"

_MAIN_JSONL = "/tmp/hermes-main.jsonl"
_MAIN_STDERR = "/tmp/hermes-main.err"
_REFLECT_JSONL = "/tmp/hermes-reflect.jsonl"
_REFLECT_STDERR = "/tmp/hermes-reflect.err"

_REFLECT_MIN_REMAINING_SECONDS = 60
_SCHEDULE_RESERVE_SECONDS = 25
_CONTAINER_POLL_SECONDS = 1.0
_MAX_RESULT_BYTES = 500_000

_PROVIDER_ENV_KEYS = (
    "OPENAI_API_KEY",
    "OPENROUTER_API_KEY",
    "ANTHROPIC_API_KEY",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "DEEPSEEK_API_KEY",
    "XAI_API_KEY",
    "MISTRAL_API_KEY",
    "GROQ_API_KEY",
    "TOGETHER_API_KEY",
    "FIREWORKS_API_KEY",
    "NOUS_API_KEY",
)

_RUN_SCRIPT = r"""#!/bin/sh
set -u

# Ephemeral jobs keep skills, but deliberately disable the small per-agent
# memory stores and the automatic post-turn review. Learning happens in the
# explicit reflect run below, before this container is removed.
hermes config set memory.memory_enabled false >/dev/null 2>&1 || true
hermes config set memory.user_profile_enabled false >/dev/null 2>&1 || true
hermes config set auxiliary.background_review.enabled false --force >/dev/null 2>&1 || true

# The skills volume is shared between jobs. Keep it owned by the runtime user
# so the next job and the gateway runtime can write to it.
trap 'chown -R 10001:10001 /opt/data/skills 2>/dev/null || true' EXIT

mkdir -p /workspace/outputs 2>/dev/null || true

START=$(date +%s)
BUDGET="${HERMES_JOB_BUDGET_SECONDS:-0}"
RESERVE=25

hermes config set agent.run_budget_seconds "${HERMES_JOB_MAIN_RUN_BUDGET:-300}" >/dev/null 2>&1 || true

set -- --oneshot --query-file /tmp/hermes-job-prompt.txt --format stream-json --source tool --yolo --in /workspace
if [ -n "${HERMES_JOB_MODEL:-}" ]; then
  set -- "$@" --model "$HERMES_JOB_MODEL"
  if [ -n "${HERMES_JOB_PROVIDER:-}" ]; then
    set -- "$@" --provider "$HERMES_JOB_PROVIDER"
  fi
fi
if [ -n "${HERMES_JOB_MAX_TURNS:-}" ]; then
  set -- "$@" --max-turns "$HERMES_JOB_MAX_TURNS"
fi

/usr/bin/timeout -s TERM -k 10 "${HERMES_JOB_MAIN_TIMEOUT:-330}" hermes chat "$@" \
  > /tmp/hermes-main.jsonl 2> /tmp/hermes-main.err
MAIN_RC=$?

if [ "$MAIN_RC" -ne 0 ]; then
  exit "$MAIN_RC"
fi

if [ "${HERMES_JOB_REFLECT_ENABLED:-0}" != "1" ]; then
  exit 0
fi

SESSION_ID=$(/opt/hermes/.venv/bin/python /tmp/hermes-session-id.py /tmp/hermes-main.jsonl 2>/dev/null || true)
if [ -z "$SESSION_ID" ]; then
  exit 0
fi

# The reflect pass inherits the time the main run did not use. The cap keeps a
# single reflect bounded even for long jobs.
NOW=$(date +%s)
ELAPSED=$(( NOW - START ))
LEFT=$(( BUDGET - ELAPSED - RESERVE ))
if [ "$LEFT" -lt 30 ]; then
  exit 0
fi
REFLECT_TIMEOUT=$LEFT
CAP="${HERMES_JOB_REFLECT_TIMEOUT:-300}"
if [ "$REFLECT_TIMEOUT" -gt "$CAP" ]; then
  REFLECT_TIMEOUT=$CAP
fi
REFLECT_BUDGET=$(( REFLECT_TIMEOUT - 15 ))
if [ "$REFLECT_BUDGET" -lt 10 ]; then
  REFLECT_BUDGET=10
fi

hermes config set agent.run_budget_seconds "$REFLECT_BUDGET" >/dev/null 2>&1 || true
set -- --oneshot --resume "$SESSION_ID" --query-file /tmp/hermes-reflect-prompt.txt --format stream-json --source tool --yolo --in /workspace
if [ -n "${HERMES_JOB_MODEL:-}" ]; then
  set -- "$@" --model "$HERMES_JOB_MODEL"
  if [ -n "${HERMES_JOB_PROVIDER:-}" ]; then
    set -- "$@" --provider "$HERMES_JOB_PROVIDER"
  fi
fi

# A resumed Hermes session keeps its original turn policy. Passing --max-turns
# here would fail whenever it differs from the main run, so the reflect pass is
# bounded by its run budget and the hard timeout instead.

/usr/bin/timeout -s TERM -k 10 "$REFLECT_TIMEOUT" hermes chat "$@" \
  > /tmp/hermes-reflect.jsonl 2> /tmp/hermes-reflect.err
exit 0
"""
_SESSION_ID_SCRIPT = r"""import json
import sys

session_id = ""
with open(sys.argv[1], encoding="utf-8", errors="replace") as stream:
    for line in stream:
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("type") == "result":
            session_id = str(event.get("session_id") or "")
print(session_id)
"""

_REFLECT_PROMPT = """Review the completed job session you just resumed. Improve Hermes' reusable skills for this class of job.

1. Call skills_list once. Inspect at most the two most relevant skills with skill_view.
2. Update the best match with skill_manage, or create one class-level skill if none fits. Never name a skill after this job, run, error, or one-off artifact.
3. Capture durable procedures, commands, tool quirks, verification steps, and templates. Do not store secrets, credentials, customer data, job-specific outputs, or transient facts.
4. Do not write to memory; it is disabled for this runtime.
5. If there is nothing durable to save, do nothing.

Keep it short: at most one skill change and a few tool calls. Return a one-line summary.
"""


class HermesJobRuntimeError(RuntimeError):
    pass


class HermesJobRunner:
    """Run each job in its own disposable Hermes container."""

    def __init__(
        self,
        settings: Settings,
        client: Any | None = None,
    ) -> None:
        self.settings = settings
        self._client = client
        self._owns_client = client is None
        self._active_container: Any | None = None
        self._active_lock = threading.Lock()

    @property
    def client(self) -> Any:
        if self._client is None:
            import docker

            self._client = docker.from_env()
        return self._client

    def wait_ready(self) -> None:
        deadline = time.monotonic() + self.settings.hermes_startup_timeout_seconds
        delay = 1.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.client.ping()
                self._ensure_image()
                self._cleanup_orphans()
                logger.info(
                    "Hermes job runtime is ready (image=%s, skills_volume=%s)",
                    self.settings.hermes_image,
                    self.settings.hermes_skills_volume,
                )
                return
            except Exception as exc:
                last_error = exc
                remaining = max(0.0, deadline - time.monotonic())
                logger.warning(
                    "Hermes job runtime not ready yet (%s); retrying in %.1fs",
                    exc,
                    min(delay, remaining),
                )
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 10.0)
        raise HermesJobRuntimeError(
            "Hermes job runtime did not become ready within the startup timeout."
        ) from last_error

    def close(self) -> None:
        self.cancel_active()
        if self._owns_client and self._client is not None:
            close = getattr(self._client, "close", None)
            if close is not None:
                close()
        self._client = None

    def cancel_active(self) -> None:
        with self._active_lock:
            container = self._active_container
        if container is None:
            return
        logger.info("Stopping active Hermes job container %s", container.id[:12])
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop Hermes job container %s", container.id[:12]
            )

    def run(
        self,
        job: Job,
        workspace: Path,
        deadline: float,
        max_steps: int,
        audit: JobAudit | None = None,
    ) -> AgentResult:
        del max_steps
        workspace = Path(workspace)
        prompt = self._build_prompt(job)
        remaining = max(0.0, deadline - time.monotonic())
        reflect_reserve = self._reflect_reserve(remaining)
        main_budget = max(
            1, int(remaining - reflect_reserve - _SCHEDULE_RESERVE_SECONDS)
        )
        environment = self._environment(
            budget=int(remaining),
            main_budget=main_budget,
            reflect_reserve=reflect_reserve,
        )
        created_at = int(time.time())
        labels = {
            _JOB_LABEL: "1",
            _JOB_ID_LABEL: job.job_id,
            _RUN_ID_LABEL: job.run_id,
            _CREATED_AT_LABEL: str(created_at),
        }
        attributes = {
            "openinference.span.kind": "AGENT",
            "agent.runtime": "hermes-job",
            "hermes.image": self.settings.hermes_image,
            "hermes.model": self.settings.hermes_model or "configured-default",
            "hermes.provider": (
                self.settings.hermes_provider or "configured-default"
            ),
            "job.id": job.job_id,
            "job.run_id": job.run_id,
        }
        container: Any | None = None
        volume: Any | None = None
        audit_completed = False
        run_started = time.perf_counter()

        with get_tracer().start_as_current_span(
            "hermes.job.run", attributes=attributes
        ) as span:
            set_content(span, "input.value", prompt)
            try:
                volume = self._create_workspace_volume(job, created_at)
                labels[_VOLUME_LABEL] = volume.name
                container = self._create_container(
                    environment=environment,
                    labels=labels,
                    volume_name=volume.name,
                )
                with self._active_lock:
                    self._active_container = container
                container.start()
                span.set_attribute("hermes.container_id", container.id)
                self._prepare_workspace(container, workspace)
                self._put_job_files(container, prompt)
                if audit:
                    audit.emit(
                        "hermes.started",
                        attributes={
                            "container_id": container.id,
                            "workspace": str(workspace),
                            "skills_volume": self.settings.hermes_skills_volume,
                            "model": self.settings.hermes_model or None,
                            "provider": self.settings.hermes_provider or None,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )

                exit_code, timed_out = self._wait(container, deadline)
                main_event = self._read_result(container, _MAIN_JSONL)
                main_error = self._read_text(container, _MAIN_STDERR)
                if main_event is None:
                    if timed_out:
                        raise HermesJobRuntimeError(
                            "Hermes job exceeded the job deadline before "
                            "producing a result."
                        )
                    detail = main_error.strip() or self._log_tail(container)
                    raise HermesJobRuntimeError(
                        "Hermes job container produced no result event "
                        f"(exit code {exit_code}). {detail[:4000]}"
                    )

                output = main_event.get("text")
                event_error = main_event.get("error")
                event_exit = main_event.get("exit_code")
                if (
                    event_error
                    or (
                        isinstance(event_exit, int)
                        and not isinstance(event_exit, bool)
                        and event_exit != 0
                    )
                    or not isinstance(output, str)
                    or not output.strip()
                ):
                    raise HermesJobRuntimeError(
                        self._event_error(main_event, exit_code)
                    )
                if len(output.encode("utf-8")) > _MAX_RESULT_BYTES:
                    raise HermesJobRuntimeError(
                        "Hermes response is too large for jobs.out."
                    )

                if timed_out:
                    logger.warning(
                        "Hermes job %s finished its main run, but the reflect "
                        "run exceeded the job deadline; returning the main result.",
                        job.job_id,
                    )
                usage = self._usage(main_event.get("tokens"))
                session_id = str(main_event.get("session_id") or "") or None
                span.set_attribute("hermes.session_id", session_id)
                span.set_attribute("hermes.exit_code", int(exit_code or 0))
                self._collect_outputs(container, workspace)
                self._audit_reflect(container, audit, reflect_reserve)
                if audit:
                    audit.emit(
                        "hermes.completed",
                        "succeeded",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={
                            "container_id": container.id,
                            "session_id": session_id,
                            "usage": usage,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )
                    audit_completed = True
                set_content(span, "output.value", output)
                return AgentResult(text=output, steps=0, usage=usage)
            except Exception as exc:
                set_error(span, exc)
                if audit and not audit_completed:
                    audit.emit(
                        "hermes.completed",
                        "failed",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={"container_id": container.id if container else None},
                        error=str(exc),
                    )
                raise
            finally:
                with self._active_lock:
                    if self._active_container is container:
                        self._active_container = None
                self._remove_container(container)
                self._remove_volume(volume)

    def _ensure_image(self) -> None:
        try:
            self.client.images.get(self.settings.hermes_image)
        except Exception as exc:
            raise HermesJobRuntimeError(
                f"Hermes image {self.settings.hermes_image} is not available. "
                "Start the stack with `docker compose up` or pull it with "
                "`docker compose pull hermes-agent`."
            ) from exc

    def _cleanup_orphans(self) -> None:
        max_age = self.settings.max_job_timeout_seconds + 300
        now = time.time()
        try:
            containers = self.client.containers.list(
                all=True, filters={"label": f"{_JOB_LABEL}=1"}
            )
        except Exception:
            logger.warning("Could not list orphaned Hermes job containers")
            containers = []
        for container in containers:
            labels = getattr(container, "labels", None) or {}
            created = self._label_age(labels, now)
            status = getattr(container, "status", "")
            if status == "running" and created is not None and created < max_age:
                continue
            logger.warning(
                "Removing orphaned Hermes job container %s", container.id[:12]
            )
            try:
                if status == "running":
                    container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                logger.warning(
                    "Could not remove orphaned Hermes job container %s",
                    container.id[:12],
                )
        try:
            volumes = self.client.volumes.list(
                filters={"label": f"{_JOB_LABEL}=1"}
            )
        except Exception:
            logger.warning("Could not list orphaned Hermes job volumes")
            return
        for volume in volumes:
            labels = getattr(volume, "attrs", {}).get("Labels", {}) or {}
            created = self._label_age(labels, now)
            if created is not None and created < max_age:
                continue
            logger.warning("Removing orphaned Hermes job volume %s", volume.name)
            try:
                volume.remove(force=True)
            except Exception:
                logger.warning(
                    "Could not remove orphaned Hermes job volume %s", volume.name
                )

    @staticmethod
    def _label_age(labels: dict[str, str], now: float) -> float | None:
        raw = labels.get(_CREATED_AT_LABEL)
        if not raw:
            return None
        try:
            return now - float(raw)
        except ValueError:
            return None

    def _create_workspace_volume(self, job: Job, created_at: int) -> Any:
        name = f"hermes-job-{job.run_id}"
        try:
            existing = self.client.volumes.get(name)
        except Exception as exc:
            if not self._is_not_found(exc):
                logger.debug("Checking for existing volume %s failed: %s", name, exc)
        else:
            existing.remove(force=True)
        return self.client.volumes.create(
            name=name,
            labels={
                _JOB_LABEL: "1",
                _JOB_ID_LABEL: job.job_id,
                _RUN_ID_LABEL: job.run_id,
                _CREATED_AT_LABEL: str(created_at),
            },
        )

    def _create_container(
        self,
        environment: dict[str, str],
        labels: dict[str, str],
        volume_name: str,
    ) -> Any:
        return self.client.containers.create(
            image=self.settings.hermes_image,
            command=[
                "sh",
                "-c",
                (
                    "i=0; while [ ! -f /tmp/hermes-go ]; do "
                    'i=$((i+1)); [ "$i" -gt 900 ] && exit 1; '
                    "sleep 0.2; done; exec sh /tmp/hermes-job.sh"
                ),
            ],
            environment=environment,
            labels=labels,
            network_mode="bridge",
            tmpfs={
                "/opt/data": (
                    f"size={self.settings.hermes_job_tmpfs_size},mode=0700"
                ),
            },
            volumes={
                volume_name: {"bind": "/workspace", "mode": "rw"},
                self.settings.hermes_skills_volume: {
                    "bind": "/opt/data/skills",
                    "mode": "rw",
                },
            },
            detach=True,
            auto_remove=False,
        )

    def _prepare_workspace(self, container: Any, workspace: Path) -> None:
        result = container.exec_run(
            [
                "sh",
                "-c",
                (
                    "mkdir -p /workspace/inputs /workspace/outputs && "
                    "chown -R 10001:10001 /workspace"
                ),
            ],
            user="root",
        )
        if getattr(result, "exit_code", 1) != 0:
            output = getattr(result, "output", b"")
            if isinstance(output, bytes):
                output = output.decode("utf-8", errors="replace")
            raise HermesJobRuntimeError(
                f"Could not prepare the Hermes job workspace: {output[:1000]}"
            )
        inputs = workspace / "inputs"
        if inputs.is_dir():
            data = self._tar_directory(inputs, "inputs")
            container.put_archive("/workspace", data)

    def _put_job_files(self, container: Any, prompt: str) -> None:
        files = {
            "hermes-job.sh": _RUN_SCRIPT.encode("utf-8"),
            "hermes-session-id.py": _SESSION_ID_SCRIPT.encode("utf-8"),
            "hermes-job-prompt.txt": prompt.encode("utf-8"),
            "hermes-reflect-prompt.txt": _REFLECT_PROMPT.encode("utf-8"),
        }
        container.put_archive("/tmp", self._tar_files(files))
        container.put_archive(
            "/tmp", self._tar_files({"hermes-go": b"1"})
        )

    def _wait(self, container: Any, deadline: float) -> tuple[int | None, bool]:
        while time.monotonic() < deadline:
            container.reload()
            if container.status not in {"created", "running", "restarting"}:
                wait_result = container.wait(timeout=10)
                return int(wait_result.get("StatusCode", 1)), False
            time.sleep(_CONTAINER_POLL_SECONDS)
        logger.warning(
            "Hermes job container %s exceeded the job deadline; stopping it",
            container.id[:12],
        )
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop timed-out Hermes job container %s",
                container.id[:12],
            )
        return None, True

    def _collect_outputs(self, container: Any, workspace: Path) -> None:
        target = workspace / "outputs"
        target.mkdir(parents=True, exist_ok=True)
        try:
            stream, _ = container.get_archive("/workspace/outputs")
        except Exception as exc:
            if self._is_not_found(exc):
                return
            raise HermesJobRuntimeError(
                f"Could not collect Hermes job outputs: {exc}"
            ) from exc
        data = b"".join(stream)
        if not data:
            return
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            self._extract_output_members(archive, target)

    @staticmethod
    def _extract_output_members(archive: tarfile.TarFile, target: Path) -> None:
        for member in archive.getmembers():
            name = member.name
            if name.startswith("./"):
                name = name[2:]
            if name.rstrip("/") == "outputs":
                continue
            if not name.startswith("outputs/"):
                continue
            relative = name[len("outputs/") :]
            if not relative:
                continue
            member.name = relative
            archive.extract(member, path=target, filter="data")

    def _read_result(self, container: Any, path: str) -> dict[str, Any] | None:
        raw = self._read_bytes(container, path)
        if not raw:
            return None
        result: dict[str, Any] | None = None
        for line in raw.decode("utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("type") == "result":
                result = event
        return result

    def _read_text(self, container: Any, path: str) -> str:
        raw = self._read_bytes(container, path)
        return raw.decode("utf-8", errors="replace") if raw else ""

    def _read_bytes(self, container: Any, path: str) -> bytes:
        try:
            stream, _ = container.get_archive(path)
        except Exception as exc:
            if self._is_not_found(exc):
                return b""
            logger.warning("Could not read %s from Hermes job container: %s", path, exc)
            return b""
        data = b"".join(stream)
        if not data:
            return b""
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                extracted = archive.extractfile(member)
                if extracted is not None:
                    return extracted.read()
        return b""

    def _log_tail(self, container: Any, lines: int = 200) -> str:
        try:
            logs = container.logs(stdout=True, stderr=True, tail=lines)
        except Exception:
            return ""
        if isinstance(logs, bytes):
            return logs.decode("utf-8", errors="replace")
        return str(logs)

    def _audit_reflect(
        self,
        container: Any,
        audit: JobAudit | None,
        reflect_reserve: int,
    ) -> None:
        if audit is None:
            return
        if not reflect_reserve:
            audit.emit(
                "hermes.reflect",
                attributes={"status": "skipped", "reason": "no time budget"},
            )
            return
        event = self._read_result(container, _REFLECT_JSONL)
        if event is None:
            stderr = self._read_text(container, _REFLECT_STDERR).strip()
            if stderr:
                audit.emit(
                    "hermes.reflect",
                    "failed",
                    attributes={"status": "failed"},
                    error=stderr[:4000],
                )
            else:
                audit.emit(
                    "hermes.reflect",
                    attributes={"status": "skipped"},
                )
            return
        error = event.get("error")
        event_exit = event.get("exit_code")
        failed = bool(error) or (
            isinstance(event_exit, int)
            and not isinstance(event_exit, bool)
            and event_exit != 0
        )
        if failed:
            message = str(error) if error else event.get("text")
            if not message:
                message = f"Reflect run ended with exit code {event_exit}."
            audit.emit(
                "hermes.reflect",
                "failed",
                attributes={
                    "status": "failed",
                    "usage": self._usage(event.get("tokens")),
                },
                error=str(message)[:4000],
            )
            return
        audit.emit(
            "hermes.reflect",
            "succeeded",
            attributes={
                "status": "succeeded",
                "session_id": event.get("session_id"),
                "usage": self._usage(event.get("tokens")),
            },
        )

    def _reflect_reserve(self, remaining: float) -> int:
        """Time reserved for the reflect pass when the main run uses its budget.

        The container computes the actual reflect timeout from the time the
        main run left unused, so this only bounds the main run.
        """
        if not self.settings.hermes_job_reflect_enabled:
            return 0
        if remaining < _REFLECT_MIN_REMAINING_SECONDS:
            return 0
        return max(
            1,
            min(
                self.settings.hermes_job_reflect_timeout_seconds,
                int(remaining * 0.25),
            ),
        )

    def _environment(
        self,
        budget: int,
        main_budget: int,
        reflect_reserve: int,
    ) -> dict[str, str]:
        environment = {
            "HERMES_UID": "10001",
            "HERMES_GID": "10001",
            "HOME": "/opt/data",
            "HERMES_WRITE_SAFE_ROOT": "/workspace:/opt/data",
            "TERMINAL_CWD": "/workspace",
            "HERMES_JOB_MAX_TURNS": str(self.settings.hermes_job_max_turns),
            "HERMES_JOB_BUDGET_SECONDS": str(max(0, budget)),
            "HERMES_JOB_MAIN_RUN_BUDGET": str(main_budget),
            "HERMES_JOB_MAIN_TIMEOUT": str(main_budget + 10),
            "HERMES_JOB_REFLECT_ENABLED": "1" if reflect_reserve else "0",
            "HERMES_JOB_REFLECT_TIMEOUT": str(
                self.settings.hermes_job_reflect_timeout_seconds
            ),
        }
        if self.settings.hermes_model:
            environment["HERMES_JOB_MODEL"] = self.settings.hermes_model
        if self.settings.hermes_provider:
            environment["HERMES_JOB_PROVIDER"] = self.settings.hermes_provider
        for key in _PROVIDER_ENV_KEYS:
            value = os.getenv(key)
            if value:
                environment[key] = value
        return environment

    def _build_prompt(self, job: Job) -> str:
        return (
            "Execute this job inside an isolated Hermes job container.\n"
            "Workspace root: /workspace\n"
            "Input files: /workspace/inputs\n"
            "Write all generated result files to: /workspace/outputs\n"
            "Do not modify files outside /workspace.\n"
            "Work autonomously; do not ask for approval or clarification.\n"
            "Return the completed result as your final response.\n\n"
            f"Instruction:\n{job.instruction}\n\n"
            "Structured job input:\n"
            f"{job.input}"
        )

    @staticmethod
    def _usage(raw: Any) -> dict[str, int]:
        if not isinstance(raw, dict):
            return {}
        mapping = {
            "input": "input_tokens",
            "output": "output_tokens",
            "total": "total_tokens",
            "cache_read": "cache_read_tokens",
            "cache_write": "cache_write_tokens",
        }
        return {
            mapping.get(key, key): int(value)
            for key, value in raw.items()
            if isinstance(value, int) and not isinstance(value, bool)
        }

    @staticmethod
    def _event_error(event: dict[str, Any], exit_code: int | None) -> str:
        error = event.get("error")
        if error:
            return str(error)[:4000]
        output = event.get("text")
        if isinstance(output, str) and output.strip():
            return f"Hermes job failed: {output.strip()[:4000]}"
        return f"Hermes job ended with exit code {exit_code or 'unknown'}."

    def _remove_container(self, container: Any | None) -> None:
        if container is None:
            return
        try:
            container.remove(force=True)
        except Exception as exc:
            if not self._is_not_found(exc):
                logger.warning(
                    "Could not remove Hermes job container %s: %s",
                    container.id[:12],
                    exc,
                )

    def _remove_volume(self, volume: Any | None) -> None:
        if volume is None:
            return
        try:
            volume.remove(force=True)
        except Exception as exc:
            if not self._is_not_found(exc):
                logger.warning(
                    "Could not remove Hermes job volume %s: %s", volume.name, exc
                )

    @staticmethod
    def _tar_files(files: dict[str, bytes]) -> bytes:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            for name, content in files.items():
                info = tarfile.TarInfo(name=name)
                info.size = len(content)
                info.mode = 0o600
                info.uid = 10001
                info.gid = 10001
                archive.addfile(info, io.BytesIO(content))
        return buffer.getvalue()

    @staticmethod
    def _tar_directory(path: Path, prefix: str) -> bytes:
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            for entry in sorted(path.rglob("*")):
                name = f"{prefix}/{entry.relative_to(path).as_posix()}"
                info = archive.gettarinfo(str(entry), arcname=name)
                info.uid = 10001
                info.gid = 10001
                if entry.is_file():
                    with entry.open("rb") as stream:
                        archive.addfile(info, stream)
                else:
                    archive.addfile(info)
        return buffer.getvalue()

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        return (
            getattr(exc, "status_code", None) == 404
            or exc.__class__.__name__ in {"NotFound", "ImageNotFound"}
        )
