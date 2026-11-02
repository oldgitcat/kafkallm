from __future__ import annotations

import json
import logging
import os
import shutil
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

_JOB_LABEL = "hermes.vm"
_JOB_ID_LABEL = "hermes.job_id"
_RUN_ID_LABEL = "hermes.run_id"
_CREATED_AT_LABEL = "hermes.created_at"
_WORKDIR_LABEL = "hermes.workdir"

_RESULT_DIR = "/workspace/.hermes"
_MAIN_JSONL = f"{_RESULT_DIR}/main.jsonl"
_MAIN_STDERR = f"{_RESULT_DIR}/main.err"
_REFLECT_JSONL = f"{_RESULT_DIR}/reflect.jsonl"
_REFLECT_STDERR = f"{_RESULT_DIR}/reflect.err"

_BUNDLE_DIR = "/jobboot"

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

# The job bundle is bind mounted into the microVM, so the script reads its
# inputs from /jobboot and writes its results into the shared workspace. That
# keeps every artifact on the host, where the worker can collect it.
_VM_RUN_SCRIPT = r"""#!/bin/sh
set -u

RESULT_DIR=/workspace/.hermes
if ! mkdir -p "$RESULT_DIR" 2>/dev/null; then
  echo "Cannot create $RESULT_DIR in the shared workspace." >&2
  exit 3
fi
if [ ! -w "$RESULT_DIR" ]; then
  echo "$RESULT_DIR is not writable. The job uid must own the workspace." >&2
  exit 3
fi

# Ephemeral jobs keep skills, but deliberately disable the small per-agent
# memory stores and the automatic post-turn review. Learning happens in the
# explicit reflect run below, before this container is removed.
hermes config set memory.memory_enabled false >/dev/null 2>&1 || true
hermes config set memory.user_profile_enabled false >/dev/null 2>&1 || true
hermes config set auxiliary.background_review.enabled false --force >/dev/null 2>&1 || true

START=$(date +%s)
BUDGET="${HERMES_JOB_BUDGET_SECONDS:-0}"
RESERVE=25

hermes config set agent.run_budget_seconds "${HERMES_JOB_MAIN_RUN_BUDGET:-300}" >/dev/null 2>&1 || true

set -- --oneshot --query-file /jobboot/hermes-job-prompt.txt --format stream-json --source tool --yolo --in /workspace
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
  > "$RESULT_DIR/main.jsonl" 2> "$RESULT_DIR/main.err"
MAIN_RC=$?

if [ "$MAIN_RC" -ne 0 ]; then
  exit "$MAIN_RC"
fi

if [ "${HERMES_JOB_REFLECT_ENABLED:-0}" != "1" ]; then
  exit 0
fi

SESSION_ID=$(/opt/hermes/.venv/bin/python /jobboot/hermes-session-id.py "$RESULT_DIR/main.jsonl" 2>/dev/null || true)
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
set -- --oneshot --resume "$SESSION_ID" --query-file /jobboot/hermes-reflect-prompt.txt --format stream-json --source tool --yolo --in /workspace
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
  > "$RESULT_DIR/reflect.jsonl" 2> "$RESULT_DIR/reflect.err"
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


class HermesVmRuntimeError(RuntimeError):
    pass


class HermesVmJobRunner:
    """Run each job in its own KVM microVM through Podman's krun runtime.

    Podman exposes a Docker compatible API, so the container lifecycle uses the
    same calls as the plain container runtime. Two details differ: the runtime
    is selected through OCI annotations, and the job bundle is a bind mount
    instead of an archive, because the guest filesystem lives inside the
    microVM.
    """

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

            self._client = docker.DockerClient(
                base_url=self.settings.hermes_vm_docker_host,
                timeout=120,
            )
        return self._client

    def wait_ready(self) -> None:
        deadline = (
            time.monotonic() + self.settings.hermes_startup_timeout_seconds
        )
        delay = 1.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.client.ping()
                self._ensure_image()
                self._cleanup_orphans()
                logger.info(
                    "Hermes microVM runtime is ready (image=%s, docker_host=%s)",
                    self.settings.hermes_vm_image,
                    self.settings.hermes_vm_docker_host,
                )
                return
            except Exception as exc:
                last_error = exc
                remaining = max(0.0, deadline - time.monotonic())
                logger.warning(
                    "Hermes microVM runtime not ready yet (%s); retrying in %.1fs",
                    exc,
                    min(delay, remaining),
                )
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 10.0)
        raise HermesVmRuntimeError(
            "Hermes microVM runtime did not become ready within the startup "
            "timeout."
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
        logger.info("Stopping active Hermes microVM container %s", container.id[:12])
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop Hermes microVM container %s", container.id[:12]
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
        workdir = self._workdir(job)
        labels = {
            _JOB_LABEL: "1",
            _JOB_ID_LABEL: job.job_id,
            _RUN_ID_LABEL: job.run_id,
            _CREATED_AT_LABEL: str(created_at),
            _WORKDIR_LABEL: str(workdir),
        }
        attributes = {
            "openinference.span.kind": "AGENT",
            "agent.runtime": "hermes-job-vm",
            "hermes.image": self.settings.hermes_vm_image,
            "hermes.model": self.settings.hermes_model or "configured-default",
            "hermes.provider": (
                self.settings.hermes_provider or "configured-default"
            ),
            "job.id": job.job_id,
            "job.run_id": job.run_id,
        }
        container: Any | None = None
        run_started = time.perf_counter()

        with get_tracer().start_as_current_span(
            "hermes.job.vm.run", attributes=attributes
        ) as span:
            set_content(span, "input.value", prompt)
            try:
                self._prepare_workdir(workdir, workspace, prompt)
                container = self._create_container(
                    environment=environment,
                    labels=labels,
                    workdir=workdir,
                )
                with self._active_lock:
                    self._active_container = container
                container.start()
                span.set_attribute("hermes.container_id", container.id)
                if audit:
                    audit.emit(
                        "hermes.started",
                        attributes={
                            "container_id": container.id,
                            "workspace": str(workdir),
                            "skills_dir": self.settings.hermes_vm_skills_dir,
                            "model": self.settings.hermes_model or None,
                            "provider": self.settings.hermes_provider or None,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )

                exit_code, timed_out = self._wait(container, deadline)
                main_event = self._read_result(workdir, "main.jsonl")
                main_error = self._read_text(workdir, "main.err")
                if main_event is None:
                    if timed_out:
                        raise HermesVmRuntimeError(
                            "Hermes job exceeded the job deadline before "
                            "producing a result."
                        )
                    detail = main_error.strip() or self._log_tail(container)
                    raise HermesVmRuntimeError(
                        "Hermes microVM produced no result event "
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
                    raise HermesVmRuntimeError(
                        self._event_error(main_event, exit_code)
                    )
                if len(output.encode("utf-8")) > _MAX_RESULT_BYTES:
                    raise HermesVmRuntimeError(
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
                self._collect_outputs(workdir, workspace)
                self._audit_reflect(workdir, audit, reflect_reserve)
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
                set_content(span, "output.value", output)
                return AgentResult(text=output, steps=0, usage=usage)
            except Exception as exc:
                set_error(span, exc)
                if audit:
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
                self._remove_workdir(workdir)

    def _ensure_image(self) -> None:
        try:
            self.client.images.get(self.settings.hermes_vm_image)
            return
        except Exception:
            pass
        logger.info(
            "Pulling %s into the Podman storage", self.settings.hermes_vm_image
        )
        try:
            self.client.images.pull(self.settings.hermes_vm_image)
        except Exception as exc:
            raise HermesVmRuntimeError(
                f"Hermes image {self.settings.hermes_vm_image} is not available "
                f"in the Podman storage and could not be pulled: {exc}"
            ) from exc
        try:
            self.client.images.get(self.settings.hermes_vm_image)
        except Exception as exc:
            raise HermesVmRuntimeError(
                f"Hermes image {self.settings.hermes_vm_image} is not available "
                "in the Podman storage."
            ) from exc

    def _cleanup_orphans(self) -> None:
        max_age = self.settings.max_job_timeout_seconds + 300
        now = time.time()
        try:
            containers = self.client.containers.list(
                all=True, filters={"label": f"{_JOB_LABEL}=1"}
            )
        except Exception:
            logger.warning("Could not list orphaned Hermes microVM containers")
            containers = []
        for container in containers:
            labels = getattr(container, "labels", None) or {}
            created = self._label_age(labels, now)
            status = getattr(container, "status", "")
            if status == "running" and created is not None and created < max_age:
                continue
            logger.warning(
                "Removing orphaned Hermes microVM container %s",
                container.id[:12],
            )
            try:
                if status == "running":
                    container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                logger.warning(
                    "Could not remove orphaned Hermes microVM container %s",
                    container.id[:12],
                )
        for workdir in self._orphan_workdirs():
            logger.warning("Removing orphaned Hermes job directory %s", workdir)
            shutil.rmtree(workdir, ignore_errors=True)

    def _orphan_workdirs(self) -> list[Path]:
        root = Path(self.settings.hermes_vm_workspace_root)
        if not root.is_dir():
            return []
        orphans: list[Path] = []
        for entry in root.iterdir():
            if not entry.is_dir() or not entry.name.startswith("hermes-vm-"):
                continue
            try:
                age = time.time() - entry.stat().st_mtime
            except OSError:
                continue
            if age > self.settings.max_job_timeout_seconds + 300:
                orphans.append(entry)
        return orphans

    @staticmethod
    def _label_age(labels: dict[str, str], now: float) -> float | None:
        raw = labels.get(_CREATED_AT_LABEL)
        if not raw:
            return None
        try:
            return now - float(raw)
        except ValueError:
            return None

    def _workdir(self, job: Job) -> Path:
        return (
            Path(self.settings.hermes_vm_workspace_root)
            / f"hermes-vm-{job.run_id}"
        )

    def _prepare_workdir(self, workdir: Path, workspace: Path, prompt: str) -> None:
        shutil.rmtree(workdir, ignore_errors=True)
        (workdir / "inputs").mkdir(parents=True, exist_ok=True)
        (workdir / "outputs").mkdir(parents=True, exist_ok=True)
        (workdir / "bundle").mkdir(parents=True, exist_ok=True)
        # The guest writes its results here. Creating it on the host means the
        # job only needs write access, not the right to create directories.
        (workdir / ".hermes").mkdir(parents=True, exist_ok=True)
        inputs = workspace / "inputs"
        if inputs.is_dir():
            for entry in inputs.rglob("*"):
                target = workdir / "inputs" / entry.relative_to(inputs)
                if entry.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                elif entry.is_file():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(entry, target)
        bundle = workdir / "bundle"
        (bundle / "hermes-job.sh").write_text(_VM_RUN_SCRIPT, encoding="utf-8")
        (bundle / "hermes-session-id.py").write_text(
            _SESSION_ID_SCRIPT, encoding="utf-8"
        )
        (bundle / "hermes-job-prompt.txt").write_text(prompt, encoding="utf-8")
        (bundle / "hermes-reflect-prompt.txt").write_text(
            _REFLECT_PROMPT, encoding="utf-8"
        )
        os.chmod(bundle / "hermes-job.sh", 0o755)

    def _create_container(
        self,
        environment: dict[str, str],
        labels: dict[str, str],
        workdir: Path,
    ) -> Any:
        annotations = {
            "run.oci.handler": "krun",
            "krun.cpus": str(self.settings.hermes_vm_cpus),
            "krun.ram_mib": str(self.settings.hermes_vm_ram_mib),
        }
        if self.settings.hermes_vm_use_passt:
            annotations["krun.use_passt"] = "1"
        body = {
            "Image": self.settings.hermes_vm_image,
            "Cmd": ["/bin/sh", "/jobboot/hermes-job.sh"],
            "Env": [f"{key}={value}" for key, value in environment.items()],
            "Labels": labels,
            "WorkingDir": "/workspace",
            "HostConfig": {
                "Binds": [
                    f"{workdir}:/workspace:rw",
                    f"{workdir}/bundle:/jobboot:ro",
                    f"{self.settings.hermes_vm_skills_dir}:/opt/data/skills:rw",
                ],
                "Tmpfs": {
                    "/opt/data": (
                        f"size={self.settings.hermes_job_tmpfs_size},mode=0700"
                    )
                },
                "Annotations": annotations,
                "NetworkMode": "bridge",
                "Memory": self._memory_bytes(),
                "MemorySwap": self._memory_bytes(),
                "PidsLimit": self.settings.hermes_job_pids_limit,
                "NanoCpus": int(self.settings.hermes_job_cpus * 1_000_000_000),
                "SecurityOpt": ["no-new-privileges:true"],
            },
        }
        api = self.client.api
        response = api._post(  # noqa: SLF001
            api._url("/containers/create"),  # noqa: SLF001
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code >= 300:
            raise HermesVmRuntimeError(
                "Could not create the Hermes microVM container: "
                f"{response.status_code} {response.text[:1000]}"
            )
        container_id = response.json()["Id"]
        return self.client.containers.get(container_id)

    def _memory_bytes(self) -> int:
        limit = self.settings.hermes_job_memory_limit.strip()
        if not limit or limit == "0":
            return 0
        return _parse_bytes(limit)

    def _wait(self, container: Any, deadline: float) -> tuple[int | None, bool]:
        while time.monotonic() < deadline:
            container.reload()
            if container.status not in {"created", "running", "restarting"}:
                wait_result = container.wait(timeout=10)
                return int(wait_result.get("StatusCode", 1)), False
            time.sleep(_CONTAINER_POLL_SECONDS)
        logger.warning(
            "Hermes microVM container %s exceeded the job deadline; stopping it",
            container.id[:12],
        )
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop timed-out Hermes microVM container %s",
                container.id[:12],
            )
        return None, True

    def _collect_outputs(self, workdir: Path, workspace: Path) -> None:
        source = workdir / "outputs"
        target = workspace / "outputs"
        target.mkdir(parents=True, exist_ok=True)
        if not source.is_dir():
            return
        for entry in source.rglob("*"):
            if not entry.is_file():
                continue
            destination = target / entry.relative_to(source)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, destination)

    def _read_result(self, workdir: Path, name: str) -> dict[str, Any] | None:
        raw = self._read_text(workdir, name)
        if not raw:
            return None
        result: dict[str, Any] | None = None
        for line in raw.splitlines():
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

    def _read_text(self, workdir: Path, name: str) -> str:
        path = workdir / ".hermes" / name
        if not path.is_file():
            return ""
        return path.read_text(encoding="utf-8", errors="replace")

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
        workdir: Path,
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
        event = self._read_result(workdir, "reflect.jsonl")
        if event is None:
            stderr = self._read_text(workdir, "reflect.err").strip()
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
            "Execute this job inside an isolated Hermes job microVM.\n"
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
                    "Could not remove Hermes microVM container %s: %s",
                    container.id[:12],
                    exc,
                )

    def _remove_workdir(self, workdir: Path) -> None:
        shutil.rmtree(workdir, ignore_errors=True)

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        return (
            getattr(exc, "status_code", None) == 404
            or exc.__class__.__name__ in {"NotFound", "ImageNotFound"}
        )


def _parse_bytes(value: str) -> int:
    """Parse a Docker style size string such as 4g into bytes."""
    text = value.strip().lower()
    units = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
        "t": 1024**4,
        "tb": 1024**4,
    }
    digits = ""
    index = 0
    while index < len(text) and (text[index].isdigit() or text[index] == "."):
        digits += text[index]
        index += 1
    unit = text[index:].strip()
    if not digits:
        raise ValueError(f"Invalid size value: {value}")
    multiplier = units.get(unit, 1) if unit else 1
    return int(float(digits) * multiplier)
