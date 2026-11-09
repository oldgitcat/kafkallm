from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from kafkallm.audit import JobAudit
from kafkallm.goose_output import parse_goose_output, stream_event_log_records
from kafkallm.job_network import ensure_job_network
from kafkallm.models import AgentResult, AgentSpec, Job, SandboxSpec
from kafkallm.sandboxes import SandboxStore, builtin_sandbox
from kafkallm.settings import Settings
from kafkallm.telemetry import get_tracer, set_content, set_error

logger = logging.getLogger(__name__)

# Private alias kept for the log collector and existing tests.
_stream_event_log_records = stream_event_log_records

_JOB_LABEL = "kafkallm.vm"
_JOB_ID_LABEL = "kafkallm.job_id"
_RUN_ID_LABEL = "kafkallm.run_id"
_CREATED_AT_LABEL = "kafkallm.created_at"
_WORKDIR_LABEL = "kafkallm.workdir"

_PROXY_NAME = "kafkallm-gateway-proxy"
_PROXY_LABEL = "kafkallm.gateway_proxy"
_PROXY_SCRIPT = "/opt/kafkallm/gateway_proxy.py"

_REFLECT_MIN_REMAINING_SECONDS = 60
_SCHEDULE_RESERVE_SECONDS = 25
_CONTAINER_POLL_SECONDS = 1.0
_MAX_RESULT_BYTES = 500_000

# The job bundle is bind mounted read-only. The goosetown template and the
# persistent skills live outside it. Every artifact is written to /workspace,
# which is the host job directory.
_VM_RUN_SCRIPT = r"""#!/bin/sh
set -u

RESULT_DIR=/workspace/.kafkallm
if ! mkdir -p "$RESULT_DIR" 2>/dev/null; then
  echo "Cannot create $RESULT_DIR in the shared workspace." >&2
  exit 3
fi

# Seed the workspace with goosetown (AGENTS.md, the goose wrapper, gtwall) and
# the orchestrator role skills. The skills directory is a persistent host bind
# mount, so a skill improved by one job is available to the next one.
if [ -d /opt/goosetown/template ]; then
  cp -an /opt/goosetown/template/. /workspace/ 2>/dev/null || true
fi
mkdir -p /workspace/.claude/skills
seed_skills() {
  source_dir=$1
  target_dir=$2
  [ -d "$source_dir" ] || return 0
  mkdir -p "$target_dir"
  for skill in "$source_dir"/*; do
    [ -e "$skill" ] || continue
    name=$(basename "$skill")
    if [ ! -e "$target_dir/$name" ]; then
      cp -a "$skill" "$target_dir/" 2>/dev/null || true
    fi
  done
}
seed_skills /opt/goosetown/skills /workspace/.claude/skills
# Sandbox-specific skills are baked into the image (for example Kuna and REA
# guidance in the reverse image) and are not visible to other sandboxes.
seed_skills /opt/kafkallm/sandbox-skills /workspace/.claude/skills
# REA's own doctor looks for its skill under HOME as well.
seed_skills /opt/kafkallm/sandbox-skills /opt/data/.claude/skills
chmod +x /workspace/goose /workspace/gtwall 2>/dev/null || true

# Seed goose's user config from the host-mounted sandbox config. This is how
# operators add MCP servers and other goose extensions without rebuilding.
if [ -d /opt/kafkallm/config ]; then
  mkdir -p "$GOOSE_PATH_ROOT/config"
  cp -an /opt/kafkallm/config/. "$GOOSE_PATH_ROOT/config/" 2>/dev/null || true
fi

cd /workspace || exit 3
export GOOSE_BIN="${GOOSE_BIN:-/usr/local/goose/bin/goose}"
export GOOSE_MOIM_MESSAGE_FILE="/tmp/goose-telepathy.txt"
export GOOSE_GTWALL_FILE="/tmp/goose-wall.log"
: > "$GOOSE_MOIM_MESSAGE_FILE"
: > "$GOOSE_GTWALL_FILE"

started=$(date +%s)
budget=${KAFKALLM_JOB_BUDGET_SECONDS:-0}
main_timeout=${KAFKALLM_JOB_MAIN_TIMEOUT:-300}

# Summon provides the delegate/load tools, and Skills makes the goosetown role
# skills discoverable inside the orchestrator and its delegates. Keep both out
# of single-agent runs so those jobs cannot spawn subagents by accident.
builtins=developer
if [ "${KAFKALLM_MULTI_AGENT:-0}" = "1" ]; then
  builtins="$builtins,summon,skills"
fi

# The sandbox image may ship local stdio MCP servers as extensions. Ghidra and
# other tools also need a large scratch area on the host-backed workspace, not
# the small /tmp tmpfs.
mkdir -p /workspace/.scratch/tmp
# krun maps guest writes to the host user that runs Podman, while the worker
# collects the workspace as a different UID. Keep the scratch tree writable
# so the worker can remove it after the job.
chmod 0777 /workspace/.scratch /workspace/.scratch/tmp 2>/dev/null || true
export TMPDIR=/workspace/.scratch/tmp

set --
if [ -n "${KAFKALLM_SANDBOX_EXTENSIONS:-}" ]; then
  old_ifs=$IFS
  IFS=,
  for extension in $KAFKALLM_SANDBOX_EXTENSIONS; do
    extension_path=${extension#*:}
    if [ -x "$extension_path" ]; then
      set -- "$@" --with-extension "$extension"
    else
      echo "Sandbox extension $extension is missing at $extension_path." >&2
    fi
  done
  IFS=$old_ifs
fi

timeout --signal=TERM "$main_timeout" \
  stdbuf -oL -eL ./goose run --no-session -q --output-format stream-json \
    --with-builtin "$builtins" \
    --max-turns "${GOOSE_MAX_TURNS:-60}" \
    "$@" \
    -i /jobboot/main-prompt.txt \
  > "$RESULT_DIR/main.jsonl" 2> "$RESULT_DIR/main.err"
echo "$?" > "$RESULT_DIR/main.exit"
if [ "$(cat "$RESULT_DIR/main.exit")" = "124" ] \
   && [ ! -s "$RESULT_DIR/main.jsonl" ]; then
  cat > "$RESULT_DIR/main.jsonl" <<'JSON'
{"type":"error","message":"goose exceeded the main time budget."}
JSON
fi

if [ "${KAFKALLM_JOB_REFLECT_ENABLED:-0}" = "1" ]; then
  now=$(date +%s)
  left=$(( budget - (now - started) - 25 ))
  cap=${KAFKALLM_JOB_REFLECT_TIMEOUT:-300}
  if [ "$left" -ge 30 ]; then
    reflect_timeout=$cap
    if [ "$left" -lt "$cap" ]; then
      reflect_timeout=$left
    fi
    timeout --signal=TERM "$reflect_timeout" \
      stdbuf -oL -eL ./goose run --no-session -q --output-format stream-json \
        --with-builtin developer \
        --max-turns "${GOOSE_REFLECT_MAX_TURNS:-20}" \
        -i /jobboot/reflect-prompt.txt \
      > "$RESULT_DIR/reflect.jsonl" 2> "$RESULT_DIR/reflect.err"
    echo "$?" > "$RESULT_DIR/reflect.exit"
  fi
fi

# Ghidra and other tools may create read-only files in the scratch tree. Make
# everything removable by the worker UID before the microVM exits.
chmod -R a+rwX /workspace/.scratch 2>/dev/null || true
"""

_REFLECT_PROMPT = """Review the job that just completed and improve the reusable goosetown skills for this class of job.

Never read or parse `/workspace/.kafkallm/main.jsonl`; it is a verbose streaming
transcript. Use the workspace outputs, the git diff, and the existing skills.
This pass has only the developer builtin: use shell and file tools, not
`load_skill` or `read_resource`.

1. Inspect the most relevant skill in /workspace/.claude/skills with the file tools.
2. Update the best match, or create one class-level skill if none fits. Never name a skill after this job, run, error, or one-off artifact.
3. Capture durable procedures, commands, tool quirks, verification steps, and templates. Do not store secrets, credentials, customer data, job-specific outputs, or transient facts.
4. If there is nothing durable to save, do nothing.

Keep it short: at most one skill change and a few tool calls. Return a one-line summary.
"""


def _agent_source(agent: AgentSpec) -> str:
    """Render one selected flock member as a goosetown agent source.

    goose's Summon extension discovers agent sources from `.claude/agents/*.md`
    and exposes them through `delegate(source: "<name>")`. The YAML values are
    JSON strings because JSON is valid YAML and safely quotes multiline text.
    """
    description = agent.description.strip() or f"{agent.role} delegate."
    instructions = agent.instructions.strip() or "No additional role instructions."
    if agent.skills:
        skills = "\n".join(
            f'- `{skill}`: call `load_skill(name: "{skill}")` before starting. '
            "If that tool is unavailable, read "
            f"`/workspace/.claude/skills/{skill}/SKILL.md` and follow it."
            for skill in agent.skills
        )
    else:
        skills = "- No reusable skill is assigned."
    return (
        "---\n"
        f"name: {json.dumps(agent.name)}\n"
        f"description: {json.dumps(description)}\n"
        "---\n\n"
        f"You are {agent.name}, the {agent.role} delegate in one isolated "
        "Kafkallm job.\n\n"
        f"{description}\n\n"
        "Role instructions:\n"
        f"{instructions}\n\n"
        "Reusable skills:\n"
        f"{skills}\n\n"
        "Coordination:\n"
        "- Your gtwall ID is provided in the delegate instructions. FIRST "
        "ACTION: run `./gtwall --usage`, then post and read updates with "
        "`./gtwall <your-id>`.\n"
        "- Work only on the files assigned by the orchestrator. Other delegates "
        "may be working in parallel, so never edit files they own.\n"
        "- Write results incrementally to the exact output path assigned to "
        "you, and report that path when you finish.\n"
        "- You cannot spawn further delegates.\n"
        "- There is no web access. Use only /workspace and the installed "
        "command-line tools.\n"
    )


class GooseJobRuntimeError(RuntimeError):
    pass


class JobCancelledError(GooseJobRuntimeError):
    pass


class WorkerShutdown(GooseJobRuntimeError):
    pass


class _LogCollector:
    """Tail the job workspace and turn goose stream-json into live activity."""

    def __init__(self, workdir: Path, log: Callable[[str, str], None]) -> None:
        self.workdir = workdir
        self.log = log
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        jobs = (
            ("main.err", "stderr", False, ""),
            ("reflect.err", "reflect.stderr", False, ""),
            ("main.jsonl", "goose", True, "goose"),
            ("reflect.jsonl", "reflect", True, "reflect"),
        )
        for name, kind, parse_json, prefix in jobs:
            thread = threading.Thread(
                target=self._tail,
                args=(self.workdir / ".kafkallm" / name, kind, parse_json, prefix),
                name=f"kafkallm-log-{name}",
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2)

    def _tail(
        self,
        path: Path,
        kind: str,
        parse_json: bool,
        prefix: str,
    ) -> None:
        pending: dict[str, str] = {}

        def flush(kind_name: str | None = None) -> None:
            if kind_name is None:
                for name in list(pending):
                    flush(name)
                return
            text = pending.pop(kind_name, "").strip()
            if text:
                self.log(kind_name, text)

        def emit(record_kind: str, text: str) -> None:
            if record_kind.endswith(
                (".tool", ".tool_result", ".usage", ".error", ".raw")
            ):
                flush()
                self.log(record_kind, text)
                return
            if record_kind not in pending:
                flush()
                pending[record_kind] = ""
            pending[record_kind] += text
            if len(pending[record_kind]) >= 400 or "\n" in pending[record_kind]:
                flush(record_kind)

        while not self._stop.is_set():
            try:
                stream = path.open("r", encoding="utf-8", errors="replace")
            except OSError:
                self._stop.wait(0.5)
                continue
            idle_since = time.monotonic()
            try:
                while not self._stop.is_set():
                    line = stream.readline()
                    if line == "":
                        if pending and time.monotonic() - idle_since >= 1.0:
                            flush()
                        self._stop.wait(0.5)
                        continue
                    idle_since = time.monotonic()
                    line = line.rstrip("\r\n")
                    if not line:
                        continue
                    if not parse_json:
                        self.log(kind, line)
                        continue
                    try:
                        event = json.loads(line)
                    except ValueError:
                        emit(f"{prefix}.raw", line)
                        continue
                    if isinstance(event, dict):
                        for record_kind, text in _stream_event_log_records(
                            event, prefix
                        ):
                            emit(record_kind, text)
                    else:
                        emit(f"{prefix}.raw", line)
            finally:
                flush()
                stream.close()


class GooseJobRunner:
    """Run each job in its own KVM microVM through Podman's krun runtime.

    The microVM runs the goosetown wrapper around the goose binary. Podman
    exposes a Docker compatible API, so the lifecycle uses ordinary container
    calls; the runtime is selected through OCI annotations and the job bundle
    is a bind mount because the guest filesystem lives inside the microVM.
    """

    def __init__(
        self,
        settings: Settings,
        client: Any | None = None,
        sandboxes: SandboxStore | None = None,
    ) -> None:
        self.settings = settings
        self._client = client
        self._owns_client = client is None
        self.sandboxes = sandboxes
        self._active_container: Any | None = None
        self._active_lock = threading.Lock()
        self._proxy_ipv6: str | None = None
        self._proxy_lock = threading.Lock()

    @property
    def client(self) -> Any:
        if self._client is None:
            import docker

            self._client = docker.DockerClient(
                base_url=self.settings.goose_job_docker_host,
                timeout=120,
            )
        return self._client

    def wait_ready(self) -> None:
        if not self.settings.gateway_configured:
            raise GooseJobRuntimeError(
                "The LLM gateway is not configured. Set LLM_GATEWAY_URL and "
                "LLM_GATEWAY_VIRTUAL_KEY for the worker."
            )
        deadline = time.monotonic() + self.settings.goose_startup_timeout_seconds
        self._wait_for_gateway(deadline)
        delay = 1.0
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                self.client.ping()
                self._ensure_image()
                self._ensure_extension_dirs()
                self._ensure_job_network()
                self._ensure_proxy()
                self._cleanup_orphans()
                logger.info(
                    "goose microVM runtime is ready (image=%s, docker_host=%s, "
                    "network=%s)",
                    self.settings.goose_image,
                    self.settings.goose_job_docker_host,
                    self.settings.goose_job_network,
                )
                return
            except Exception as exc:
                last_error = exc
                remaining = max(0.0, deadline - time.monotonic())
                logger.warning(
                    "goose microVM runtime not ready yet (%s); retrying in %.1fs",
                    exc,
                    min(delay, remaining),
                )
                time.sleep(min(delay, remaining))
                delay = min(delay * 2, 10.0)
        raise GooseJobRuntimeError(
            "goose microVM runtime did not become ready within the startup timeout."
        ) from last_error

    def _wait_for_gateway(self, deadline: float) -> None:
        ready_url = self.settings.llm_gateway_ready_url.strip()
        if not ready_url:
            return
        last_error: Exception | None = None
        delay = 1.0
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(ready_url, timeout=3) as response:
                    if 200 <= response.status < 300:
                        logger.info("LLM gateway is ready at %s", ready_url)
                        return
                    last_error = GooseJobRuntimeError(
                        f"LLM gateway readiness returned HTTP {response.status}"
                    )
            except (OSError, urllib.error.URLError) as exc:
                last_error = exc
            remaining = max(0.0, deadline - time.monotonic())
            logger.warning(
                "LLM gateway not ready yet (%s); retrying in %.1fs",
                last_error,
                min(delay, remaining),
            )
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, 10.0)
        raise GooseJobRuntimeError(
            f"LLM gateway at {ready_url} did not become ready within the "
            "startup timeout."
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
        logger.info("Stopping active goose microVM container %s", container.id[:12])
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop goose microVM container %s", container.id[:12]
            )

    def run(
        self,
        job: Job,
        workspace: Path,
        deadline: float,
        max_steps: int,
        audit: JobAudit | None = None,
        heartbeat: Callable[[], bool] | None = None,
        log: Callable[[str, str], None] | None = None,
    ) -> AgentResult:
        del max_steps
        workspace = Path(workspace)
        if heartbeat is not None and heartbeat():
            raise JobCancelledError("Job cancelled by user.")
        sandbox = self._sandbox_for(job)
        image = job.sandbox_image or sandbox.image
        prompt = self._build_prompt(job, sandbox=sandbox)
        remaining = max(0.0, deadline - time.monotonic())
        reflect_enabled = (
            job.reflect
            if job.reflect is not None
            else self.settings.goose_job_reflect_enabled
        )
        reflect_reserve = self._reflect_reserve(remaining, enabled=reflect_enabled)
        main_budget = max(
            1, int(remaining - reflect_reserve - _SCHEDULE_RESERVE_SECONDS)
        )
        environment = self._environment(
            job_id=job.job_id,
            budget=int(remaining),
            main_budget=main_budget,
            reflect_reserve=reflect_reserve,
            sandbox=sandbox,
            multi_agent=job.multi_agent,
            agent_count=len(job.agents),
            unlimited=job.unlimited,
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
            "agent.runtime": "goose-job",
            "goose.image": image,
            "goose.sandbox": job.sandbox,
            "goose.model": self.settings.goose_model or "configured-default",
            "goose.provider": "openai",
            "goose.upstream_provider": (self.settings.goose_provider or "gateway"),
            "llm.gateway": self.settings.llm_gateway_url,
            "job.id": job.job_id,
            "job.run_id": job.run_id,
        }
        container: Any | None = None
        collector: _LogCollector | None = None
        run_started = time.perf_counter()

        with get_tracer().start_as_current_span(
            "goose.job.vm.run", attributes=attributes
        ) as span:
            set_content(span, "input.value", prompt)
            try:
                self._ensure_image(image)
                self._prepare_workdir(workdir, workspace, prompt, job)
                self._ensure_extension_dirs()
                proxy_ipv6 = self._ensure_proxy()
                container = self._create_container(
                    image=image,
                    environment=environment,
                    labels=labels,
                    workdir=workdir,
                    proxy_ipv6=proxy_ipv6,
                )
                with self._active_lock:
                    self._active_container = container
                container.start()
                span.set_attribute("goose.container_id", container.id)
                if log is not None:
                    log(
                        "system",
                        f"container {container.id[:12]} started "
                        f"(image={image}, sandbox={job.sandbox})",
                    )
                    collector = _LogCollector(workdir, log)
                    collector.start()
                if audit:
                    audit.emit(
                        "goose.started",
                        attributes={
                            "container_id": container.id,
                            "workspace": str(workdir),
                            "skills_dir": self.settings.goose_job_skills_dir,
                            "image": image,
                            "sandbox": job.sandbox,
                            "model": self.settings.goose_model or None,
                            "provider": "openai",
                            "upstream_provider": (
                                self.settings.goose_provider or "gateway"
                            ),
                            "gateway": self.settings.llm_gateway_url,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )

                exit_code, timed_out = self._wait(
                    container, deadline, heartbeat=heartbeat
                )
                raw = self._read_text(workdir, "main.jsonl")
                if not raw.strip():
                    raw = self._read_text(workdir, "main.json")
                main_error = self._read_text(workdir, "main.err")
                if not raw.strip():
                    if timed_out:
                        raise GooseJobRuntimeError(
                            "goose job exceeded the job deadline before "
                            "producing a result."
                        )
                    detail = main_error.strip() or self._log_tail(container)
                    raise GooseJobRuntimeError(
                        "goose microVM produced no result "
                        f"(exit code {exit_code}). {detail[:4000]}"
                    )

                output, usage, steps, event_error = parse_goose_output(raw)
                if event_error:
                    raise GooseJobRuntimeError(event_error[:4000])
                if exit_code not in (None, 0):
                    raise GooseJobRuntimeError(
                        f"goose exited with code {exit_code}. "
                        f"{(main_error.strip() or output)[:4000]}"
                    )
                if not output.strip():
                    raise GooseJobRuntimeError("goose returned an empty response.")
                if (
                    not job.unlimited
                    and len(output.encode("utf-8")) > _MAX_RESULT_BYTES
                ):
                    raise GooseJobRuntimeError(
                        "goose response is too large for jobs.out."
                    )
                if timed_out:
                    logger.warning(
                        "Job %s finished its main run, but the reflect run "
                        "exceeded the job deadline; returning the main result.",
                        job.job_id,
                    )
                span.set_attribute("goose.steps", steps)
                span.set_attribute("goose.exit_code", int(exit_code or 0))
                self._collect_outputs(workdir, workspace)
                self._audit_reflect(workdir, audit, reflect_reserve)
                if audit:
                    audit.emit(
                        "goose.completed",
                        "succeeded",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={
                            "container_id": container.id,
                            "usage": usage,
                            "steps": steps,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )
                set_content(span, "output.value", output)
                return AgentResult(text=output, steps=steps, usage=usage)
            except Exception as exc:
                set_error(span, exc)
                if audit:
                    audit.emit(
                        "goose.completed",
                        "failed",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={
                            "container_id": container.id if container else None
                        },
                        error=str(exc),
                    )
                raise
            finally:
                if collector is not None:
                    collector.stop()
                with self._active_lock:
                    if self._active_container is container:
                        self._active_container = None
                self._remove_container(container)
                self._remove_workdir(workdir)

    def _sandbox_for(self, job: Job) -> SandboxSpec:
        """Resolve the environment (env, extensions, instructions) for a job.

        The container image is not resolved here: the Job message carries the
        OCI reference to run, so the worker uses exactly what the submitter
        selected. This lookup only supplies the optional environment settings
        from the registry. An ad-hoc image reference without a registry entry is
        allowed and runs with an empty environment.
        """
        sandbox = self.sandboxes.get(job.sandbox) if self.sandboxes else None
        if sandbox is None and self.sandboxes is not None:
            # The worker can receive a job before its registry consumer has
            # caught up with a newly added environment.
            sandbox = self.sandboxes.wait_for(job.sandbox)
        if sandbox is None:
            sandbox = builtin_sandbox(job.sandbox, self.settings)
        if sandbox is None and job.sandbox_image:
            return SandboxSpec(
                sandbox_id=job.sandbox,
                name=job.sandbox,
                image=job.sandbox_image,
            )
        if sandbox is None:
            raise GooseJobRuntimeError(
                f"Unknown sandbox {job.sandbox!r}. Add it in the GUI sandbox "
                "registry or set sandbox_image in the job."
            )
        if not sandbox.enabled:
            raise GooseJobRuntimeError(
                f"Sandbox {job.sandbox!r} is disabled. Choose another environment."
            )
        return sandbox

    def _ensure_image(self, image: str | None = None) -> None:
        image = image or self.settings.goose_image
        try:
            self.client.images.get(image)
            return
        except Exception:
            pass
        logger.info("Pulling %s into the Podman storage", image)
        try:
            self.client.images.pull(image)
        except Exception as exc:
            raise GooseJobRuntimeError(
                f"goose image {image} is not available in the Podman storage "
                f"and could not be pulled: {exc}. Run the matching build "
                "script on the host if this is a local sandbox image."
            ) from exc
        try:
            self.client.images.get(image)
        except Exception as exc:
            raise GooseJobRuntimeError(
                f"goose image {image} is not available in the Podman storage."
            ) from exc

    def _ensure_extension_dirs(self) -> None:
        for directory in (
            Path(self.settings.goose_job_skills_dir),
            Path(self.settings.goose_job_tools_dir) / "bin",
            Path(self.settings.goose_job_config_dir),
        ):
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise GooseJobRuntimeError(
                    f"Could not create sandbox directory {directory}: {exc}"
                ) from exc

    def _ensure_job_network(self) -> None:
        try:
            self.client.networks.get(self.settings.goose_job_network)
            return
        except Exception as exc:
            if not self._is_not_found(exc):
                raise
        if not self.settings.goose_job_network_autocreate:
            raise GooseJobRuntimeError(
                f"Podman network {self.settings.goose_job_network!r} does not "
                "exist. Run scripts/init-job-network.sh on the host before "
                "starting the worker."
            )
        directory = (
            Path(self.settings.goose_job_network_dir)
            if self.settings.goose_job_network_dir.strip()
            else None
        )
        path = ensure_job_network(
            name=self.settings.goose_job_network,
            subnet=self.settings.goose_job_network_subnet,
            interface=self.settings.goose_job_network_interface,
            directory=directory,
        )
        logger.debug("Job network definition: %s", path)

    def _ensure_proxy(self) -> str:
        """Return the proxy IPv6 address on the isolated job network.

        The proxy is the only peer on the job network. It is attached to the
        ordinary Podman network first so its default route is IPv4, then to the
        internal IPv6 job network. Jobs resolve ``host.containers.internal`` to
        this address and can reach nothing else.
        """
        with self._proxy_lock:
            if self._proxy_ipv6:
                return self._proxy_ipv6
            self._ensure_job_network()
            container: Any | None = None
            try:
                container = self.client.containers.get(_PROXY_NAME)
            except Exception as exc:
                if not self._is_not_found(exc):
                    raise
            if container is None:
                container = self._create_proxy()
            container.reload()
            if not self._container_has_network(container):
                self._connect_proxy(container)
                container.reload()
            if container.status != "running":
                container.start()
                time.sleep(1.0)
            ipv6 = self._wait_proxy_ipv6(container)
            self._proxy_ipv6 = ipv6
            logger.info(
                "Job gateway proxy is ready on [%s]:%s",
                ipv6,
                self.settings.llm_gateway_port,
            )
            return ipv6

    def _create_proxy(self) -> Any:
        body = {
            "Image": self.settings.goose_image,
            "Cmd": ["/usr/bin/python3", _PROXY_SCRIPT],
            "Env": [
                f"LISTEN_PORT={self.settings.llm_gateway_port}",
                f"UPSTREAM_HOST={self.settings.goose_job_proxy_upstream_host}",
                f"UPSTREAM_PORT={self.settings.goose_job_proxy_upstream_port}",
                "PYTHONDONTWRITEBYTECODE=1",
            ],
            "Labels": {_PROXY_LABEL: "1"},
            "WorkingDir": "/",
            "HostConfig": {
                # Create on the normal Podman network first so the default
                # route is IPv4. The internal network is connected afterwards.
                "NetworkMode": self.settings.goose_job_proxy_network,
                "Tmpfs": {"/tmp": "size=16m,mode=1777"},
                "ReadonlyRootfs": True,
                "SecurityOpt": ["no-new-privileges:true", "label=disable"],
                "CapDrop": ["ALL"],
            },
        }
        api = self.client.api
        response = api._post(
            api._url("/containers/create"),
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
            params={"name": _PROXY_NAME},
        )
        if response.status_code >= 300:
            raise GooseJobRuntimeError(
                "Could not create the job gateway proxy: "
                f"{response.status_code} {response.text[:1000]}"
            )
        return self.client.containers.get(response.json()["Id"])

    def _connect_proxy(self, container: Any) -> None:
        network = self.client.networks.get(self.settings.goose_job_network)
        try:
            network.connect(container)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
        container.reload()

    def _container_networks(self, container: Any) -> dict[str, Any]:
        attrs = getattr(container, "attrs", None) or {}
        networks = (attrs.get("NetworkSettings") or {}).get("Networks") or {}
        return networks if isinstance(networks, dict) else {}

    def _container_has_network(self, container: Any) -> bool:
        return self.settings.goose_job_network in self._container_networks(container)

    def _container_ipv6(self, container: Any) -> str:
        endpoint = (
            self._container_networks(container).get(self.settings.goose_job_network)
            or {}
        )
        value = endpoint.get("GlobalIPv6Address")
        return value if isinstance(value, str) else ""

    def _wait_proxy_ipv6(self, container: Any) -> str:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            container.reload()
            ipv6 = self._container_ipv6(container)
            if ipv6:
                return ipv6
            time.sleep(0.2)
        raise GooseJobRuntimeError(
            "The job gateway proxy did not get an IPv6 address on "
            f"{self.settings.goose_job_network}."
        )

    def _cleanup_orphans(self) -> None:
        max_age = self.settings.max_job_timeout_seconds + 300
        now = time.time()
        try:
            containers = self.client.containers.list(
                all=True, filters={"label": f"{_JOB_LABEL}=1"}
            )
        except Exception:
            logger.warning("Could not list orphaned goose microVM containers")
            containers = []
        for container in containers:
            labels = getattr(container, "labels", None) or {}
            created = self._label_age(labels, now)
            status = getattr(container, "status", "")
            if status == "running" and created is not None and created < max_age:
                continue
            logger.warning(
                "Removing orphaned goose microVM container %s",
                container.id[:12],
            )
            try:
                if status == "running":
                    container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                logger.warning(
                    "Could not remove orphaned goose microVM container %s",
                    container.id[:12],
                )
        for workdir in self._orphan_workdirs():
            logger.warning("Removing orphaned goose job directory %s", workdir)
            shutil.rmtree(workdir, ignore_errors=True)

    def _orphan_workdirs(self) -> list[Path]:
        root = Path(self.settings.goose_job_workspace_root)
        if not root.is_dir():
            return []
        orphans: list[Path] = []
        for entry in root.iterdir():
            if not entry.is_dir() or not entry.name.startswith("goose-vm-"):
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
        return Path(self.settings.goose_job_workspace_root) / f"goose-vm-{job.run_id}"

    def _prepare_workdir(
        self,
        workdir: Path,
        workspace: Path,
        prompt: str,
        job: Job,
    ) -> None:
        shutil.rmtree(workdir, ignore_errors=True)
        (workdir / "inputs").mkdir(parents=True, exist_ok=True)
        (workdir / "outputs").mkdir(parents=True, exist_ok=True)
        (workdir / "bundle").mkdir(parents=True, exist_ok=True)
        (workdir / ".kafkallm").mkdir(parents=True, exist_ok=True)
        (workdir / ".claude").mkdir(parents=True, exist_ok=True)
        agents_dir = workdir / ".claude" / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)
        if job.multi_agent:
            for agent in job.agents:
                # The orchestrator is the main session, not a delegate.
                if agent.role == "orchestrator":
                    continue
                (agents_dir / f"{agent.agent_id}.md").write_text(
                    _agent_source(agent), encoding="utf-8"
                )
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
        (bundle / "run-job.sh").write_text(_VM_RUN_SCRIPT, encoding="utf-8")
        (bundle / "main-prompt.txt").write_text(prompt, encoding="utf-8")
        (bundle / "reflect-prompt.txt").write_text(_REFLECT_PROMPT, encoding="utf-8")
        (bundle / "agents.json").write_text(
            json.dumps(
                [agent.model_dump(mode="json") for agent in job.agents]
                if job.multi_agent
                else [],
                indent=2,
            ),
            encoding="utf-8",
        )
        os.chmod(bundle / "run-job.sh", 0o755)
        skills_dir = Path(self.settings.goose_job_skills_dir)
        try:
            skills_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning("Could not create skills directory %s", skills_dir)
        # Rootless Podman maps the job uid into a subuid range, so the worker's
        # files are not owned by the job. Make the shared directories writable
        # so the microVM can write its results and the next job can read them.
        for directory in (
            workdir,
            workdir / ".kafkallm",
            workdir / ".claude",
            agents_dir,
            workdir / "inputs",
            workdir / "outputs",
            bundle,
        ):
            os.chmod(directory, 0o777)
        for agent_file in agents_dir.glob("*.md"):
            os.chmod(agent_file, 0o644)

    def _create_container(
        self,
        image: str,
        environment: dict[str, str],
        labels: dict[str, str],
        workdir: Path,
        proxy_ipv6: str,
    ) -> Any:
        annotations = {
            "run.oci.handler": "krun",
            "krun.cpus": str(self.settings.goose_job_cpus),
            "krun.ram_mib": str(self.settings.goose_job_ram_mib),
        }
        if self.settings.goose_job_use_passt:
            annotations["krun.use_passt"] = "1"
        body = {
            "Image": image,
            "Cmd": ["/bin/sh", "/jobboot/run-job.sh"],
            "Env": [f"{key}={value}" for key, value in environment.items()],
            "Labels": labels,
            "WorkingDir": "/workspace",
            "HostConfig": {
                "Binds": [
                    f"{workdir}:/workspace:rw,z",
                    f"{workdir}/bundle:/jobboot:ro,z",
                    (
                        f"{self.settings.goose_job_skills_dir}:"
                        "/workspace/.claude/skills:rw,z"
                    ),
                    f"{self.settings.goose_job_tools_dir}:/opt/kafkallm/tools:ro,z",
                    f"{self.settings.goose_job_config_dir}:/opt/kafkallm/config:ro,z",
                ],
                "Tmpfs": {
                    "/opt/data": (
                        f"size={self.settings.goose_job_tmpfs_size},mode=0700"
                    ),
                    "/tmp": "size=256m,mode=1777",
                },
                "Annotations": annotations,
                "NetworkMode": self.settings.goose_job_network,
                "ExtraHosts": [f"host.containers.internal:{proxy_ipv6}"],
                "Memory": self._memory_bytes(),
                "MemorySwap": self._memory_bytes(),
                "PidsLimit": self.settings.goose_job_pids_limit,
                "NanoCpus": self.settings.goose_job_cpus * 1_000_000_000,
                "SecurityOpt": ["no-new-privileges:true", "label=disable"],
            },
        }
        api = self.client.api
        response = api._post(
            api._url("/containers/create"),
            data=json.dumps(body),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code >= 300:
            raise GooseJobRuntimeError(
                "Could not create the goose microVM container: "
                f"{response.status_code} {response.text[:1000]}"
            )
        container_id = response.json()["Id"]
        return self.client.containers.get(container_id)

    def _memory_bytes(self) -> int:
        limit = self.settings.goose_job_memory_limit.strip()
        if not limit or limit == "0":
            return 0
        return _parse_bytes(limit)

    def _wait(
        self,
        container: Any,
        deadline: float,
        heartbeat: Callable[[], bool] | None = None,
    ) -> tuple[int | None, bool]:
        while time.monotonic() < deadline:
            if heartbeat is not None and heartbeat():
                logger.warning(
                    "Job cancellation requested; stopping microVM container %s",
                    container.id[:12],
                )
                try:
                    container.stop(timeout=10)
                except Exception:
                    logger.warning(
                        "Could not stop cancelled microVM container %s",
                        container.id[:12],
                    )
                raise JobCancelledError("Job cancelled by user.")
            container.reload()
            if container.status not in {"created", "running", "restarting"}:
                wait_result = container.wait(timeout=10)
                return int(wait_result.get("StatusCode", 1)), False
            time.sleep(_CONTAINER_POLL_SECONDS)
        logger.warning(
            "goose microVM container %s exceeded the job deadline; stopping it",
            container.id[:12],
        )
        try:
            container.stop(timeout=10)
        except Exception:
            logger.warning(
                "Could not stop timed-out goose microVM container %s",
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

    def _read_text(self, workdir: Path, name: str) -> str:
        path = workdir / ".kafkallm" / name
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
                "goose.reflect",
                attributes={"status": "skipped", "reason": "no time budget"},
            )
            return
        raw = self._read_text(workdir, "reflect.jsonl")
        if not raw.strip():
            raw = self._read_text(workdir, "reflect.json")
        exit_raw = self._read_text(workdir, "reflect.exit").strip()
        if not raw.strip():
            stderr = self._read_text(workdir, "reflect.err").strip()
            if exit_raw == "124":
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "timeout"},
                    error="The reflect run exceeded its time budget.",
                )
            elif stderr:
                audit.emit(
                    "goose.reflect",
                    "failed",
                    attributes={"status": "failed"},
                    error=stderr[:4000],
                )
            else:
                audit.emit(
                    "goose.reflect",
                    attributes={"status": "skipped"},
                )
            return
        text, usage, steps, error = parse_goose_output(raw)
        if error or (exit_raw and exit_raw != "0"):
            audit.emit(
                "goose.reflect",
                "failed",
                attributes={"status": "failed", "usage": usage, "steps": steps},
                error=(error or text or f"Reflect exited with code {exit_raw}.")[:4000],
            )
            return
        audit.emit(
            "goose.reflect",
            "succeeded",
            attributes={"status": "succeeded", "usage": usage, "steps": steps},
        )

    def _reflect_reserve(self, remaining: float, enabled: bool | None = None) -> int:
        if enabled is None:
            enabled = self.settings.goose_job_reflect_enabled
        if not enabled:
            return 0
        if remaining < _REFLECT_MIN_REMAINING_SECONDS:
            return 0
        return max(
            1,
            min(
                self.settings.goose_job_reflect_timeout_seconds,
                int(remaining * 0.25),
            ),
        )

    def _environment(
        self,
        job_id: str,
        budget: int,
        main_budget: int,
        reflect_reserve: int,
        sandbox: SandboxSpec,
        multi_agent: bool,
        agent_count: int,
        unlimited: bool,
    ) -> dict[str, str]:
        if not self.settings.gateway_configured:
            raise GooseJobRuntimeError(
                "The LLM gateway is not configured. Set LLM_GATEWAY_URL and "
                "LLM_GATEWAY_VIRTUAL_KEY for the worker."
            )
        gateway_url = self.settings.llm_gateway_url.rstrip("/")
        gateway_url = gateway_url.removesuffix("/v1")
        environment = {
            "HOME": "/opt/data",
            "PATH": (
                "/opt/kafkallm/tools/bin:/opt/java/openjdk/bin:"
                "/opt/ghidra/support:/usr/local/goose/bin:"
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ),
            "KAFKALLM_TOOLS_DIR": "/opt/kafkallm/tools",
            "KAFKALLM_CONFIG_DIR": "/opt/kafkallm/config",
            "KAFKALLM_AGENTS_PATH": "/jobboot/agents.json",
            "KAFKALLM_SANDBOX": sandbox.sandbox_id,
            "KAFKALLM_MULTI_AGENT": "1" if multi_agent else "0",
            "KAFKALLM_AGENT_COUNT": str(agent_count),
            "KAFKALLM_JOB_UNLIMITED": "1" if unlimited else "0",
            "GOOSE_PATH_ROOT": "/opt/data/goose",
            "GOOSE_MODE": "auto",
            "GOOSE_DISABLE_KEYRING": "1",
            "GOOSE_DISABLE_SESSION_NAMING": "true",
            "GOOSE_TELEMETRY_ENABLED": "false",
            "GOOSE_AUTO_COMPACT_THRESHOLD": "0.9",
            "GOOSE_PROVIDER": "openai",
            "OPENAI_HOST": gateway_url,
            # The gateway replaces this virtual key with the provider key.
            "OPENAI_API_KEY": self.settings.llm_gateway_virtual_key,
            "GOOSE_MAX_TURNS": str(
                self.settings.unlimited_max_turns
                if unlimited
                else self.settings.goose_job_max_turns
            ),
            "GOOSE_SUBAGENT_MAX_TURNS": str(
                self.settings.unlimited_max_subagent_turns
                if unlimited
                else self.settings.goose_job_subagent_max_turns
            ),
            "GOOSE_MAX_CONCURRENT_SUBAGENTS": str(
                max(1, min(5, agent_count)) if agent_count else 5
            ),
            "GOOSE_REFLECT_MAX_TURNS": "20",
            "KAFKALLM_JOB_BUDGET_SECONDS": str(max(0, budget)),
            "KAFKALLM_JOB_MAIN_TIMEOUT": "0" if unlimited else str(main_budget + 10),
            "KAFKALLM_JOB_REFLECT_ENABLED": "1" if reflect_reserve else "0",
            "KAFKALLM_JOB_REFLECT_TIMEOUT": str(
                self.settings.goose_job_reflect_timeout_seconds
            ),
            "NO_PROXY": "127.0.0.1,localhost,host.containers.internal",
        }
        if sandbox.extensions:
            environment["KAFKALLM_SANDBOX_EXTENSIONS"] = ",".join(sandbox.extensions)
        # SandboxSpec rejects reserved key names and prefixes, so custom values
        # cannot override the gateway credentials or the runtime controls.
        environment.update(sandbox.env)
        if self.settings.goose_model:
            environment["GOOSE_MODEL"] = self.settings.goose_model
        return environment

    def _build_prompt(
        self,
        job: Job,
        sandbox: SandboxSpec | None = None,
    ) -> str:
        if sandbox is None:
            try:
                sandbox = self._sandbox_for(job)
            except GooseJobRuntimeError:
                sandbox = None
        if job.multi_agent:
            role = (
                "You are the goosetown orchestrator for one isolated Kafkallm "
                "job. Follow the goosetown-orchestrator skill.\n"
            )
            delegates = [agent for agent in job.agents if agent.role != "orchestrator"]
            if delegates:
                flock = "\n".join(
                    f"- {agent.name} ({agent.role}): {agent.description}"
                    for agent in delegates
                )
                mode = (
                    "Goosetown multi-agent mode is ON. The summon extension "
                    "provides the `delegate` and `load` tools.\n"
                    "The selected flock is installed as agent sources in "
                    "/workspace/.claude/agents. Call `load()` once to list "
                    "them.\n"
                    f"Selected flock:\n{flock}\n"
                    "For each independent piece, call "
                    '`delegate(source: "<exact role name>", '
                    'instructions: "...")`. Give the delegate the gtwall '
                    "preamble, a unique gtwall ID, exact files it owns, and "
                    "acceptance criteria. Use `load(source: ...)` only to read "
                    "a role into your own context. You can also use "
                    "`delegate(instructions: ...)` for ad-hoc work.\n"
                    "Read /jobboot/agents.json for the complete selected "
                    "flock metadata. Delegate nontrivial artifacts and use a "
                    "second role to review them; only do truly trivial "
                    "single-step work yourself.\n"
                )
            else:
                mode = (
                    "Goosetown multi-agent mode is ON. The summon extension "
                    "provides the `delegate` and `load` tools. No flock roles "
                    "were selected, so use `delegate(instructions: ...)` with "
                    "the built-in goosetown role skills (researcher-local, "
                    "worker, writer, reviewer) and load those skills in the "
                    "delegate task.\n"
                )
        else:
            role = "You are the goose agent for one isolated Kafkallm job.\n"
            mode = (
                "Goosetown multi-agent mode is OFF. Do the job yourself; do "
                "not delegate or spawn subagents.\n"
            )
        if sandbox is not None and sandbox.instructions.strip():
            sandbox_note = f"{sandbox.instructions.strip()}\n"
        else:
            sandbox_note = ""
        return (
            role
            + mode
            + sandbox_note
            + "Decompose the job when useful, coordinate through gtwall, and "
            "synthesize the final result.\n"
            "Workspace root: /workspace\n"
            "Input files: /workspace/inputs\n"
            "Write all generated result files to: /workspace/outputs\n"
            "Do not modify files outside /workspace except the persistent "
            "skills in /workspace/.claude/skills.\n"
            "There is no web browsing. Your only network endpoint is the model "
            "gateway used by goose; do not try to reach any other host or "
            "internal service. Work only from the provided instruction and "
            "input files.\n"
            "Work autonomously; do not ask for approval or clarification.\n"
            "Return the completed result as your final response.\n\n"
            f"Instruction:\n{job.instruction}\n\n"
            "Structured job input:\n"
            f"{job.input}"
        )

    def _remove_container(self, container: Any | None) -> None:
        if container is None:
            return
        try:
            container.remove(force=True)
        except Exception as exc:
            if not self._is_not_found(exc):
                logger.warning(
                    "Could not remove goose microVM container %s: %s",
                    container.id[:12],
                    exc,
                )

    def _remove_workdir(self, workdir: Path) -> None:
        for attempt in range(10):
            if not workdir.exists():
                return
            shutil.rmtree(workdir, ignore_errors=True)
            if not workdir.exists():
                return
            time.sleep(0.2 * (attempt + 1))
        logger.warning("Could not remove goose job directory %s", workdir)

    @staticmethod
    def _is_not_found(exc: Exception) -> bool:
        return getattr(exc, "status_code", None) == 404 or exc.__class__.__name__ in {
            "NotFound",
            "ImageNotFound",
        }


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
