from __future__ import annotations

import json
import logging
import os
import shutil
import threading
import time
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

from gaensestall.core.models import AgentResult, Job, SandboxSpec
from gaensestall.core.settings import Settings
from gaensestall.core.telemetry import get_tracer, set_content, set_error
from gaensestall.sandbox.activity import _LogCollector, _SpanActivity
from gaensestall.sandbox.archive import (
    copy_from_container,
    copy_into_container,
    read_container_file,
)
from gaensestall.sandbox.errors import (
    GooseJobRuntimeError,
    JobCancelledError,
)
from gaensestall.sandbox.errors import WorkerShutdown as WorkerShutdown
from gaensestall.sandbox.goose_output import NO_OUTPUT_ERROR, parse_goose_run
from gaensestall.sandbox.guest_scripts import _REFLECT_PROMPT as _REFLECT_PROMPT
from gaensestall.sandbox.guest_scripts import _REFLECT_RUN_SCRIPT as _REFLECT_RUN_SCRIPT
from gaensestall.sandbox.guest_scripts import _VM_RUN_SCRIPT
from gaensestall.sandbox.network import ensure_job_network
from gaensestall.sandbox.podman import (
    container_networks,
    create_container,
    is_not_found,
    parse_size,
    remove_container,
    wait_for_ipv6,
)
from gaensestall.sandbox.prompts import (
    agent_source as _agent_source,
)
from gaensestall.sandbox.prompts import (
    build_prompt,
    main_environment,
    reflect_environment,
)
from gaensestall.sandbox.reflect import run_reflect
from gaensestall.sandbox.registry import SandboxStore, builtin_sandbox
from gaensestall.sandbox.safe_fs import iter_regular_files, read_text_no_follow
from gaensestall.worker.audit import JobAudit
from gaensestall.worker.retry import RetryTimeout, retry_until

logger = logging.getLogger(__name__)

_JOB_LABEL = "gaensestall.vm"
_JOB_ID_LABEL = "gaensestall.job_id"
_RUN_ID_LABEL = "gaensestall.run_id"
_ROLE_LABEL = "gaensestall.role"
_CREATED_AT_LABEL = "gaensestall.created_at"
_WORKDIR_LABEL = "gaensestall.workdir"

_PROXY_NAME = "gaensestall-llm-proxy"
_PROXY_LABEL = "gaensestall.llm_proxy"
_JOB_PROXY_LABEL = "gaensestall.job_proxy"
_PROXY_SCRIPT = "/app/llm_proxy.py"

_REFLECT_MIN_REMAINING_SECONDS = 60
_SCHEDULE_RESERVE_SECONDS = 25
_CONTAINER_POLL_SECONDS = 1.0
_MIRROR_MAX_BYTES = 16 * 1024 * 1024

# Everything the job needs is copied into the container's writable layer before
# it starts. Nothing from the host is bind mounted into the microVM.
#
# The persistent skill store is copied to /opt/gaensestall/skills-persistent.
# The job gets a disposable copy at /workspace/.claude/skills; the main microVM
# can never write the shared store. The learning pass runs in a separate
# microVM that starts after this one has exited.


class _SandboxMirror:
    """Append container control files to the worker's local job directory.

    krun containers do not support ``exec``, so live activity is mirrored with
    the archive API. Only the small control files are fetched; the final state
    is copied out once the container exits.
    """

    def __init__(
        self,
        container: Any,
        directory: Path,
        names: tuple[str, ...],
    ) -> None:
        self.container = container
        self.directory = directory
        self.names = names
        self.offsets = {name: 0 for name in names}
        directory.mkdir(parents=True, exist_ok=True)
        for name in names:
            (directory / name).touch()

    def sync(self) -> None:
        for name in self.names:
            try:
                data = read_container_file(
                    self.container,
                    f"/workspace/.gaensestall/{name}",
                    max_bytes=_MIRROR_MAX_BYTES,
                )
            except Exception as exc:
                logger.debug("Could not mirror sandbox file %s: %s", name, exc)
                continue
            if not data:
                continue
            offset = self.offsets[name]
            if len(data) <= offset:
                continue
            try:
                with (self.directory / name).open("ab") as handle:
                    handle.write(data[offset:])
            except OSError as exc:
                logger.debug("Could not write mirrored sandbox file %s: %s", name, exc)
                continue
            self.offsets[name] = len(data)


class GooseJobRunner:
    """Run each job in its own KVM microVM through Podman's krun runtime.

    The microVM runs the goosetown wrapper around the goose binary. Podman
    exposes a Docker compatible API, so the lifecycle uses ordinary container
    calls; the runtime is selected through OCI annotations. Job data is copied
    into the container root filesystem because the guest filesystem lives
    inside the microVM.
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
        if not self.settings.llm_proxy_configured:
            raise GooseJobRuntimeError(
                "The LLM gateway is not configured. Set LLM_PROXY_URL and "
                "LLM_PROXY_TOKEN for the worker."
            )
        deadline = time.monotonic() + self.settings.goose_startup_timeout_seconds
        self._wait_for_gateway(deadline)
        try:
            retry_until(
                deadline,
                self._prepare_runtime,
                description="goose microVM runtime",
                logger=logger,
            )
        except RetryTimeout as exc:
            raise GooseJobRuntimeError(
                "goose microVM runtime did not become ready within the startup timeout."
            ) from exc
        logger.info(
            "goose microVM runtime is ready (image=%s, docker_host=%s, network=%s)",
            self.settings.goose_image,
            self.settings.goose_job_docker_host,
            self.settings.goose_job_network,
        )

    def _prepare_runtime(self) -> bool:
        self.client.ping()
        self._ensure_image()
        self._ensure_image(self.settings.llm_proxy_image)
        self._ensure_extension_dirs()
        self._ensure_job_network()
        self._ensure_proxy()
        self._cleanup_orphans()
        return True

    def _wait_for_gateway(self, deadline: float) -> None:
        ready_url = self.settings.mlflow_ready_url.strip()
        if not ready_url:
            return

        def check() -> bool:
            with urllib.request.urlopen(ready_url, timeout=3) as response:
                if not 200 <= response.status < 300:
                    raise GooseJobRuntimeError(
                        f"LLM gateway readiness returned HTTP {response.status}"
                    )
            return True

        try:
            retry_until(
                deadline,
                check,
                description="LLM gateway",
                logger=logger,
            )
        except RetryTimeout as exc:
            raise GooseJobRuntimeError(
                f"LLM gateway at {ready_url} did not become ready within the "
                "startup timeout."
            ) from exc
        logger.info("MLflow gateway is ready at %s", ready_url)

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
        audit: JobAudit | None = None,
        heartbeat: Callable[[], bool] | None = None,
        log: Callable[[str, str], None] | None = None,
    ) -> AgentResult:
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
            main_budget=main_budget,
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
            "llm.proxy": self.settings.llm_proxy_url,
            "job.id": job.job_id,
            "job.run_id": job.run_id,
            "session.id": job.job_id,
        }
        main_container: Any | None = None
        job_proxy: Any | None = None
        collector: _LogCollector | None = None
        mirror: _SandboxMirror | None = None
        run_started = time.perf_counter()

        with get_tracer().start_as_current_span(
            "goose.job.vm.run", attributes=attributes
        ) as span:
            set_content(span, "input.value", prompt)
            try:
                self._ensure_image(image)
                self._prepare_workdir(workdir, workspace, prompt, job)
                self._ensure_extension_dirs()
                span_context = span.get_span_context()
                traceparent = (
                    f"00-{span_context.trace_id:032x}-{span_context.span_id:016x}-01"
                    if span_context.is_valid
                    else ""
                )
                proxy_ipv6 = self._ensure_proxy()
                if traceparent:
                    job_proxy, proxy_ipv6 = self._create_job_proxy(job, traceparent)
                main_container = self._create_container(
                    image=image,
                    environment=environment,
                    labels=labels,
                    workdir=workdir,
                    proxy_ipv6=proxy_ipv6,
                )
                self._set_active(main_container)
                main_container.start()
                mirror = _SandboxMirror(
                    main_container,
                    workdir / ".gaensestall",
                    ("main.jsonl", "main.err"),
                )
                span.set_attribute("goose.container_id", main_container.id)
                if log is not None:
                    log(
                        "system",
                        f"container {main_container.id[:12]} started "
                        f"(image={image}, sandbox={job.sandbox})",
                    )
                    activity = _SpanActivity(span)

                    def publish_activity(kind: str, text: str) -> None:
                        log(kind, text)
                        activity(kind, text)
                        if audit is not None and kind in {
                            "agent.delegated",
                            "agent.returned",
                        }:
                            audit.emit(kind, attributes={"detail": text[:4000]})

                    collector = _LogCollector(workdir, publish_activity)
                    collector.start()
                if audit:
                    audit.emit(
                        "goose.started",
                        attributes={
                            "container_id": main_container.id,
                            "workspace": str(workdir),
                            "skills_dir": self.settings.goose_job_skills_dir,
                            "skills_mode": "per-job-copy",
                            "image": image,
                            "sandbox": job.sandbox,
                            "model": self.settings.goose_model or None,
                            "provider": "openai",
                            "upstream_provider": (
                                self.settings.goose_provider or "gateway"
                            ),
                            "proxy": self.settings.llm_proxy_url,
                            "reflect_enabled": bool(reflect_reserve),
                        },
                    )

                exit_code, timed_out = self._wait(
                    main_container, deadline, heartbeat=heartbeat, mirror=mirror
                )
                if mirror is not None:
                    mirror.sync()
                if collector is not None:
                    collector.stop()
                    collector = None
                copy_from_container(
                    main_container,
                    "/workspace",
                    workdir,
                    include={".gaensestall", "outputs"},
                )
                main_error = self._read_text(workdir, "main.err")
                output, usage, steps, event_error = parse_goose_run(workdir, "main")
                if event_error == NO_OUTPUT_ERROR:
                    if timed_out:
                        raise GooseJobRuntimeError(
                            "goose job exceeded the job deadline before "
                            "producing a result."
                        )
                    detail = main_error.strip() or self._log_tail(main_container)
                    raise GooseJobRuntimeError(
                        "goose microVM produced no result "
                        f"(exit code {exit_code}). {detail[:4000]}"
                    )
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
                    and len(output.encode("utf-8")) > self.settings.max_result_bytes
                ):
                    raise GooseJobRuntimeError(
                        "goose response is too large for jobs.out."
                    )
                if timed_out:
                    logger.warning(
                        "Job %s finished its main run, but the job deadline "
                        "was reached; returning the main result.",
                        job.job_id,
                    )
                span.set_attribute("goose.steps", steps)
                span.set_attribute("goose.exit_code", int(exit_code or 0))
                self._collect_outputs(workdir, workspace)
                if reflect_reserve and not timed_out:
                    # Remove the main microVM before creating the reflect
                    # workspace. A lingering guest process must not be able to
                    # observe or write the tree that the worker later merges
                    # into the skill store. If removal fails, skip the pass
                    # instead of trusting a VM that might still run.
                    if not self._remove_container(main_container):
                        if audit:
                            audit.emit(
                                "goose.reflect",
                                "failed",
                                error=(
                                    "The main microVM could not be removed, so "
                                    "the learning pass was skipped."
                                ),
                            )
                    else:
                        run_reflect(
                            self,
                            job=job,
                            image=image,
                            workdir=workdir,
                            outputs=workspace / "outputs",
                            deadline=deadline,
                            reflect_reserve=reflect_reserve,
                            labels=labels,
                            proxy_ipv6=proxy_ipv6,
                            audit=audit,
                            heartbeat=heartbeat,
                        )
                else:
                    if audit:
                        audit.emit(
                            "goose.reflect",
                            attributes={
                                "status": "skipped",
                                "reason": (
                                    "no time budget"
                                    if not reflect_reserve
                                    else "deadline reached"
                                ),
                            },
                        )
                if audit:
                    audit.emit(
                        "goose.completed",
                        "succeeded",
                        duration_ms=round(
                            (time.perf_counter() - run_started) * 1000, 3
                        ),
                        attributes={
                            "container_id": main_container.id,
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
                            "container_id": (
                                main_container.id if main_container else None
                            )
                        },
                        error=str(exc),
                    )
                raise
            finally:
                if collector is not None:
                    collector.stop()
                with self._active_lock:
                    if self._active_container is main_container:
                        self._active_container = None
                self._remove_container(main_container)
                self._remove_workdir(workdir)
                if job_proxy is not None:
                    self._remove_container(job_proxy)

    def _set_active(self, container: Any) -> None:
        with self._active_lock:
            self._active_container = container

    def _sandbox_mirror(
        self,
        container: Any,
        directory: Path,
        names: tuple[str, ...],
    ) -> _SandboxMirror:
        return _SandboxMirror(container, directory, names)

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
                f"image {image} is not available in the Podman storage "
                f"and could not be pulled: {exc}. Run the matching build "
                "script on the host if this is a local sandbox image."
            ) from exc
        try:
            self.client.images.get(image)
        except Exception as exc:
            raise GooseJobRuntimeError(
                f"image {image} is not available in the Podman storage."
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
            if container is not None and not self._proxy_image_current(container):
                logger.info(
                    "Recreating job LLM proxy: image %s changed",
                    self.settings.llm_proxy_image,
                )
                self._remove_container(container)
                container = None
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
                "Job LLM proxy is ready on [%s]:%s",
                ipv6,
                self.settings.llm_proxy_port,
            )
            return ipv6

    def _proxy_image_current(self, container: Any) -> bool:
        """Return True when the proxy runs the current proxy image.

        Reusing a container created from an older image would keep an outdated
        allowlist (or other proxy fixes) alive across worker restarts.
        """
        try:
            expected = self.client.images.get(self.settings.llm_proxy_image)
        except Exception:
            return False
        attrs = getattr(container, "attrs", None) or {}
        actual = attrs.get("Image")
        expected_id = getattr(expected, "id", None)
        return bool(actual) and bool(expected_id) and actual == expected_id

    def _create_proxy(
        self,
        *,
        name: str = _PROXY_NAME,
        extra_env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
    ) -> Any:
        environment = [
            f"LISTEN_PORT={self.settings.llm_proxy_port}",
            f"UPSTREAM_HOST={self.settings.llm_proxy_upstream_host}",
            f"UPSTREAM_PORT={self.settings.llm_proxy_upstream_port}",
            f"GAENSESTALL_LLM_PROXY_TOKEN={self.settings.llm_proxy_token}",
            "PYTHONDONTWRITEBYTECODE=1",
        ]
        if extra_env:
            environment.extend(f"{key}={value}" for key, value in extra_env.items())
        body = {
            "Image": self.settings.llm_proxy_image,
            "Entrypoint": ["/usr/bin/python3"],
            "Cmd": [_PROXY_SCRIPT],
            "Env": environment,
            "Labels": {_PROXY_LABEL: "1", **(labels or {})},
            "WorkingDir": "/",
            "HostConfig": {
                # Create on the normal Podman network first so the default
                # route is IPv4. The internal network is connected afterwards.
                "NetworkMode": self.settings.llm_proxy_network,
                "Tmpfs": {"/tmp": "size=16m,mode=1777"},
                "ReadonlyRootfs": True,
                "SecurityOpt": ["no-new-privileges:true", "label=disable"],
                "CapDrop": ["ALL"],
            },
        }
        return create_container(self.client, body, name=name)

    def _create_job_proxy(self, job: Job, traceparent: str) -> tuple[Any, str]:
        """Create a per-job LLM proxy that injects the job trace context."""
        container = self._create_proxy(
            name=f"{_PROXY_NAME}-{job.run_id[:8]}",
            extra_env={
                "GAENSESTALL_TRACEPARENT": traceparent,
                "GAENSESTALL_JOB_ID": job.job_id,
            },
            labels={
                _JOB_PROXY_LABEL: "1",
                _JOB_ID_LABEL: job.job_id,
                _RUN_ID_LABEL: job.run_id,
                _CREATED_AT_LABEL: str(int(time.time())),
            },
        )
        try:
            self._connect_proxy(container)
            container.reload()
            if container.status != "running":
                container.start()
                time.sleep(0.5)
            ipv6 = self._wait_proxy_ipv6(container)
        except Exception:
            self._remove_container(container)
            raise
        return container, ipv6

    def _connect_proxy(self, container: Any) -> None:
        network = self.client.networks.get(self.settings.goose_job_network)
        try:
            network.connect(container)
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise
        container.reload()

    def _container_networks(self, container: Any) -> dict[str, Any]:
        return container_networks(container)

    def _container_has_network(self, container: Any) -> bool:
        return self.settings.goose_job_network in self._container_networks(container)

    def _wait_proxy_ipv6(self, container: Any) -> str:
        return wait_for_ipv6(container, self.settings.goose_job_network)

    def _remove_stale_containers(
        self,
        *,
        label: str,
        description: str,
        max_age: float,
        now: float,
    ) -> None:
        try:
            containers = self.client.containers.list(
                all=True, filters={"label": f"{label}=1"}
            )
        except Exception:
            logger.warning("Could not list orphaned %s containers", description)
            return
        for container in containers:
            labels = getattr(container, "labels", None) or {}
            created = self._label_age(labels, now)
            status = getattr(container, "status", "")
            if status == "running" and created is not None and created < max_age:
                continue
            logger.warning(
                "Removing orphaned %s container %s", description, container.id[:12]
            )
            try:
                if status == "running":
                    container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                logger.warning(
                    "Could not remove orphaned %s container %s",
                    description,
                    container.id[:12],
                )

    def _cleanup_orphans(self) -> None:
        max_age = self.settings.max_job_timeout_seconds + 300
        now = time.time()
        self._remove_stale_containers(
            label=_JOB_LABEL,
            description="goose microVM",
            max_age=max_age,
            now=now,
        )
        for workdir in self._orphan_workdirs():
            logger.warning("Removing orphaned goose job directory %s", workdir)
            shutil.rmtree(workdir, ignore_errors=True)
            if workdir.exists():
                logger.warning(
                    "Could not remove orphaned goose job directory %s", workdir
                )
        self._remove_stale_containers(
            label=_JOB_PROXY_LABEL,
            description="job proxy",
            max_age=max_age,
            now=now,
        )

    def _orphan_workdirs(self) -> list[Path]:
        root = Path(self.settings.goose_job_workspace_root)
        if not root.is_dir():
            return []
        orphans: list[Path] = []
        for entry in root.iterdir():
            if not entry.is_dir() or not (
                entry.name.startswith("goose-vm-") or entry.name.startswith(".reflect-")
            ):
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
        (workdir / ".gaensestall").mkdir(parents=True, exist_ok=True)
        (workdir / ".claude").mkdir(parents=True, exist_ok=True)
        skills_dir = workdir / ".claude" / "skills"
        skills_dir.mkdir(parents=True, exist_ok=True)
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
        for entry, relative in iter_regular_files(inputs):
            target = workdir / "inputs" / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, target)
        bundle = workdir / "bundle"
        (bundle / "run-job.sh").write_text(_VM_RUN_SCRIPT, encoding="utf-8")
        (bundle / "main-prompt.txt").write_text(prompt, encoding="utf-8")
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
        persistent_skills = Path(self.settings.goose_job_skills_dir)
        try:
            persistent_skills.mkdir(parents=True, exist_ok=True)
        except OSError:
            logger.warning("Could not create skills directory %s", persistent_skills)

    def _create_container(
        self,
        image: str,
        environment: dict[str, str],
        labels: dict[str, str],
        workdir: Path,
        proxy_ipv6: str,
    ) -> Any:
        container = self._create_microvm(
            image=image,
            environment=environment,
            labels={**labels, _ROLE_LABEL: "main"},
            cmd=["/bin/sh", "/jobboot/run-job.sh"],
            proxy_ipv6=proxy_ipv6,
        )
        try:
            copy_into_container(
                container,
                "/workspace",
                workdir,
                exclude={"bundle"},
            )
            copy_into_container(
                container,
                "/",
                workdir / "bundle",
                prefix="jobboot",
            )
            copy_into_container(
                container,
                "/opt/gaensestall",
                self.settings.goose_job_skills_dir,
                prefix="skills-persistent",
            )
            copy_into_container(
                container,
                "/opt/gaensestall",
                self.settings.goose_job_tools_dir,
                prefix="tools",
            )
            copy_into_container(
                container,
                "/opt/gaensestall",
                self.settings.goose_job_config_dir,
                prefix="config",
            )
        except Exception:
            self._remove_container(container)
            raise
        return container

    def _create_reflect_container(
        self,
        image: str,
        environment: dict[str, str],
        labels: dict[str, str],
        reflect_workdir: Path,
        outputs: Path,
        proxy_ipv6: str,
    ) -> Any:
        if outputs.is_symlink() or not outputs.is_dir():
            raise GooseJobRuntimeError(
                "Collected outputs are not a safe directory for the learning pass."
            )
        container = self._create_microvm(
            image=image,
            environment=environment,
            labels={**labels, _ROLE_LABEL: "reflect"},
            cmd=["/bin/sh", "/jobboot/run-reflect.sh"],
            proxy_ipv6=proxy_ipv6,
        )
        try:
            copy_into_container(
                container,
                "/workspace",
                reflect_workdir,
                exclude={"bundle"},
            )
            copy_into_container(
                container,
                "/workspace",
                outputs,
                prefix="outputs",
            )
            copy_into_container(
                container,
                "/",
                reflect_workdir / "bundle",
                prefix="jobboot",
            )
        except Exception:
            self._remove_container(container)
            raise
        return container

    def _create_microvm(
        self,
        *,
        image: str,
        environment: dict[str, str],
        labels: dict[str, str],
        cmd: list[str],
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
            "Cmd": cmd,
            "Env": [f"{key}={value}" for key, value in environment.items()],
            "Labels": labels,
            "WorkingDir": "/workspace",
            "HostConfig": {
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
        return create_container(self.client, body)

    def _memory_bytes(self) -> int:
        return parse_size(self.settings.goose_job_memory_limit)

    def _wait(
        self,
        container: Any,
        deadline: float,
        heartbeat: Callable[[], bool] | None = None,
        mirror: _SandboxMirror | None = None,
    ) -> tuple[int | None, bool]:
        while time.monotonic() < deadline:
            if mirror is not None:
                mirror.sync()
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
        # Never follow a job-created symlink: it could point at a host file.
        for entry, relative in iter_regular_files(source):
            destination = target / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(entry, destination)

    def _read_text(self, workdir: Path, name: str) -> str:
        return read_text_no_follow(workdir / ".gaensestall" / name)

    def _log_tail(self, container: Any, lines: int = 200) -> str:
        try:
            logs = container.logs(stdout=True, stderr=True, tail=lines)
        except Exception:
            return ""
        if isinstance(logs, bytes):
            return logs.decode("utf-8", errors="replace")
        return str(logs)

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
        main_budget: int,
        sandbox: SandboxSpec,
        multi_agent: bool,
        agent_count: int,
        unlimited: bool,
    ) -> dict[str, str]:
        return main_environment(
            self.settings,
            main_budget=main_budget,
            sandbox=sandbox,
            multi_agent=multi_agent,
            agent_count=agent_count,
            unlimited=unlimited,
        )

    def _reflect_environment(self, reflect_seconds: int) -> dict[str, str]:
        return reflect_environment(self.settings, reflect_seconds)

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
        return build_prompt(job, sandbox)

    def _remove_container(self, container: Any | None) -> bool:
        removed = remove_container(container)
        if not removed and container is not None:
            logger.warning(
                "Could not remove goose microVM container %s", container.id[:12]
            )
        return removed

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
        return is_not_found(exc)
