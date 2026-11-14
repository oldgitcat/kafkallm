from __future__ import annotations

import io
import json
import os
import tarfile
import tempfile
import time
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

from kafkallm.core.models import AgentResult, AgentSpec, Job, SandboxSpec
from kafkallm.core.settings import Settings
from kafkallm.sandbox.output import (
    NO_OUTPUT_ERROR,
    parse_goose_output,
    parse_goose_output_file,
)
from kafkallm.sandbox.runtime import (
    _REFLECT_RUN_SCRIPT,
    _VM_RUN_SCRIPT,
    GooseJobRunner,
    GooseJobRuntimeError,
    JobCancelledError,
    _DelegationTracker,
    _LogCollector,
    _SpanActivity,
    _stream_event_log_records,
)
from kafkallm.sandbox.sandboxes import SandboxStore
from kafkallm.worker.harness import Harness


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: dict[str, Any] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        return self._payload


class NotFound(Exception):
    pass


class FakeNetwork:
    def __init__(self, name: str) -> None:
        self.name = name
        self.connected: list[FakeContainer] = []

    def connect(self, container: FakeContainer, **kwargs: Any) -> None:
        del kwargs
        wrapper = {
            "GlobalIPv6Address": "fd00:6b61:666b:616c::2",
            "GlobalIPv6PrefixLen": 64,
        }
        container.attrs.setdefault("NetworkSettings", {}).setdefault("Networks", {})[
            self.name
        ] = wrapper
        self.connected.append(container)


class FakeNetworks:
    def __init__(self) -> None:
        self.items = {
            "kafkallm-jobs": FakeNetwork("kafkallm-jobs"),
            "podman": FakeNetwork("podman"),
        }

    def get(self, name: str) -> FakeNetwork:
        try:
            return self.items[name]
        except KeyError as exc:
            raise NotFound(name) from exc


class FakeContainer:
    def __init__(
        self,
        client: FakeDockerClient,
        container_id: str,
        name: str = "",
        body: dict[str, Any] | None = None,
    ) -> None:
        self.client = client
        self.id = container_id
        self.name = name
        self.body = body or client.create_body
        self.labels = self.body.get("Labels", {})
        self.role = self.labels.get("kafkallm.role", "main")
        self.is_job = self.labels.get("kafkallm.vm") == "1"
        self.status = "created"
        self.exit_code = 0
        self.stopped = False
        self.removed = False
        self.attrs: dict[str, Any] = {
            "Image": client.image_id,
            "NetworkSettings": {"Networks": {}},
        }
        self.rootfs = client.rootfs / container_id
        self.rootfs.mkdir(parents=True, exist_ok=True)

    @property
    def workspace(self) -> Path:
        return self.rootfs / "workspace"

    def _path(self, path: str) -> Path:
        return self.rootfs / Path(path).relative_to("/")

    def put_archive(self, path: str, data: Any) -> bool:
        destination = self._path(path)
        destination.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=data, mode="r:*") as archive:
            for member in archive:
                relative = Path(member.name)
                if relative.is_absolute() or ".." in relative.parts:
                    continue
                target = destination / relative
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                elif member.isfile():
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = archive.extractfile(member)
                    if source is not None:
                        target.write_bytes(source.read())
        return True

    def get_archive(self, path: str) -> tuple[Any, dict[str, Any]]:
        source = self._path(path)
        if not source.exists():
            raise NotFound(path)
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            root_name = Path(path).name
            if source.is_dir():
                archive.add(source, arcname=root_name, recursive=True)
            else:
                archive.add(source, arcname=root_name)
        buffer.seek(0)
        return iter([buffer.read()]), {
            "name": Path(path).name,
            "isDir": source.is_dir(),
        }

    def start(self) -> None:
        if not self.is_job:
            self.status = "running"
            return
        self.status = self.client.exit_status
        if self.role == "reflect":
            self.exit_code = self.client.reflect_exit_code
        else:
            self.exit_code = self.client.exit_code
        self.client.write_results(self)
        hook = self.client.on_start
        if hook is not None:
            hook(self)

    def reload(self) -> None:
        pass

    def wait(self, timeout: int | None = None) -> dict[str, int]:
        del timeout
        return {"StatusCode": self.exit_code}

    def stop(self, timeout: int = 10) -> None:
        del timeout
        self.stopped = True
        self.status = "exited"

    def remove(self, force: bool = False) -> None:
        del force
        if self.client.fail_remove and self.is_job and self.role == "main":
            raise RuntimeError("cannot remove")
        self.removed = True

    def logs(self, **kwargs: Any) -> bytes:
        del kwargs
        return b"microvm logs"


class FakeApi:
    def __init__(self, client: FakeDockerClient) -> None:
        self.client = client

    def _url(self, path: str) -> str:
        return f"http://fake{path}"

    def _post(
        self,
        url: str,
        data: str | None = None,
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> FakeResponse:
        del headers
        body = json.loads(data or "{}")
        name = str((kwargs.get("params") or {}).get("name", ""))
        if self.client.create_error is not None and not name:
            return FakeResponse(
                status_code=500,
                text=str(self.client.create_error),
            )
        container_id = (
            f"proxy-container-id-{len(self.client.containers.items)}"
            if name
            else f"vm-container-id-{len(self.client.containers.items)}"
        )
        container = FakeContainer(self.client, container_id, name=name, body=body)
        self.client.containers.items.append(container)
        if not name:
            role = str((body.get("Labels") or {}).get("kafkallm.role", "main"))
            if role == "reflect":
                self.client.reflect_body = body
                self.client.reflect_container = container
            else:
                self.client.create_body = body
                self.client.job_body = body
                self.client.main_body = body
                self.client.create_container = container
                self.client.main_container = container
        else:
            self.client.proxy_body = body
        return FakeResponse(201, {"Id": container_id})


class FakeContainers:
    def __init__(self, client: FakeDockerClient) -> None:
        self.client = client
        self.items: list[FakeContainer] = []

    def get(self, container_id: str) -> FakeContainer:
        for container in self.client.containers.items:
            if container.id == container_id or container.name == container_id:
                return container
        raise NotFound(container_id)

    def create(self, **kwargs: Any) -> FakeContainer:
        del kwargs
        container = FakeContainer(self.client, "vm-container-id")
        self.client.containers.items.append(container)
        return container

    def list(
        self, all: bool = False, filters: dict[str, str] | None = None
    ) -> list[FakeContainer]:
        del all, filters
        return list(self.client.containers.items)


class FakeImages:
    def __init__(self, client: FakeDockerClient) -> None:
        self.client = client

    def get(self, name: str) -> Any:
        available = (
            self.client.proxy_image_available
            if name == self.client.proxy_image
            else self.client.image_available
        )
        if not available:
            raise RuntimeError("no such image")
        return SimpleNamespace(id=self.client.image_id, tags=[name])

    def pull(self, name: str, **kwargs: Any) -> Any:
        del kwargs
        self.client.pulled.append(name)
        if name == self.client.proxy_image:
            self.client.proxy_image_available = True
        else:
            self.client.image_available = True
        return SimpleNamespace(id=self.client.image_id, tags=[name])


class FakeDockerClient:
    def __init__(self) -> None:
        self.containers = FakeContainers(self)
        self.images = FakeImages(self)
        self.networks = FakeNetworks()
        self.api = FakeApi(self)
        self.rootfs = Path(tempfile.mkdtemp(prefix="kafkallm-fake-rootfs-"))
        self.create_body: dict[str, Any] = {}
        self.main_body: dict[str, Any] = {}
        self.job_body: dict[str, Any] = {}
        self.reflect_body: dict[str, Any] = {}
        self.proxy_body: dict[str, Any] = {}
        self.create_container: FakeContainer | None = None
        self.main_container: FakeContainer | None = None
        self.reflect_container: FakeContainer | None = None
        self.create_error: str | None = None
        self.image_available = True
        self.proxy_image_available = True
        self.image_id = "image-id"
        self.proxy_image = "proxy:test"
        self.exit_status = "exited"
        self.exit_code = 0
        self.reflect_exit_code = 0
        self.pinged = False
        self.closed = False
        self.pulled: list[str] = []
        self.results: dict[str, str] = {}
        self.on_start: Callable[[FakeContainer], None] | None = None
        self.fail_remove = False

    def ping(self) -> bool:
        self.pinged = True
        return True

    def close(self) -> None:
        self.closed = True

    def write_results(self, container: FakeContainer) -> None:
        result_dir = self._result_dir(container)
        result_dir.mkdir(parents=True, exist_ok=True)
        for name, content in self.results.items():
            if (container.role == "reflect") != name.startswith("reflect"):
                continue
            (result_dir / name).write_text(content, encoding="utf-8")

    @staticmethod
    def _result_dir(container: FakeContainer) -> Path:
        return container.workspace / ".kafkallm"


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event_type: str, status: str = "info", **kwargs: Any) -> str:
        self.events.append({"type": event_type, "status": status, **kwargs})
        return f"event-{len(self.events)}"


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "goose_job_docker_host": "unix:///run/user/1000/podman/podman.sock",
        "goose_image": "goose:test",
        "goose_re_image": "rea:test",
        "goose_job_proxy_image": "proxy:test",
        "goose_job_ram_mib": 2048,
        "goose_job_cpus": 2,
        "goose_job_use_passt": False,
        "goose_job_workspace_root": "/home/test/.local/share/kafkallm/jobs",
        "goose_job_skills_dir": "/home/test/.local/share/kafkallm/skills",
        "goose_job_max_turns": 20,
        "goose_job_subagent_max_turns": 10,
        "goose_job_memory_limit": "4g",
        "goose_job_pids_limit": 512,
        "goose_job_reflect_enabled": True,
        "goose_job_reflect_timeout_seconds": 60,
        "goose_model": "test-model",
        "goose_provider": "openrouter",
        "llm_gateway_url": "http://gateway:8091",
        "llm_gateway_virtual_key": "test-virtual-key",
        "llm_gateway_ready_url": "",
        "llm_gateway_port": 8091,
        "goose_job_network": "kafkallm-jobs",
        "goose_job_network_subnet": "fd00:6b61:666b:616c::/64",
        "goose_job_network_interface": "",
        "goose_job_network_dir": "",
        "goose_job_network_autocreate": False,
        "goose_job_proxy_network": "podman",
        "goose_job_proxy_upstream_host": "host.containers.internal",
        "goose_job_proxy_upstream_port": 8091,
    }
    values.update(overrides)
    return replace(Settings.from_env(), **values)


def _goose_result(
    *,
    text: str = "Done",
    error: str | None = None,
    status: str = "ok",
) -> str:
    if error:
        content = [{"type": "error", "message": error}]
    else:
        content = [{"type": "text", "text": text}]
    return json.dumps(
        {
            "messages": [{"role": "assistant", "content": content}],
            "metadata": {
                "status": status,
                "input_tokens": 10,
                "output_tokens": 5,
                "total_tokens": 15,
                "cache_read_input_tokens": 2,
            },
        }
    )


def _settings_for(tmp_path: Path, **overrides: Any) -> Settings:
    """Settings whose job directories live inside the test directory."""
    values: dict[str, Any] = {
        "goose_job_workspace_root": str(tmp_path / "jobs"),
        "goose_job_skills_dir": str(tmp_path / "skills"),
        "goose_job_tools_dir": str(tmp_path / "tools"),
        "goose_job_config_dir": str(tmp_path / "config"),
    }
    values.update(overrides)
    return _settings(**values)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "job-workspace"
    inputs = workspace / "inputs"
    inputs.mkdir(parents=True)
    (inputs / "input.txt").write_text("input data", encoding="utf-8")
    return workspace


def test_vm_run_starts_microvm_and_collects_result(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "0",
    }
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)
    job = Job(instruction="Do the job", input={"key": "value"})

    result = runner.run(
        job=job,
        workspace=workspace,
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    assert result.usage["total_tokens"] == 15
    assert result.usage["cache_read_tokens"] == 2

    body = client.create_body
    annotations = body["HostConfig"]["Annotations"]
    assert annotations["run.oci.handler"] == "krun"
    assert annotations["krun.ram_mib"] == "2048"
    assert annotations["krun.cpus"] == "2"
    binds = body["HostConfig"].get("Binds", [])
    assert binds == []
    assert "Tmpfs" not in body["HostConfig"]
    assert client.main_container is not None
    root = client.main_container.rootfs
    assert (root / "workspace" / "inputs").is_dir()
    assert (root / "workspace" / ".claude" / "agents").is_dir()
    assert (root / "jobboot" / "run-job.sh").is_file()
    assert (root / "opt" / "kafkallm" / "skills-persistent").is_dir()
    assert (root / "opt" / "kafkallm" / "tools").is_dir()
    assert (root / "opt" / "kafkallm" / "config").is_dir()
    assert body["Image"] == "goose:test"
    assert body["HostConfig"]["Memory"] == 4 * 1024**3
    assert body["HostConfig"]["PidsLimit"] == 512
    environment = body["Env"]
    assert "GOOSE_MODE=auto" in environment
    assert "GOOSE_PROVIDER=openai" in environment
    assert "OPENAI_HOST=http://gateway:8091" in environment
    assert "GOOSE_MAX_TURNS=20" in environment
    assert "GOOSE_SUBAGENT_MAX_TURNS=10" in environment
    assert "OPENAI_API_KEY=test-virtual-key" in environment
    assert any(item.startswith("PATH=/opt/kafkallm/tools/bin:") for item in environment)
    assert "KAFKALLM_TOOLS_DIR=/opt/kafkallm/tools" in environment
    assert "KAFKALLM_CONFIG_DIR=/opt/kafkallm/config" in environment
    assert "KAFKALLM_MULTI_AGENT=1" in environment
    assert "KAFKALLM_AGENT_COUNT=0" in environment
    assert body["HostConfig"]["NetworkMode"] == "kafkallm-jobs"
    assert body["HostConfig"]["ExtraHosts"] == [
        "host.containers.internal:fd00:6b61:666b:616c::2"
    ]
    assert "LISTEN_PORT=8091" in client.proxy_body["Env"]
    assert "UPSTREAM_HOST=host.containers.internal" in client.proxy_body["Env"]
    assert client.proxy_body["Image"] == "proxy:test"
    assert client.proxy_body["Entrypoint"] == ["/usr/bin/python3"]

    assert [event["type"] for event in audit.events] == [
        "goose.started",
        "goose.reflect",
        "goose.completed",
    ]
    assert audit.events[1]["status"] == "succeeded"


def test_vm_run_copies_outputs_and_removes_workdir(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    def write_output(container: FakeContainer) -> None:
        if container.role != "main":
            return
        outputs = container.workspace / "outputs"
        outputs.mkdir(parents=True, exist_ok=True)
        (outputs / "result.txt").write_text("artifact", encoding="utf-8")

    client.on_start = write_output

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
    )

    assert result.text == "Completed"
    assert (workspace / "outputs" / "result.txt").read_text() == "artifact"
    assert not list((tmp_path / "jobs").glob("goose-vm-*"))


def test_vm_provider_error_is_propagated(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.exit_code = 1
    client.results = {
        "main.json": _goose_result(error="Provider failed", status="error")
    }
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    with pytest.raises(GooseJobRuntimeError, match="Provider failed"):
        runner.run(
            job=Job(instruction="Fail"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
            audit=audit,  # type: ignore[arg-type]
        )

    assert audit.events[-1]["type"] == "goose.completed"
    assert audit.events[-1]["status"] == "failed"


def test_vm_missing_result_uses_stderr(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.exit_code = 1
    client.results = {"main.err": "No AI provider is configured."}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    with pytest.raises(GooseJobRuntimeError, match="No AI provider is configured"):
        runner.run(
            job=Job(instruction="Fail without provider"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
        )


def test_vm_create_failure_is_reported(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.create_error = "no runtime handler"
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    with pytest.raises(GooseJobRuntimeError, match="no runtime handler"):
        runner.run(
            job=Job(instruction="Fail"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
        )


def test_vm_passt_annotation_is_opt_in(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(
        _settings_for(tmp_path, goose_job_use_passt=True), client=client
    )
    workspace = _workspace(tmp_path)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
    )

    annotations = client.create_body["HostConfig"]["Annotations"]
    assert annotations["krun.use_passt"] == "1"


def test_vm_wait_ready_pulls_a_missing_image(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.image_available = False
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    runner.wait_ready()

    assert client.pulled == ["goose:test"]
    assert client.pinged


def test_reverse_sandbox_uses_re_image_and_rea_environment(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Analyzed")}
    runner = GooseJobRunner(
        _settings_for(tmp_path, goose_re_image="rea:test"), client=client
    )

    runner.run(
        job=Job(instruction="Analyze the binary", sandbox="reverse"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    body = client.create_body
    assert body["Image"] == "rea:test"
    environment = dict(item.split("=", 1) for item in body["Env"])
    assert environment["KAFKALLM_SANDBOX"] == "reverse"
    assert environment["GHIDRA_INSTALL_DIR"] == "/opt/ghidra"
    assert environment["JAVA_HOME"] == "/opt/java/openjdk"
    assert environment["REA_MCP_INPUT_SCHEMA_PROFILE"] == "compact"
    assert environment["REA_MCP_OUTPUT_SCHEMA_PROFILE"] == "off"
    assert "REA_GHIDRA_STARTUP_TIMEOUT_MS" in environment


def test_default_sandbox_uses_the_goose_image(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(
        _settings_for(tmp_path, goose_re_image="rea:test"), client=client
    )

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    body = client.create_body
    assert body["Image"] == "goose:test"
    environment = dict(item.split("=", 1) for item in body["Env"])
    assert environment["KAFKALLM_SANDBOX"] == "default"
    assert "GHIDRA_INSTALL_DIR" not in environment
    assert "REA_GHIDRA_STARTUP_TIMEOUT_MS" not in environment


def test_registered_sandbox_supplies_image_env_extensions_and_prompt(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Analyzed")}
    settings = _settings_for(tmp_path)
    sandboxes = SandboxStore(settings)
    sandboxes.put(
        SandboxSpec(
            sandbox_id="malware",
            name="Malware analysis",
            image="malware:test",
            instructions="Malware sandbox: use YARA before disassembly.",
            env={"YARA_RULES": "/opt/rules"},
            extensions=["rea:/opt/kafkallm/rea-mcp"],
        )
    )
    runner = GooseJobRunner(settings, client=client, sandboxes=sandboxes)
    job = Job(instruction="Analyze the sample", sandbox="malware")

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    body = client.create_body
    assert body["Image"] == "malware:test"
    environment = dict(item.split("=", 1) for item in body["Env"])
    assert environment["KAFKALLM_SANDBOX"] == "malware"
    assert environment["YARA_RULES"] == "/opt/rules"
    assert environment["KAFKALLM_SANDBOX_EXTENSIONS"] == "rea:/opt/kafkallm/rea-mcp"
    assert "Malware sandbox" in runner._build_prompt(job)


def test_disabled_registered_sandbox_is_rejected(tmp_path: Path) -> None:
    settings = _settings_for(tmp_path)
    sandboxes = SandboxStore(settings)
    sandboxes.put(
        SandboxSpec(
            sandbox_id="malware",
            name="Malware analysis",
            image="malware:test",
            enabled=False,
        )
    )
    runner = GooseJobRunner(settings, client=FakeDockerClient(), sandboxes=sandboxes)

    with pytest.raises(GooseJobRuntimeError, match="disabled"):
        runner.run(
            job=Job(instruction="Analyze", sandbox="malware"),
            workspace=_workspace(tmp_path),
            deadline=time.monotonic() + 120,
        )


def test_unknown_sandbox_is_rejected(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))

    with pytest.raises(GooseJobRuntimeError, match="Unknown sandbox"):
        runner._sandbox_for(Job(instruction="Analyze", sandbox="missing"))


def test_job_image_overrides_the_registry_image(tmp_path: Path) -> None:
    """The image reference from the Job message is the one that runs."""
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}
    settings = _settings_for(tmp_path)
    sandboxes = SandboxStore(settings)
    sandboxes.put(
        SandboxSpec(
            sandbox_id="custom",
            name="Custom",
            image="registry:new",
            env={"CUSTOM_FLAG": "on"},
        )
    )
    runner = GooseJobRunner(settings, client=client, sandboxes=sandboxes)
    job = Job(
        instruction="Run the submitted image",
        sandbox="custom",
        sandbox_image="localhost/custom:old",
    )

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    assert client.create_body["Image"] == "localhost/custom:old"
    # Environment settings still come from the selected registry entry.
    assert "CUSTOM_FLAG=on" in client.create_body["Env"]


def test_job_image_works_without_a_registry_entry(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    job = Job(
        instruction="Run an ad-hoc image",
        sandbox="adhoc",
        sandbox_image="ghcr.io/example/agent:1.2.3",
    )

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    assert client.create_body["Image"] == "ghcr.io/example/agent:1.2.3"


def test_job_image_is_pulled_when_missing(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.image_available = False
    client.results = {"main.json": _goose_result(text="Completed")}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    job = Job(
        instruction="Pull the image",
        sandbox="adhoc",
        sandbox_image="ghcr.io/example/agent:1.2.3",
    )

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    assert client.pulled == ["ghcr.io/example/agent:1.2.3"]
    assert client.create_body["Image"] == "ghcr.io/example/agent:1.2.3"


def test_job_can_disable_the_learning_pass(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    job = Job(instruction="Do the job", reflect=False)

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert client.reflect_body == {}
    assert "KAFKALLM_JOB_REFLECT_ENABLED" not in client.create_body["Env"]
    reflect = [event for event in audit.events if event["type"] == "goose.reflect"]
    assert reflect
    assert reflect[0]["attributes"]["status"] == "skipped"


def test_job_can_enable_the_learning_pass_when_the_worker_default_is_off(
    tmp_path: Path,
) -> None:
    settings = _settings_for(tmp_path, goose_job_reflect_enabled=False)
    client = FakeDockerClient()
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "0",
    }
    runner = GooseJobRunner(settings, client=client)
    job = Job(instruction="Do the job", reflect=True)

    runner.run(
        job=job,
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    # The job opted in even though the worker default is off.
    assert client.reflect_body
    assert "KAFKALLM_JOB_REFLECT_ENABLED" not in client.create_body["Env"]
    assert client.reflect_body["Labels"]["kafkallm.role"] == "reflect"
    timeout = [
        item
        for item in client.reflect_body["Env"]
        if item.startswith("KAFKALLM_JOB_REFLECT_TIMEOUT=")
    ]
    assert timeout
    assert int(timeout[0].split("=", 1)[1]) >= 10


def test_reverse_prompt_mentions_rea(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))

    prompt = runner._build_prompt(Job(instruction="Analyze", sandbox="reverse"))

    assert "REA MCP server" in prompt
    assert "Ghidra is installed at /opt/ghidra" in prompt
    assert "REA MCP server" not in runner._build_prompt(Job(instruction="Plain"))


def test_run_script_enables_registered_sandbox_extensions() -> None:
    assert "KAFKALLM_SANDBOX_EXTENSIONS" in _VM_RUN_SCRIPT
    assert '--with-extension "$extension"' in _VM_RUN_SCRIPT
    assert "extension_path=${extension#*:}" in _VM_RUN_SCRIPT
    assert "TMPDIR=/workspace/.scratch/tmp" in _VM_RUN_SCRIPT
    # The worker and the guest root run as the same host user, so neither
    # script needs world-writable modes.
    assert "chmod -R a+rwX" not in _VM_RUN_SCRIPT
    assert "chmod 0777" not in _VM_RUN_SCRIPT
    assert "chmod -R a+rwX" not in _REFLECT_RUN_SCRIPT
    assert "chmod 0777" not in _REFLECT_RUN_SCRIPT
    assert "seed_skills /opt/kafkallm/sandbox-skills" in _VM_RUN_SCRIPT
    assert "--output-format stream-json" in _VM_RUN_SCRIPT
    assert "stdbuf -oL -eL ./goose" in _VM_RUN_SCRIPT
    # Multi-agent runs need goose's summon and skills builtins.
    assert 'builtins="$builtins,summon,skills"' in _VM_RUN_SCRIPT
    assert '--with-builtin "$builtins"' in _VM_RUN_SCRIPT
    # Extensions are only passed to the main run; the learning pass has none.
    assert _VM_RUN_SCRIPT.count('--with-extension "$extension"') == 1
    assert "reflect-prompt.txt" not in _VM_RUN_SCRIPT
    assert "--with-extension" not in _REFLECT_RUN_SCRIPT
    assert '"$@"' not in _REFLECT_RUN_SCRIPT


def test_worker_removes_orphaned_job_and_reflect_directories(tmp_path: Path) -> None:
    root = tmp_path / "jobs"
    root.mkdir()
    old_job = root / "goose-vm-old-run"
    old_reflect = root / ".reflect-old-run"
    fresh_job = root / "goose-vm-fresh-run"
    for directory in (old_job, old_reflect, fresh_job):
        directory.mkdir()
    settings = _settings_for(tmp_path)
    stale = time.time() - settings.max_job_timeout_seconds - 600
    os.utime(old_job, (stale, stale))
    os.utime(old_reflect, (stale, stale))

    client = FakeDockerClient()
    runner = GooseJobRunner(settings, client=client)
    runner.wait_ready()

    assert not old_job.exists()
    assert not old_reflect.exists()
    assert fresh_job.exists()


def test_worker_recreates_a_stale_gateway_proxy(tmp_path: Path) -> None:
    client = FakeDockerClient()
    stale = FakeContainer(client, "stale-proxy", name="kafkallm-gateway-proxy")
    stale.attrs["Image"] = "old-image-id"
    stale.status = "running"
    client.containers.items.append(stale)

    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    runner.wait_ready()

    assert stale.removed
    current = [
        container
        for container in client.containers.items
        if container.name == "kafkallm-gateway-proxy" and container is not stale
    ]
    assert current
    assert current[-1].attrs["Image"] == client.image_id


def test_vm_does_not_pass_provider_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "real-provider-key")
    monkeypatch.setenv("LLM_UPSTREAM_API_KEY", "real-provider-key")
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
    )

    environment = client.create_body["Env"]
    assert not any("real-provider-key" in item for item in environment)
    assert not any(item.startswith("OPENROUTER_API_KEY=") for item in environment)
    assert not any(item.startswith("LLM_UPSTREAM_API_KEY=") for item in environment)
    assert not any(item.startswith("OPENAI_API_KEY=real") for item in environment)


def test_wait_ready_requires_gateway(tmp_path: Path) -> None:
    client = FakeDockerClient()
    runner = GooseJobRunner(
        _settings_for(tmp_path, llm_gateway_virtual_key=""), client=client
    )

    with pytest.raises(GooseJobRuntimeError, match="LLM gateway"):
        runner.wait_ready()

    assert not client.pinged


def test_harness_uses_goose_runtime(tmp_path: Path) -> None:
    settings = _settings(workspace_root=str(tmp_path))
    harness = Harness(settings)

    assert isinstance(harness.goose, GooseJobRunner)

    harness.close()


def test_unlimited_job_is_rejected_when_disabled(tmp_path: Path) -> None:
    settings = _settings(
        workspace_root=str(tmp_path),
        allow_unlimited_jobs=False,
    )
    harness = Harness(settings)

    result = harness.process(Job(instruction="Run forever", unlimited=True))

    harness.close()
    assert result.status == "failed"
    assert result.error is not None
    assert "Unlimited jobs are disabled" in result.error


def test_harness_marks_cancelled_results(tmp_path: Path) -> None:
    settings = _settings(workspace_root=str(tmp_path))
    harness = Harness(settings)

    def cancel(**kwargs: Any) -> AgentResult:
        del kwargs
        raise JobCancelledError("Job cancelled by user.")

    harness.goose.run = cancel  # type: ignore[method-assign]
    result = harness.process(Job(instruction="Do it"))

    harness.close()
    assert result.status == "failed"
    assert result.cancelled is True
    assert result.error == "Job cancelled by user."


def test_unlimited_environment_disables_timeout_and_turn_caps(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    settings = _settings_for(
        tmp_path,
        allow_unlimited_jobs=True,
        unlimited_max_turns=12345,
        unlimited_max_subagent_turns=6789,
    )
    runner = GooseJobRunner(settings, client=client)

    runner.run(
        job=Job(instruction="Run forever", unlimited=True),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    environment = dict(item.split("=", 1) for item in client.create_body["Env"])
    assert environment["KAFKALLM_JOB_UNLIMITED"] == "1"
    assert environment["KAFKALLM_JOB_MAIN_TIMEOUT"] == "0"
    assert environment["GOOSE_MAX_TURNS"] == "12345"
    assert environment["GOOSE_SUBAGENT_MAX_TURNS"] == "6789"


def test_run_polls_the_heartbeat_while_waiting(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    calls: list[int] = []

    runner.run(
        job=Job(instruction="Do it"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        heartbeat=lambda: calls.append(1) or False,
    )

    assert calls


def test_cancelled_job_stops_the_microvm(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    calls: list[int] = []

    def heartbeat() -> bool:
        calls.append(1)
        return len(calls) > 1

    with pytest.raises(JobCancelledError, match="cancelled"):
        runner.run(
            job=Job(instruction="Do it"),
            workspace=_workspace(tmp_path),
            deadline=time.monotonic() + 120,
            heartbeat=heartbeat,
        )

    assert len(calls) >= 2


def test_unlimited_result_is_stored_as_an_artifact(tmp_path: Path) -> None:
    settings = _settings_for(
        tmp_path,
        allow_unlimited_jobs=True,
        max_result_bytes=10,
    )
    harness = Harness(settings)
    outputs = tmp_path / "outputs"
    outputs.mkdir()

    result = harness._limit_result(
        AgentResult(text="x" * 100, steps=2),
        outputs,
        Job(instruction="Run forever", unlimited=True),
    )

    harness.close()
    assert (outputs / "result-full.md").read_text(encoding="utf-8") == "x" * 100
    assert result.text.startswith("x" * 10)
    assert "result-full.md" in result.text
    assert result.steps == 2


def test_normal_jobs_use_the_configured_result_limit(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="x" * 100)}
    runner = GooseJobRunner(
        _settings_for(
            tmp_path,
            goose_job_reflect_enabled=False,
            max_result_bytes=10,
        ),
        client=client,
    )

    with pytest.raises(GooseJobRuntimeError, match="too large"):
        runner.run(
            job=Job(instruction="Do it"),
            workspace=_workspace(tmp_path),
            deadline=time.monotonic() + 120,
        )


def test_prompt_and_bundle_include_selected_agents(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))
    agent = AgentSpec(
        agent_id="security-reviewer",
        name="Security reviewer",
        role="reviewer",
        description="Checks security-sensitive outputs.",
        instructions="Review for injection and secret leaks.",
        skills=["goosetown-reviewer"],
    )
    job = Job(
        instruction="Review this",
        multi_agent=True,
        agents=[agent],
    )

    prompt = runner._build_prompt(job)
    assert "Goosetown multi-agent mode is ON" in prompt
    assert "Security reviewer (reviewer)" in prompt
    assert "delegate(source:" in prompt
    assert "/jobboot/agents.json" in prompt
    assert "KAFKALLM_MULTI_AGENT" not in prompt

    workdir = tmp_path / "vm"
    workspace = _workspace(tmp_path)
    runner._prepare_workdir(workdir, workspace, prompt, job)
    bundle = json.loads(
        (workdir / "bundle" / "agents.json").read_text(encoding="utf-8")
    )
    assert bundle[0]["agent_id"] == "security-reviewer"
    source = (workdir / ".claude" / "agents" / "security-reviewer.md").read_text(
        encoding="utf-8"
    )
    assert 'name: "Security reviewer"' in source
    assert "Review for injection and secret leaks." in source
    assert "load_skill" in source
    assert "gtwall" in source


def test_orchestrator_is_not_exposed_as_a_delegate(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))
    orchestrator = AgentSpec(
        agent_id="orchestrator",
        name="Orchestrator",
        role="orchestrator",
        description="Coordinates the flock.",
    )
    worker = AgentSpec(
        agent_id="worker",
        name="Worker",
        role="worker",
        description="Builds artifacts.",
    )
    job = Job(
        instruction="Build it",
        multi_agent=True,
        agents=[orchestrator, worker],
    )
    workdir = tmp_path / "vm"

    runner._prepare_workdir(workdir, _workspace(tmp_path), "prompt", job)

    agents_dir = workdir / ".claude" / "agents"
    assert sorted(path.name for path in agents_dir.iterdir()) == ["worker.md"]
    prompt = runner._build_prompt(job)
    assert "Worker (worker)" in prompt
    assert "Orchestrator (orchestrator)" not in prompt


def test_single_agent_prompt_disables_delegation(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))

    prompt = runner._build_prompt(Job(instruction="Do it", multi_agent=False))

    assert "Goosetown multi-agent mode is OFF" in prompt
    assert "do not delegate" in prompt


def test_parse_goose_output_extracts_text_usage_and_steps() -> None:
    text, usage, steps, error = parse_goose_output(_goose_result(text="PONG"))

    assert text == "PONG"
    assert usage == {
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
        "cache_read_tokens": 2,
    }
    assert steps == 1
    assert error is None


def test_parse_goose_output_accepts_a_leading_banner() -> None:
    raw = "goose bannner\n" + _goose_result(text="PONG")

    text, _, _, error = parse_goose_output(raw)

    assert text == "PONG"
    assert error is None


def test_parse_goose_output_accepts_stream_json() -> None:
    raw = "\n".join(
        [
            json.dumps(
                {
                    "type": "message",
                    "message": {
                        "id": "m1",
                        "role": "assistant",
                        "content": [{"type": "thinking", "thinking": "think"}],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "message",
                    "message": {
                        "id": "m1",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "stream"}],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "message",
                    "message": {
                        "id": "m1",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "-ok"}],
                    },
                }
            ),
            json.dumps(
                {
                    "type": "complete",
                    "total_tokens": 10,
                    "input_tokens": 8,
                    "output_tokens": 2,
                }
            ),
        ]
    )

    text, usage, steps, error = parse_goose_output(raw)

    assert text == "stream-ok"
    assert usage == {
        "input_tokens": 8,
        "output_tokens": 2,
        "total_tokens": 10,
    }
    assert steps == 1
    assert error is None


def test_parse_goose_output_file_reads_past_the_control_file_limit(
    tmp_path: Path,
) -> None:
    path = tmp_path / "main.jsonl"
    tools = [f"tool_{index}" for index in range(180)]
    filler = json.dumps(
        {
            "type": "message",
            "message": {
                "id": "m0",
                "role": "assistant",
                "content": [{"type": "thinking", "thinking": "..."}],
                "metadata": {"operations": {"llm": {"advertised_tools": tools}}},
            },
        }
    )
    with path.open("w", encoding="utf-8") as stream:
        stream.write(
            json.dumps(
                {
                    "type": "message",
                    "message": {
                        "id": "m0",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "STALE"}],
                    },
                }
            )
            + "\n"
        )
        written = stream.tell()
        while written < 17 * 1024 * 1024:
            stream.write(filler + "\n")
            written += len(filler) + 1
        stream.write(
            json.dumps(
                {
                    "type": "message",
                    "message": {
                        "id": "m1",
                        "role": "assistant",
                        "content": [{"type": "text", "text": "FINAL ANSWER"}],
                    },
                }
            )
            + "\n"
        )
        stream.write(
            json.dumps(
                {
                    "type": "complete",
                    "input_tokens": 1,
                    "output_tokens": 2,
                    "total_tokens": 3,
                }
            )
            + "\n"
        )

    text, usage, steps, error = parse_goose_output_file(path)

    assert text == "FINAL ANSWER"
    assert usage == {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
    assert steps == 2
    assert error is None


def test_parse_goose_output_file_accepts_legacy_document(tmp_path: Path) -> None:
    path = tmp_path / "main.json"
    path.write_text(_goose_result(text="PONG"), encoding="utf-8")

    text, usage, steps, error = parse_goose_output_file(path)

    assert text == "PONG"
    assert usage["total_tokens"] == 15
    assert steps == 1
    assert error is None


def test_parse_goose_output_file_reports_missing_output(tmp_path: Path) -> None:
    assert parse_goose_output_file(tmp_path / "main.jsonl") == (
        "",
        {},
        0,
        NO_OUTPUT_ERROR,
    )


def test_parse_goose_output_file_rejects_oversized_lines(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("kafkallm.sandbox.output._MAX_STREAM_LINE_BYTES", 128)
    path = tmp_path / "main.jsonl"
    event = {
        "type": "message",
        "message": {
            "id": "m1",
            "role": "assistant",
            "content": [{"type": "text", "text": "x" * 500}],
        },
    }
    path.write_text(json.dumps(event) + "\n", encoding="utf-8")

    text, _, _, error = parse_goose_output_file(path)

    assert text == ""
    assert error is not None
    assert "larger than" in error


def test_stream_event_log_records_extract_tool_activity() -> None:
    event = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {"type": "tool_use", "name": "shell", "input": {"command": "ls"}}
            ],
        },
    }

    records = _stream_event_log_records(event, "goose")

    assert records == [("goose.tool", '{"name": "shell", "input": {"command": "ls"}}')]


def test_stream_event_log_records_extract_goose_tool_request_and_response() -> None:
    request = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "toolRequest",
                    "toolCall": {
                        "value": {
                            "name": "shell",
                            "arguments": {"command": "echo hello"},
                        }
                    },
                }
            ],
        },
    }
    response = {
        "type": "message",
        "message": {
            "role": "user",
            "content": [
                {
                    "type": "toolResponse",
                    "toolResult": {
                        "value": {
                            "content": [{"type": "text", "text": "hello"}],
                            "structuredContent": {"stdout": "hello"},
                        }
                    },
                }
            ],
        },
    }

    assert _stream_event_log_records(request, "goose") == [
        (
            "goose.tool",
            '{"name": "shell", "input": {"command": "echo hello"}}',
        )
    ]
    assert _stream_event_log_records(response, "goose") == [
        ("goose.tool_result", "hello")
    ]


def test_parse_goose_output_reports_error_blocks() -> None:
    _, _, _, error = parse_goose_output(
        _goose_result(error="API key expired.", status="error")
    )

    assert error == "API key expired."


def test_parse_goose_output_rejects_empty_output() -> None:
    _, _, _, error = parse_goose_output("")

    assert error is not None


def test_span_activity_records_bounded_events() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider(span_limits=SpanLimits(max_events=512))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    span = provider.get_tracer("test").start_span("goose.job.vm.run")

    activity = _SpanActivity(span, limit=2)
    activity("goose.thinking", "first")
    activity("goose.tool", '{"name": "delegate"}')
    activity("goose.assistant", "dropped by the limit")
    activity("stderr", "not a trace event")
    span.end()

    events = exporter.get_finished_spans()[0].events
    assert [event.name for event in events] == ["goose.thinking", "goose.tool"]
    assert events[0].attributes["kafkallm.text"] == "first"


def test_delegation_tracker_emits_agent_events() -> None:
    tracker = _DelegationTracker()
    request = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "toolRequest",
                    "id": "call-1",
                    "toolCall": {
                        "value": {
                            "name": "delegate",
                            "arguments": {
                                "source": "Reviewer",
                                "instructions": "Check it",
                            },
                        }
                    },
                }
            ],
        },
    }
    ack = {
        "type": "message",
        "message": {
            "role": "user",
            "content": [
                {
                    "type": "toolResponse",
                    "id": "call-1",
                    "toolResult": {
                        "value": {
                            "content": [
                                {"type": "text", "text": "Delegated to subagent s-1"}
                            ],
                            "_meta": {"subagent_session_id": "s-1"},
                        }
                    },
                }
            ],
        },
    }
    completed = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "systemNotification",
                    "notificationType": "inlineMessage",
                    "msg": "Subagent s-1 completed",
                }
            ],
        },
    }

    assert tracker.observe_event(request) == [("agent.delegated", "Reviewer: Check it")]
    assert tracker.observe_event(ack) == []
    returned = tracker.observe_event(completed)
    assert returned[0][0] == "agent.returned"
    assert returned[0][1].startswith("Reviewer completed (")
    assert "subagent s-1" in returned[0][1]


def test_delegation_tracker_handles_ad_hoc_and_other_tools() -> None:
    tracker = _DelegationTracker()

    other = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [{"type": "tool_use", "name": "shell", "input": {"cmd": "ls"}}],
        },
    }
    assert tracker.observe_event(other) == []

    ad_hoc = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "toolRequest",
                    "id": "call-2",
                    "toolCall": {"value": {"name": "delegate", "arguments": {}}},
                }
            ],
        },
    }
    assert tracker.observe_event(ad_hoc) == [("agent.delegated", "ad-hoc")]

    result = {
        "type": "message",
        "message": {
            "role": "user",
            "content": [
                {
                    "type": "toolResponse",
                    "id": "call-2",
                    "toolResult": {
                        "value": {"content": [{"type": "text", "text": "Review text"}]}
                    },
                }
            ],
        },
    }
    returned = tracker.observe_event(result)
    assert returned[0][0] == "agent.returned"
    assert returned[0][1].startswith("ad-hoc (")
    assert returned[0][1].endswith(": Review text")


def test_log_collector_emits_delegation_events(tmp_path: Path) -> None:
    result_dir = tmp_path / ".kafkallm"
    result_dir.mkdir(parents=True)
    call = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "toolRequest",
                    "id": "call-1",
                    "toolCall": {
                        "value": {
                            "name": "delegate",
                            "arguments": {
                                "source": "Reviewer",
                                "instructions": "Check it",
                            },
                        }
                    },
                }
            ],
        },
    }
    ack = {
        "type": "message",
        "message": {
            "role": "user",
            "content": [
                {
                    "type": "toolResponse",
                    "id": "call-1",
                    "toolResult": {
                        "value": {
                            "content": [
                                {"type": "text", "text": "Delegated to subagent s-1"}
                            ],
                            "_meta": {"subagent_session_id": "s-1"},
                        }
                    },
                }
            ],
        },
    }
    completed = {
        "type": "message",
        "message": {
            "role": "assistant",
            "content": [
                {
                    "type": "systemNotification",
                    "notificationType": "inlineMessage",
                    "msg": "Subagent s-1 completed",
                }
            ],
        },
    }
    (result_dir / "main.jsonl").write_text(
        "\n".join(json.dumps(event) for event in (call, ack, completed)) + "\n",
        encoding="utf-8",
    )
    records: list[tuple[str, str]] = []
    collector = _LogCollector(tmp_path, lambda kind, text: records.append((kind, text)))
    collector.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not any(
            kind == "agent.returned" for kind, _ in records
        ):
            time.sleep(0.1)
    finally:
        collector.stop()

    kinds = [kind for kind, _ in records]
    assert "agent.delegated" in kinds
    assert "agent.returned" in kinds


def test_main_bundle_has_no_learning_pass_assets(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))
    workdir = tmp_path / "workdir"
    workspace = _workspace(tmp_path)

    runner._prepare_workdir(workdir, workspace, "prompt", Job(instruction="Do it"))

    bundle = workdir / "bundle"
    assert (bundle / "run-job.sh").exists()
    assert (bundle / "main-prompt.txt").read_text() == "prompt"
    assert not (bundle / "run-reflect.sh").exists()
    assert not (bundle / "reflect-prompt.txt").exists()
    assert (workdir / ".claude" / "skills").is_dir()
    assert list((workdir / ".claude" / "skills").iterdir()) == []


def test_main_vm_skill_edits_stay_in_the_disposable_copy(tmp_path: Path) -> None:
    store = tmp_path / "skills" / "guard"
    store.mkdir(parents=True)
    (store / "SKILL.md").write_text("# Guard\ntrusted\n", encoding="utf-8")
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}

    def edit_copy(container: FakeContainer) -> None:
        if container.role != "main":
            return
        workdir = container.workspace
        skill = workdir / ".claude" / "skills" / "guard" / "SKILL.md"
        skill.parent.mkdir(parents=True, exist_ok=True)
        skill.write_text("# Guard\npoisoned by the job\n", encoding="utf-8")

    client.on_start = edit_copy
    audit = FakeAudit()
    runner = GooseJobRunner(
        _settings_for(tmp_path, goose_job_reflect_enabled=False), client=client
    )

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert "trusted" in (store / "SKILL.md").read_text()
    assert "poisoned" not in (store / "SKILL.md").read_text()
    assert not [event for event in audit.events if event["type"] == "skills.persisted"]


def test_reflect_is_skipped_when_the_main_vm_cannot_be_removed(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}
    client.fail_remove = True
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    assert client.reflect_body == {}
    reflect = [event for event in audit.events if event["type"] == "goose.reflect"]
    assert reflect[-1]["status"] == "failed"
    assert "could not be removed" in reflect[-1]["error"]


def test_reflect_vm_uses_a_fresh_workspace_and_safe_mounts(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "0",
    }
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    main_removed: list[bool] = []

    def check_main_removed(container: FakeContainer) -> None:
        if container.role == "reflect":
            main_removed.append(
                any(
                    item.is_job and item.role == "main" and item.removed
                    for item in client.containers.items
                )
            )

    client.on_start = check_main_removed
    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
    )

    assert main_removed == [True]

    body = client.reflect_body
    assert body["Labels"]["kafkallm.role"] == "reflect"
    assert body["HostConfig"].get("Binds", []) == []
    assert client.reflect_container is not None
    reflect_root = client.reflect_container.rootfs
    assert (reflect_root / "jobboot" / "run-reflect.sh").is_file()
    assert (reflect_root / "workspace" / "outputs").is_dir()
    assert not (reflect_root / "workspace" / "inputs").exists()
    assert not (reflect_root / "opt" / "kafkallm" / "skills-persistent").exists()

    environment = body["Env"]
    assert not any(item.startswith("KAFKALLM_SANDBOX=") for item in environment)
    assert "KAFKALLM_MULTI_AGENT=1" not in environment
    assert "OPENAI_API_KEY=test-virtual-key" in environment


def test_reflect_merges_a_validated_skill_into_the_store(tmp_path: Path) -> None:
    store = tmp_path / "skills" / "guard"
    store.mkdir(parents=True)
    (store / "SKILL.md").write_text("# Guard\ntrusted\n", encoding="utf-8")
    client = FakeDockerClient()
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "0",
    }

    def write_skill(container: FakeContainer) -> None:
        if container.role != "reflect":
            return
        root = container.workspace
        learned = root / ".claude" / "skills" / "learned"
        learned.mkdir(parents=True)
        (learned / "SKILL.md").write_text("# Learned\nprocedure\n", encoding="utf-8")

    client.on_start = write_skill
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    merged = tmp_path / "skills" / "learned" / "SKILL.md"
    assert merged.read_text() == "# Learned\nprocedure\n"
    assert "trusted" in (store / "SKILL.md").read_text()
    persisted = [event for event in audit.events if event["type"] == "skills.persisted"]
    assert persisted
    assert persisted[0]["attributes"]["skills"] == ["learned"]


def test_failed_reflect_does_not_merge_skills(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "1",
    }

    def write_skill(container: FakeContainer) -> None:
        if container.role != "reflect":
            return
        root = container.workspace
        learned = root / ".claude" / "skills" / "learned"
        learned.mkdir(parents=True)
        (learned / "SKILL.md").write_text("# Learned\nprocedure\n", encoding="utf-8")

    client.on_start = write_skill
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert not (tmp_path / "skills" / "learned").exists()
    reflect = [event for event in audit.events if event["type"] == "goose.reflect"]
    assert reflect[-1]["status"] == "failed"
    assert not [event for event in audit.events if event["type"] == "skills.persisted"]


def test_symlinked_outputs_are_not_collected(tmp_path: Path) -> None:
    secret = tmp_path / "host-secret.txt"
    secret.write_text("host secret", encoding="utf-8")
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result(text="Completed")}

    def write_outputs(container: FakeContainer) -> None:
        if container.role != "main":
            return
        workdir = container.workspace
        outputs = workdir / "outputs"
        outputs.mkdir(parents=True, exist_ok=True)
        (outputs / "result.txt").write_text("artifact", encoding="utf-8")
        (outputs / "leak.txt").symlink_to(secret)
        (outputs / "leakdir").symlink_to(tmp_path)

    client.on_start = write_outputs
    runner = GooseJobRunner(
        _settings_for(tmp_path, goose_job_reflect_enabled=False), client=client
    )
    workspace = _workspace(tmp_path)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
    )

    assert (workspace / "outputs" / "result.txt").read_text() == "artifact"
    assert not (workspace / "outputs" / "leak.txt").exists()
    assert not (workspace / "outputs" / "leak.txt").is_symlink()
    assert not (workspace / "outputs" / "leakdir").exists()


def test_control_file_reads_reject_symlinks(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))
    secret = tmp_path / "secret.txt"
    secret.write_text("host secret", encoding="utf-8")

    workdir = tmp_path / "job"
    control = workdir / ".kafkallm"
    control.mkdir(parents=True)
    (control / "main.jsonl").symlink_to(secret)

    assert runner._read_text(workdir, "main.jsonl") == ""

    other = tmp_path / "other"
    other.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    (target / "main.jsonl").write_text("not allowed", encoding="utf-8")
    (other / ".kafkallm").symlink_to(target)

    assert runner._read_text(other, "main.jsonl") == ""


def test_reflect_vm_process_failure_does_not_merge_skills(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.reflect_exit_code = 1
    client.results = {
        "main.json": _goose_result(text="Completed"),
        "reflect.json": _goose_result(text="Skill updated"),
        "reflect.exit": "0",
    }

    def write_skill(container: FakeContainer) -> None:
        if container.role != "reflect":
            return
        root = container.workspace
        learned = root / ".claude" / "skills" / "learned"
        learned.mkdir(parents=True)
        (learned / "SKILL.md").write_text("# Learned\nprocedure\n", encoding="utf-8")

    client.on_start = write_skill
    audit = FakeAudit()
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=_workspace(tmp_path),
        deadline=time.monotonic() + 120,
        audit=audit,  # type: ignore[arg-type]
    )

    assert not (tmp_path / "skills" / "learned").exists()
    reflect = [event for event in audit.events if event["type"] == "goose.reflect"]
    assert reflect[-1]["status"] == "failed"
    assert "exited with code 1" in reflect[-1]["error"]
