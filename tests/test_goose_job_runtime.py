from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kafkallm.goose_job_runtime import (
    _VM_RUN_SCRIPT,
    GooseJobRunner,
    GooseJobRuntimeError,
    _parse_bytes,
    parse_goose_output,
)
from kafkallm.harness import Harness
from kafkallm.models import AgentSpec, Job
from kafkallm.settings import Settings


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
        self.is_job = self.labels.get("kafkallm.vm") == "1"
        self.status = "created"
        self.exit_code = 0
        self.stopped = False
        self.removed = False
        self.attrs: dict[str, Any] = {
            "NetworkSettings": {"Networks": {}},
        }

    def start(self) -> None:
        if not self.is_job:
            self.status = "running"
            return
        self.status = self.client.exit_status
        self.exit_code = self.client.exit_code
        self.client.write_results()

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
        container_id = "proxy-container-id" if name else "vm-container-id"
        container = FakeContainer(self.client, container_id, name=name, body=body)
        self.client.containers.items.append(container)
        if not name:
            self.client.create_body = body
            self.client.job_body = body
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
        if not self.client.image_available:
            raise RuntimeError("no such image")
        return SimpleNamespace(id="image-id", tags=[name])

    def pull(self, name: str, **kwargs: Any) -> Any:
        del kwargs
        self.client.pulled.append(name)
        self.client.image_available = True
        return SimpleNamespace(id="image-id", tags=[name])


class FakeDockerClient:
    def __init__(self) -> None:
        self.containers = FakeContainers(self)
        self.images = FakeImages(self)
        self.networks = FakeNetworks()
        self.api = FakeApi(self)
        self.create_body: dict[str, Any] = {}
        self.job_body: dict[str, Any] = {}
        self.proxy_body: dict[str, Any] = {}
        self.create_error: str | None = None
        self.image_available = True
        self.exit_status = "exited"
        self.exit_code = 0
        self.pinged = False
        self.closed = False
        self.pulled: list[str] = []
        self.results: dict[str, str] = {}

    def ping(self) -> bool:
        self.pinged = True
        return True

    def close(self) -> None:
        self.closed = True

    @property
    def workdir(self) -> Path:
        binds = self.create_body.get("HostConfig", {}).get("Binds", [])
        return Path(binds[0].split(":")[0])

    def write_results(self) -> None:
        result_dir = self.workdir / ".kafkallm"
        result_dir.mkdir(parents=True, exist_ok=True)
        for name, content in self.results.items():
            (result_dir / name).write_text(content, encoding="utf-8")


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event_type: str, status: str = "info", **kwargs: Any) -> str:
        self.events.append({"type": event_type, "status": status, **kwargs})
        return f"event-{len(self.events)}"


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "goose_job_docker_host": "tcp://podman-proxy:2375",
        "goose_image": "goose:test",
        "goose_re_image": "rea:test",
        "goose_job_ram_mib": 2048,
        "goose_job_cpus": 2,
        "goose_job_use_passt": False,
        "goose_job_workspace_root": "/var/lib/kafkallm/jobs",
        "goose_job_skills_dir": "/var/lib/kafkallm/skills",
        "goose_job_tmpfs_size": "256m",
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
        max_steps=5,
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
    binds = body["HostConfig"]["Binds"]
    assert any(bind.endswith(":/workspace:rw,z") for bind in binds)
    assert any(bind.endswith(":/jobboot:ro,z") for bind in binds)
    assert any("/workspace/.claude/skills:rw,z" in bind for bind in binds)
    assert any("/opt/kafkallm/tools:ro,z" in bind for bind in binds)
    assert any("/opt/kafkallm/config:ro,z" in bind for bind in binds)
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

    original_create = client.api._post

    def _post(url: str, **kwargs: Any) -> FakeResponse:
        response = original_create(url, **kwargs)
        if (kwargs.get("params") or {}).get("name"):
            return response
        outputs = client.workdir / "outputs"
        outputs.mkdir(parents=True, exist_ok=True)
        (outputs / "result.txt").write_text("artifact", encoding="utf-8")
        return response

    client.api._post = _post  # type: ignore[method-assign]

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
    )

    assert result.text == "Completed"
    assert (workspace / "outputs" / "result.txt").read_text() == "artifact"
    assert not client.workdir.exists()


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
            max_steps=5,
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
            max_steps=5,
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
            max_steps=5,
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
        max_steps=5,
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
        max_steps=5,
    )

    body = client.create_body
    assert body["Image"] == "rea:test"
    environment = dict(item.split("=", 1) for item in body["Env"])
    assert environment["KAFKALLM_SANDBOX"] == "reverse"
    assert environment["GHIDRA_INSTALL_DIR"] == "/opt/ghidra"
    assert environment["JAVA_HOME"] == "/opt/java/openjdk"
    assert environment["REA_MCP_INPUT_SCHEMA_PROFILE"] == "compact"
    assert environment["REA_MCP_OUTPUT_SCHEMA_PROFILE"] == "compact"
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
        max_steps=5,
    )

    body = client.create_body
    assert body["Image"] == "goose:test"
    environment = dict(item.split("=", 1) for item in body["Env"])
    assert environment["KAFKALLM_SANDBOX"] == "default"
    assert "GHIDRA_INSTALL_DIR" not in environment
    assert "REA_GHIDRA_STARTUP_TIMEOUT_MS" not in environment


def test_reverse_prompt_mentions_rea(tmp_path: Path) -> None:
    runner = GooseJobRunner(_settings_for(tmp_path))

    prompt = runner._build_prompt(Job(instruction="Analyze", sandbox="reverse"))

    assert "REA MCP server" in prompt
    assert "Ghidra is installed at /opt/ghidra" in prompt
    assert "REA MCP server" not in runner._build_prompt(Job(instruction="Plain"))


def test_run_script_enables_rea_only_for_reverse_sandbox() -> None:
    assert '--with-extension "rea:/opt/kafkallm/rea-mcp"' in _VM_RUN_SCRIPT
    assert "TMPDIR=/workspace/.scratch/tmp" in _VM_RUN_SCRIPT
    assert "chmod -R a+rwX /workspace/.scratch" in _VM_RUN_SCRIPT
    assert 'KAFKALLM_SANDBOX:-default' in _VM_RUN_SCRIPT


def test_vm_does_not_pass_provider_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "real-provider-key")
    client = FakeDockerClient()
    client.results = {"main.json": _goose_result()}
    runner = GooseJobRunner(_settings_for(tmp_path), client=client)
    workspace = _workspace(tmp_path)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
    )

    environment = client.create_body["Env"]
    assert not any("real-provider-key" in item for item in environment)
    assert not any(item.startswith("OPENROUTER_API_KEY=") for item in environment)
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
    assert "/jobboot/agents.json" in prompt
    assert "KAFKALLM_MULTI_AGENT" not in prompt

    workdir = tmp_path / "vm"
    workspace = _workspace(tmp_path)
    runner._prepare_workdir(workdir, workspace, prompt, job)
    bundle = json.loads(
        (workdir / "bundle" / "agents.json").read_text(encoding="utf-8")
    )
    assert bundle[0]["agent_id"] == "security-reviewer"


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


def test_parse_goose_output_reports_error_blocks() -> None:
    _, _, _, error = parse_goose_output(
        _goose_result(error="API key expired.", status="error")
    )

    assert error == "API key expired."


def test_parse_goose_output_rejects_empty_output() -> None:
    _, _, _, error = parse_goose_output("")

    assert error is not None


def test_parse_bytes() -> None:
    assert _parse_bytes("4g") == 4 * 1024**3
    assert _parse_bytes("512m") == 512 * 1024**2
    assert _parse_bytes("1024") == 1024
