from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from kafkallm.goose_job_runtime import (
    GooseJobRunner,
    GooseJobRuntimeError,
    _parse_bytes,
    parse_goose_output,
)
from kafkallm.harness import Harness
from kafkallm.models import Job
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


class FakeContainer:
    def __init__(self, client: "FakeDockerClient", container_id: str) -> None:
        self.client = client
        self.id = container_id
        self.labels = client.create_body.get("Labels", {})
        self.status = "created"
        self.exit_code = 0
        self.stopped = False
        self.removed = False

    def start(self) -> None:
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
    def __init__(self, client: "FakeDockerClient") -> None:
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
        del headers, kwargs
        body = json.loads(data or "{}")
        self.client.create_body = body
        if self.client.create_error is not None:
            return FakeResponse(
                status_code=500,
                text=str(self.client.create_error),
            )
        return FakeResponse(201, {"Id": "vm-container-id"})


class FakeContainers:
    def __init__(self, client: "FakeDockerClient") -> None:
        self.client = client
        self.items: list[FakeContainer] = []

    def get(self, container_id: str) -> FakeContainer:
        for container in self.client.containers.items:
            if container.id == container_id:
                return container
        container = FakeContainer(self.client, container_id)
        self.client.containers.items.append(container)
        return container

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
    def __init__(self, client: "FakeDockerClient") -> None:
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
        self.api = FakeApi(self)
        self.create_body: dict[str, Any] = {}
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

    result = runner.run(
        job=Job(instruction="Do the job", input={"key": "value"}),
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
    assert body["Image"] == "goose:test"
    assert body["HostConfig"]["Memory"] == 4 * 1024**3
    assert body["HostConfig"]["PidsLimit"] == 512
    environment = body["Env"]
    assert "GOOSE_MODE=auto" in environment
    assert "GOOSE_PROVIDER=openrouter" in environment
    assert "GOOSE_MAX_TURNS=20" in environment
    assert "GOOSE_SUBAGENT_MAX_TURNS=10" in environment

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
        "main.json": _goose_result(
            error="Provider failed", status="error"
        )
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

    with pytest.raises(
        GooseJobRuntimeError, match="No AI provider is configured"
    ):
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


def test_harness_uses_goose_runtime(tmp_path: Path) -> None:
    settings = _settings(workspace_root=str(tmp_path))
    harness = Harness(settings)

    assert isinstance(harness.goose, GooseJobRunner)

    harness.close()


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
