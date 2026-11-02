from __future__ import annotations

import io
import json
import tarfile
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from hermes.harness import Harness
from hermes.hermes_job_runtime import (
    _RUN_SCRIPT,
    HermesJobRunner,
    HermesJobRuntimeError,
)
from hermes.models import Job
from hermes.settings import Settings


class FakeNotFound(Exception):
    status_code = 404


def _tar_file(name: str, content: bytes) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        info = tarfile.TarInfo(name=name)
        info.size = len(content)
        info.mode = 0o600
        archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _tar_outputs(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for name, content in files.items():
            info = tarfile.TarInfo(name=f"outputs/{name}")
            info.size = len(content)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


def _read_tar(data: bytes, name: str) -> bytes:
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as archive:
        member = archive.getmember(name)
        extracted = archive.extractfile(member)
        assert extracted is not None
        return extracted.read()


class FakeVolume:
    def __init__(self, name: str, labels: dict[str, str] | None = None) -> None:
        self.name = name
        self.attrs = {"Labels": labels or {}}
        self.removed = False

    def remove(self, force: bool = False) -> None:
        del force
        self.removed = True


class FakeVolumes:
    def __init__(self) -> None:
        self.items: list[FakeVolume] = []

    def get(self, name: str) -> FakeVolume:
        for volume in self.items:
            if volume.name == name and not volume.removed:
                return volume
        raise FakeNotFound(name)

    def create(self, name: str, labels: dict[str, str]) -> FakeVolume:
        volume = FakeVolume(name, labels)
        self.items.append(volume)
        return volume

    def list(self, filters: dict[str, str] | None = None) -> list[FakeVolume]:
        del filters
        return [volume for volume in self.items if not volume.removed]


class FakeContainer:
    def __init__(self, client: "FakeDockerClient", kwargs: dict[str, Any]) -> None:
        self.client = client
        self.kwargs = kwargs
        self.id = "container-id-1234567890"
        self.labels = kwargs.get("labels", {})
        self.status = "created"
        self.exit_code = 0
        self.started = False
        self.stopped = False
        self.removed = False
        self.exec_calls: list[Any] = []
        self.put_archives: list[tuple[str, bytes]] = []

    def start(self) -> None:
        self.started = True
        self.status = self.client.exit_status

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
        if self in self.client.containers.items:
            self.client.containers.items.remove(self)

    def exec_run(self, command: Any, user: str | None = None) -> Any:
        self.exec_calls.append((command, user))
        return SimpleNamespace(exit_code=0, output=b"")

    def put_archive(self, path: str, data: bytes) -> bool:
        self.put_archives.append((path, data))
        return True

    def get_archive(self, path: str) -> tuple[Any, dict[str, Any]]:
        if path == "/workspace/outputs":
            if self.client.output_tar is None:
                raise FakeNotFound(path)
            return iter([self.client.output_tar]), {}
        content = self.client.files.get(path)
        if content is None:
            raise FakeNotFound(path)
        return iter([_tar_file(Path(path).name, content)]), {}

    def logs(self, **kwargs: Any) -> bytes:
        del kwargs
        return b"container logs"


class FakeContainers:
    def __init__(self, client: "FakeDockerClient") -> None:
        self.client = client
        self.items: list[FakeContainer] = []
        self.created: list[FakeContainer] = []

    def create(self, **kwargs: Any) -> FakeContainer:
        if self.client.create_error is not None:
            raise self.client.create_error
        container = FakeContainer(self.client, kwargs)
        self.items.append(container)
        self.created.append(container)
        return container

    def list(
        self, all: bool = False, filters: dict[str, str] | None = None
    ) -> list[FakeContainer]:
        del all, filters
        return list(self.items)


class FakeImages:
    def __init__(self, client: "FakeDockerClient") -> None:
        self.client = client

    def get(self, name: str) -> Any:
        if not self.client.image_available:
            raise FakeNotFound(name)
        return SimpleNamespace(id="image-id", tags=[name])


class FakeDockerClient:
    def __init__(self) -> None:
        self.volumes = FakeVolumes()
        self.containers = FakeContainers(self)
        self.images = FakeImages(self)
        self.files: dict[str, bytes] = {}
        self.output_tar: bytes | None = None
        self.exit_status = "exited"
        self.image_available = True
        self.create_error: Exception | None = None
        self.pinged = False
        self.closed = False

    def ping(self) -> bool:
        self.pinged = True
        return True

    def close(self) -> None:
        self.closed = True


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit(self, event_type: str, status: str = "info", **kwargs: Any) -> str:
        self.events.append(
            {"type": event_type, "status": status, **kwargs}
        )
        return f"event-{len(self.events)}"


def _settings(**overrides: Any) -> Settings:
    values: dict[str, Any] = {
        "agent_runtime": "hermes-job",
        "hermes_image": "hermes:test",
        "hermes_skills_volume": "hermes-skills-test",
        "hermes_startup_timeout_seconds": 1,
        "hermes_job_tmpfs_size": "256m",
        "hermes_job_max_turns": 20,
        "hermes_job_memory_limit": "4g",
        "hermes_job_pids_limit": 512,
        "hermes_job_cpus": 2.0,
        "hermes_job_reflect_enabled": True,
        "hermes_job_reflect_timeout_seconds": 60,
        "hermes_model": "test-model",
        "hermes_provider": "openrouter",
    }
    values.update(overrides)
    return replace(Settings.from_env(), **values)


def _workspace(tmp_path: Path) -> Path:
    workspace = tmp_path / "job-workspace"
    inputs = workspace / "inputs"
    inputs.mkdir(parents=True)
    (inputs / "input.txt").write_text("input data", encoding="utf-8")
    return workspace


def _result_event(
    *,
    text: str = "Done",
    error: str | None = None,
    exit_code: int = 0,
    session_id: str = "session-1",
) -> bytes:
    event: dict[str, Any] = {
        "type": "result",
        "session_id": session_id,
        "exit_code": exit_code,
        "text": text,
        "tokens": {
            "input": 10,
            "output": 5,
            "total": 15,
            "cache_read": 2,
            "cache_write": 1,
        },
    }
    if error is not None:
        event["error"] = error
    return (json.dumps(event) + "\n").encode("utf-8")


def _runner(
    client: FakeDockerClient, **overrides: Any
) -> HermesJobRunner:
    return HermesJobRunner(_settings(**overrides), client=client)


def test_run_creates_isolated_container_and_collects_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "provider-secret")
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(text="Completed")
    client.files["/tmp/hermes-reflect.jsonl"] = _result_event(
        text="Skill updated", session_id="session-1"
    )
    client.output_tar = _tar_outputs({"result.txt": b"artifact"})
    audit = FakeAudit()
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    result = runner.run(
        job=Job(instruction="Do the job", input={"key": "value"}),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    assert result.steps == 0
    assert result.usage == {
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
        "cache_read_tokens": 2,
        "cache_write_tokens": 1,
    }
    assert (workspace / "outputs" / "result.txt").read_text() == "artifact"

    container = client.containers.created[0]
    assert container.started
    assert container.removed
    assert container.kwargs["image"] == "hermes:test"
    assert container.kwargs["network_mode"] == "bridge"
    assert container.kwargs["labels"]["hermes.job"] == "1"
    assert container.kwargs["tmpfs"]["/opt/data"].startswith("size=256m")
    assert container.kwargs["security_opt"] == ["no-new-privileges:true"]
    assert container.kwargs["pids_limit"] == 512
    assert container.kwargs["mem_limit"] == "4g"
    assert container.kwargs["memswap_limit"] == "4g"
    assert container.kwargs["nano_cpus"] == 2_000_000_000
    workspace_volume = next(
        name
        for name in container.kwargs["volumes"]
        if name.startswith("hermes-job-")
    )
    assert container.kwargs["volumes"][workspace_volume]["bind"] == "/workspace"
    assert container.kwargs["volumes"]["hermes-skills-test"]["bind"] == "/opt/data/skills"
    environment = container.kwargs["environment"]
    assert environment["HOME"] == "/opt/data"
    assert environment["HERMES_JOB_REFLECT_ENABLED"] == "1"
    assert environment["OPENAI_API_KEY"] == "provider-secret"
    assert environment["HERMES_JOB_MODEL"] == "test-model"
    assert environment["HERMES_JOB_PROVIDER"] == "openrouter"
    assert 100 <= int(environment["HERMES_JOB_BUDGET_SECONDS"]) <= 120
    assert environment["HERMES_JOB_REFLECT_TIMEOUT"] == "60"

    volumes = client.volumes.items
    assert len(volumes) == 1
    assert volumes[0].name.startswith("hermes-job-")
    assert volumes[0].removed

    put_paths = [path for path, _ in container.put_archives]
    assert "/workspace" in put_paths
    assert "/tmp" in put_paths
    prompt_data = next(
        data
        for path, data in container.put_archives
        if b"hermes-job-prompt.txt" in data
    )
    prompt = _read_tar(prompt_data, "hermes-job-prompt.txt").decode()
    assert "Do the job" in prompt
    assert "{'key': 'value'}" in prompt

    assert [event["type"] for event in audit.events] == [
        "hermes.started",
        "hermes.reflect",
        "hermes.completed",
    ]
    assert audit.events[1]["status"] == "succeeded"
    assert audit.events[2]["status"] == "succeeded"


def test_run_script_keeps_the_session_turn_policy_on_resume() -> None:
    reflect_section = _RUN_SCRIPT.split("--resume", 1)[1]
    assert '"--max-turns"' not in reflect_section


def test_resource_limits_can_be_disabled(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(text="Completed")
    runner = _runner(
        client,
        hermes_job_memory_limit="0",
        hermes_job_cpus=0.0,
        hermes_job_pids_limit=0,
    )
    workspace = _workspace(tmp_path)

    runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
    )

    container = client.containers.created[0]
    assert "mem_limit" not in container.kwargs
    assert "memswap_limit" not in container.kwargs
    assert "nano_cpus" not in container.kwargs
    assert container.kwargs["pids_limit"] == 0
    assert container.kwargs["security_opt"] == ["no-new-privileges:true"]


def test_result_error_is_propagated_and_resources_removed(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(
        text="Provider failed", error="Provider failed", exit_code=1
    )
    audit = FakeAudit()
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    with pytest.raises(HermesJobRuntimeError, match="Provider failed"):
        runner.run(
            job=Job(instruction="Fail"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
            max_steps=5,
            audit=audit,  # type: ignore[arg-type]
        )

    assert client.containers.created[0].removed
    assert client.volumes.items[0].removed
    assert [event["type"] for event in audit.events] == [
        "hermes.started",
        "hermes.completed",
    ]
    assert audit.events[-1]["status"] == "failed"
    assert audit.events[-1]["error"] == "Provider failed"


def test_missing_result_event_uses_stderr(tmp_path: Path) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.err"] = b"No AI provider is configured."
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    with pytest.raises(
        HermesJobRuntimeError, match="No AI provider is configured"
    ):
        runner.run(
            job=Job(instruction="Fail without provider"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
            max_steps=5,
        )

    assert client.containers.created[0].removed
    assert client.volumes.items[0].removed


def test_deadline_stops_container_and_removes_resources(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.exit_status = "running"
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    with pytest.raises(HermesJobRuntimeError, match="job deadline"):
        runner.run(
            job=Job(instruction="Hang"),
            workspace=workspace,
            deadline=time.monotonic() - 1,
            max_steps=5,
        )

    container = client.containers.created[0]
    assert container.stopped
    assert container.removed
    assert client.volumes.items[0].removed


def test_reflect_failure_does_not_fail_successful_job(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(text="Completed")
    client.files["/tmp/hermes-reflect.err"] = b"reflect exploded"
    audit = FakeAudit()
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    reflect_events = [
        event for event in audit.events if event["type"] == "hermes.reflect"
    ]
    assert reflect_events[0]["status"] == "failed"
    assert reflect_events[0]["error"] == "reflect exploded"
    assert audit.events[-1]["type"] == "hermes.completed"
    assert audit.events[-1]["status"] == "succeeded"


def test_reflect_nonzero_exit_is_reported_as_failed(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(text="Completed")
    client.files["/tmp/hermes-reflect.jsonl"] = _result_event(
        text="Reflect did not finish", exit_code=1
    )
    audit = FakeAudit()
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    reflect_events = [
        event for event in audit.events if event["type"] == "hermes.reflect"
    ]
    assert reflect_events[0]["status"] == "failed"
    assert reflect_events[0]["error"] == "Reflect did not finish"
    assert audit.events[-1]["type"] == "hermes.completed"
    assert audit.events[-1]["status"] == "succeeded"


def test_reflect_disabled_is_reported_as_skipped(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.files["/tmp/hermes-main.jsonl"] = _result_event(text="Completed")
    audit = FakeAudit()
    runner = _runner(client, hermes_job_reflect_enabled=False)
    workspace = _workspace(tmp_path)

    result = runner.run(
        job=Job(instruction="Do the job"),
        workspace=workspace,
        deadline=time.monotonic() + 120,
        max_steps=5,
        audit=audit,  # type: ignore[arg-type]
    )

    assert result.text == "Completed"
    assert "skill" not in result.text.lower()
    reflect_events = [
        event for event in audit.events if event["type"] == "hermes.reflect"
    ]
    assert reflect_events[0]["status"] == "info"
    assert reflect_events[0]["attributes"]["status"] == "skipped"


def test_container_creation_failure_removes_workspace_volume(
    tmp_path: Path,
) -> None:
    client = FakeDockerClient()
    client.create_error = RuntimeError("create failed")
    runner = _runner(client)
    workspace = _workspace(tmp_path)

    with pytest.raises(RuntimeError, match="create failed"):
        runner.run(
            job=Job(instruction="Fail"),
            workspace=workspace,
            deadline=time.monotonic() + 120,
            max_steps=5,
        )

    assert client.volumes.items[0].removed


def test_cancel_active_stops_running_container() -> None:
    client = FakeDockerClient()
    runner = _runner(client)
    container = FakeContainer(client, {})
    container.status = "running"
    runner._active_container = container

    runner.cancel_active()

    assert container.stopped


def test_wait_ready_checks_docker_and_cleans_old_orphans() -> None:
    client = FakeDockerClient()
    old = FakeContainer(client, {"labels": {"hermes.job": "1", "hermes.created_at": "0"}})
    old.status = "exited"
    client.containers.items.append(old)
    old_volume = client.volumes.create(
        "orphan-volume", {"hermes.job": "1", "hermes.created_at": "0"}
    )
    runner = _runner(client)

    runner.wait_ready()

    assert client.pinged
    assert old.removed
    assert old_volume.removed


def test_harness_selects_hermes_job_runtime(tmp_path: Path) -> None:
    settings = _settings(workspace_root=str(tmp_path))
    harness = Harness(settings, None)

    assert isinstance(harness.hermes, HermesJobRunner)

    harness.close()


def test_harness_rejects_unknown_runtime(tmp_path: Path) -> None:
    settings = _settings(agent_runtime="unknown", workspace_root=str(tmp_path))

    with pytest.raises(ValueError, match="Unknown AGENT_RUNTIME"):
        Harness(settings, None)
