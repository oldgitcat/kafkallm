import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from hermes.harness import Harness
from hermes.llm import EchoLLM, LLMReply, ToolCall
from hermes.models import Job
from hermes.settings import Settings


class ArtifactLLM:
    def __init__(self) -> None:
        self.calls = 0

    def complete(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        timeout_seconds: float,
    ) -> LLMReply:
        del messages, tools, timeout_seconds
        self.calls += 1
        if self.calls == 1:
            return LLMReply(
                tool_calls=[
                    ToolCall(
                        id="write-artifact",
                        name="write_workspace_file",
                        arguments_json=json.dumps(
                            {"path": "outputs/result.txt", "content": "Result"}
                        ),
                    )
                ]
            )
        return LLMReply(content="Done")


class FailingStorage:
    def upload_outputs(self, **kwargs: Any) -> list:
        del kwargs
        raise RuntimeError("Upload failed")


def test_harness_processes_echo_job(tmp_path: Path) -> None:
    settings = replace(
        Settings.from_env(),
        worker_id="test-worker",
        workspace_root=str(tmp_path),
        llm_provider="echo",
        s3_endpoint_url="",
    )
    harness = Harness(settings, EchoLLM())

    result = harness.process(Job(instruction="Hello"))

    assert result.status == "succeeded"
    assert result.worker_id == "test-worker"
    assert result.result is not None
    assert "Hello" in result.result
    assert list(tmp_path.iterdir()) == []


def test_missing_storage_does_not_fail_successful_agent(tmp_path: Path) -> None:
    settings = replace(
        Settings.from_env(),
        worker_id="test-worker",
        workspace_root=str(tmp_path),
        s3_endpoint_url="",
    )
    harness = Harness(settings, ArtifactLLM())

    result = harness.process(Job(instruction="Create a file"))

    assert result.status == "succeeded"
    assert result.result == "Done"
    assert result.artifacts == []
    assert result.artifact_error is not None
    assert "no S3 endpoint" in result.artifact_error


def test_upload_failure_does_not_fail_successful_agent(tmp_path: Path) -> None:
    settings = replace(
        Settings.from_env(),
        worker_id="test-worker",
        workspace_root=str(tmp_path),
        s3_endpoint_url="http://storage.invalid",
    )
    harness = Harness(settings, ArtifactLLM())
    harness._storage = FailingStorage()

    result = harness.process(Job(instruction="Create a file"))

    assert result.status == "succeeded"
    assert result.result == "Done"
    assert result.artifacts == []
    assert result.artifact_error == "Upload failed"

