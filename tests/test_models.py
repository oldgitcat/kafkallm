import json

import pytest
from pydantic import ValidationError

from kafkallm.models import Job, JobState, SandboxSpec, to_json_bytes


def test_job_defaults_and_serialization() -> None:
    job = Job(instruction="Hello")

    assert job.type == "llm_task"
    assert job.sandbox == "default"
    assert job.unlimited is False
    assert job.job_id != job.run_id
    assert json.loads(to_json_bytes(job))["instruction"] == "Hello"


def test_job_and_state_keep_the_unlimited_flag() -> None:
    job = Job(instruction="Run for as long as needed", unlimited=True)
    state = JobState.queued(job)

    assert job.unlimited is True
    assert state.unlimited is True


def test_job_accepts_a_registry_sandbox_id() -> None:
    job = Job.model_validate({"instruction": "Hello", "sandbox": "malware-analysis"})

    assert job.sandbox == "malware-analysis"


def test_job_rejects_an_unsafe_sandbox_id() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "sandbox": "../escape"})


def test_sandbox_spec_accepts_custom_environment_settings() -> None:
    sandbox = SandboxSpec(
        sandbox_id="malware",
        name="Malware analysis",
        image="localhost/kafkallm-malware:latest",
        env={"YARA_RULES": "/opt/rules"},
        extensions=["rea:/opt/kafkallm/rea-mcp"],
    )

    assert sandbox.enabled
    assert sandbox.env["YARA_RULES"] == "/opt/rules"
    assert sandbox.extensions == ["rea:/opt/kafkallm/rea-mcp"]


@pytest.mark.parametrize(
    "key",
    ["OPENAI_API_KEY", "GOOSE_MODEL", "KAFKALLM_SANDBOX", "PATH"],
)
def test_sandbox_spec_rejects_reserved_environment_names(key: str) -> None:
    with pytest.raises(ValidationError):
        SandboxSpec(
            sandbox_id="custom",
            name="Custom",
            image="custom:latest",
            env={key: "value"},
        )


def test_sandbox_spec_rejects_unsafe_extensions() -> None:
    with pytest.raises(ValidationError):
        SandboxSpec(
            sandbox_id="custom",
            name="Custom",
            image="custom:latest",
            extensions=["rea:/opt/tool,other"],
        )


def test_job_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "unexpected": True})


@pytest.mark.parametrize("run_id", ["../escape", "/tmp/escape", "a/b"])
def test_job_rejects_unsafe_run_ids(run_id: str) -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "run_id": run_id})


def test_job_rejects_overlong_trace_id() -> None:
    with pytest.raises(ValidationError):
        Job.model_validate({"instruction": "Hello", "trace_id": "x" * 129})


def test_job_state_is_the_gui_projection() -> None:
    job = Job(instruction="Hello", trace_id="trace-1")
    state = JobState.queued(job, worker_id="worker-1")

    assert state.job_id == job.job_id
    assert state.status == "queued"
    assert state.worker_id == "worker-1"
    assert state.sandbox == "default"
    assert state.submitted_at is not None
    assert state.started_at is None


def test_job_state_keeps_the_reverse_sandbox() -> None:
    job = Job(instruction="Analyze this", sandbox="reverse")
    state = JobState.queued(job)

    assert state.sandbox == "reverse"
